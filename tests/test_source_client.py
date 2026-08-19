"""Unit tests for the source read transport.

Every test drives the real HotdataSourceClient with its SDK surface faked out --
no network, no engine. The fake models the two facts the transport is built
around: the inline query body is a bounded preview, and the authoritative row
count arrives as a response HEADER while the body's ``row_count`` describes only
the slice that was asked for.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, ClassVar

import pyarrow as pa
import pytest
from hotdata.models.async_query_response import AsyncQueryResponse
from hotdata.models.query_response import QueryResponse

import hotdata_dlt_destination.source_client as source_client
from hotdata_dlt_destination.source_client import (
    HotdataSourceClient,
    IncompleteReadError,
    UnverifiableReadError,
    _describes_a_result,
    _empty_batch,
    _has_list_view,
    _materialized,
    _materialized_schema,
    _response_row_count,
    _sliced,
    _total_from_headers,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

DB = "dbid_source"

CANNED = pa.table(
    {
        "id": [1, 2, 3, 4, 5],
        "name": ["a", "b", "c", "d", "e"],
    }
)


class _FakeHeaders(dict[str, str]):
    pass


class _FakeStatusResponse:
    def __init__(self, headers: dict[str, str]) -> None:
        self.headers = _FakeHeaders(headers)
        # row_count describes the requested slice, not the whole result. With
        # limit=0 the real API returns 0 here -- the transport must not read it.
        self.data = type("Data", (), {"status": "ready", "row_count": 0, "rows": []})()


class _FakeReader:
    """Stands in for pyarrow's RecordBatchStreamReader.

    Carries `schema` as well as the batches, because the real reader does: an
    empty result is a schema with no batches, and that is the only place the
    column names come from.
    """

    def __init__(self, batches: list[pa.RecordBatch], schema: pa.Schema) -> None:
        self._batches = batches
        self.schema = schema

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        return iter(self._batches)


class FakeSourceClient(HotdataSourceClient):
    """HotdataSourceClient with the three SDK touchpoints replaced.

    Subclassed rather than mocked so the real control flow runs: submit ->
    total -> fetch -> assert.
    """

    def __init__(
        self,
        *,
        table: pa.Table = CANNED,
        total: int | None = None,
        result_id: str | None = "rslt_1",
        header_name: str = "X-Total-Row-Count",
        unpersisted_rows: int = 0,
    ) -> None:
        # Deliberately no super().__init__(): that would build a real API client.
        # _runtime still has to exist -- the real streaming path reads
        # self._runtime.api to construct the Arrow API.
        self._runtime = SimpleNamespace(api=None)
        self._max_retries = 1
        self._retry_backoff_seconds = 0.0
        self._table = table
        self._total = table.num_rows if total is None else total
        self._result_id = result_id
        self._header_name = header_name
        self._unpersisted_rows = unpersisted_rows
        self.submitted: list[str] = []
        self.closed = False

    # --- faked SDK surface -------------------------------------------------

    def _query_database_scoped(self, sql: str, *, database_id: str) -> str | None:
        # Retained for tests that drive the shared client's own path.
        assert database_id == DB
        self.submitted.append(sql)
        return self._result_id

    def _submit(self, sql: str, *, database_id: str) -> tuple[str, int] | None:
        assert database_id == DB
        self.submitted.append(sql)
        if self._result_id is None:
            if self._unpersisted_rows > 0:
                raise UnverifiableReadError(
                    "the query returned rows but its result could not be persisted, so "
                    "there is nothing to read them back from in full"
                )
            return None
        # Mirrors the real method: the readiness wait is where the total comes
        # from, so a missing header surfaces here too.
        response = _FakeStatusResponse({self._header_name: str(self._total)})
        return self._result_id, _total_from_headers(
            response.headers, result_id=self._result_id
        )

    def _fetch_result_arrow(self, result_id: str, *, database_id: str) -> pa.Table:
        return self._table

    def close(self) -> None:
        self.closed = True


class _FakeArrowApi:
    """Stands in for hotdata.arrow.ResultsApi.

    Patched over the name `source_client` imports, so the REAL
    `read_arrow_batches` runs -- including its row accounting and its schema
    normalisation. An earlier version of this file subclassed the method instead,
    which meant the streaming tests re-implemented the code they were checking
    and could not have caught a change to it.
    """

    # Class-level so the fixture can seed it before the real code constructs an
    # instance of this class itself.
    batches: ClassVar[list[pa.RecordBatch]] = []
    schema: ClassVar[pa.Schema] = CANNED.schema

    def __init__(self, api: object) -> None:
        pass

    @contextmanager
    def stream_result_arrow(self, result_id: str, database_id: str) -> Iterator[_FakeReader]:
        yield _FakeReader(list(type(self).batches), type(self).schema)


@pytest.fixture
def streaming(monkeypatch: pytest.MonkeyPatch) -> Callable[..., FakeSourceClient]:
    """Build a client whose Arrow streaming path is served from a table."""

    def build(*, table: pa.Table = CANNED, total: int | None = None) -> FakeSourceClient:
        _FakeArrowApi.batches = table.to_batches()
        _FakeArrowApi.schema = table.schema
        monkeypatch.setattr(source_client, "ArrowResultsApi", _FakeArrowApi)
        return FakeSourceClient(table=table, total=total)

    return build


# --- the read goes to the persisted result, never the inline body -----------


def test_read_arrow_returns_every_row() -> None:
    client = FakeSourceClient()
    table = client.read_arrow("SELECT * FROM public.t", database_id=DB)
    assert table.num_rows == 5
    assert client.submitted == ["SELECT * FROM public.t"]


def test_read_rows_returns_dicts() -> None:
    rows = FakeSourceClient().read_rows("SELECT * FROM public.t", database_id=DB)
    assert rows[0] == {"id": 1, "name": "a"}
    assert len(rows) == 5


def test_a_statement_with_no_persisted_result_is_empty_not_an_error() -> None:
    """A result_id of None means the engine persisted nothing.

    An empty table rather than an exception: a statement that produces no rows
    is a legitimate outcome, and raising would make every caller special-case it.
    """
    client = FakeSourceClient(result_id=None)
    assert client.read_arrow("SET something = 1", database_id=DB).num_rows == 0


# --- the assertion this module exists for ----------------------------------


def test_a_short_read_raises_rather_than_returning_a_prefix() -> None:
    """The failure this transport exists to prevent.

    The result holds 9 rows and the fetch produced 5. Nothing about the 5 rows
    says so, so a caller that recorded progress from them would mark 4 rows as
    read that it never saw -- silently, permanently, and reporting success.
    """
    client = FakeSourceClient(total=9)
    with pytest.raises(IncompleteReadError) as excinfo:
        client.read_arrow("SELECT * FROM public.t", database_id=DB)
    assert excinfo.value.expected == 9
    assert excinfo.value.received == 5
    assert "partial result cannot be distinguished" in str(excinfo.value)


def test_an_over_long_read_also_raises() -> None:
    """Not only short reads: any disagreement is refused.

    More rows than the result holds means the count and the data are describing
    different things, and which one is wrong is not knowable from here. A caller
    told 'complete' either way would be told something false.
    """
    client = FakeSourceClient(total=2)
    with pytest.raises(IncompleteReadError):
        client.read_arrow("SELECT * FROM public.t", database_id=DB)


def test_a_missing_total_header_raises_rather_than_skipping_the_check() -> None:
    """No header means the check cannot be made, which is not the same as passing.

    Defaulting to 'assume complete' would turn the one guarantee this module
    offers into a silent no-op the day the header stops being sent. A distinct
    error from IncompleteReadError, because "the check failed" and "the check
    could not run" call for different responses from an operator.
    """
    client = FakeSourceClient(header_name="X-Something-Else")
    with pytest.raises(UnverifiableReadError, match="completeness cannot be established"):
        client.read_arrow("SELECT * FROM public.t", database_id=DB)


def test_the_completeness_error_survives_the_transport_error_mapping() -> None:
    """IncompleteReadError must reach the caller as itself.

    The base client maps everything raised under `_request_with_retry` through
    `classify_sdk_error`, which would deliver this as a generic terminal
    transport error -- indistinguishable from a network fault, and retried on the
    way. So the assertion deliberately sits outside that wrapper; this test is
    what holds it there.
    """
    client = FakeSourceClient(total=9)
    with pytest.raises(IncompleteReadError):
        client.read_arrow("SELECT * FROM public.t", database_id=DB)


def test_a_completeness_failure_is_not_retried() -> None:
    """One submit, not `max_retries` of them.

    Retrying a short read re-runs the query and spends the retry budget on a
    condition no retry can fix, which shows up as latency rather than as the
    error it is.
    """
    client = FakeSourceClient(total=9)
    client._max_retries = 5
    with pytest.raises(IncompleteReadError):
        client.read_arrow("SELECT * FROM public.t", database_id=DB)
    assert len(client.submitted) == 1


def test_the_slice_row_count_in_the_body_is_not_used_as_the_total() -> None:
    """The body's row_count is the slice's, and the fake returns 0 for it.

    If the transport ever read that field instead of the header, every read of a
    non-empty result would raise here -- so this passing is what proves it does
    not.
    """
    table = FakeSourceClient().read_arrow("SELECT * FROM public.t", database_id=DB)
    assert table.num_rows == 5


# --- streaming path --------------------------------------------------------


def test_batches_stream_and_verify_at_the_end(
    streaming: Callable[..., FakeSourceClient],
) -> None:
    batches = list(streaming().read_arrow_batches("SELECT * FROM public.t", database_id=DB))
    assert sum(b.num_rows for b in batches) == 5


def test_a_short_stream_raises_after_the_last_batch(
    streaming: Callable[..., FakeSourceClient],
) -> None:
    """The consumer receives batches, then the read fails.

    The check cannot happen earlier: a short read is only visible as a total.
    What must not happen is the stream ending quietly -- so the exception has to
    arrive on exhaustion, which means a consumer using a for-loop still sees it.
    """
    client = streaming(total=99)
    with pytest.raises(IncompleteReadError):
        list(client.read_arrow_batches("SELECT * FROM public.t", database_id=DB))


def test_batch_rows_splits_but_never_drops_rows(
    streaming: Callable[..., FakeSourceClient],
) -> None:
    client = streaming()
    batches = list(
        client.read_arrow_batches("SELECT * FROM public.t", database_id=DB, batch_rows=2)
    )
    assert [b.num_rows for b in batches] == [2, 2, 1]
    assert sum(b.num_rows for b in batches) == 5


def test_slicing_a_batch_preserves_every_row() -> None:
    batch = CANNED.to_batches()[0]
    for size in (1, 2, 4, 5, 6, 0, -1):
        pieces = list(_sliced(batch, size))
        assert sum(p.num_rows for p in pieces) == batch.num_rows
        assert pa.Table.from_batches(pieces).column("id").to_pylist() == [1, 2, 3, 4, 5]


# --- Arrow view layouts ------------------------------------------------------
#
# The engine returns `string_view` for text. dlt raises
# UnsupportedArrowTypeException on it, so a read that passed views straight
# through failed on any table with a string column -- which unit tests with a
# hand-built pa.table() did not catch, because pa.table() infers `string`.


def test_string_view_is_materialized_so_dlt_can_consume_it() -> None:
    viewed = pa.table(
        {
            "name": pa.array(["a", "b"], type=pa.string_view()),
            "n": pa.array([1, 2], type=pa.int64()),
        }
    )
    table = FakeSourceClient(table=viewed).read_arrow("SELECT * FROM t", database_id=DB)
    assert table.schema.field("name").type == pa.string()
    assert table.column("name").to_pylist() == ["a", "b"]
    # Untouched types stay untouched.
    assert table.schema.field("n").type == pa.int64()


def test_binary_view_is_materialized_too() -> None:
    viewed = pa.table({"blob": pa.array([b"x", b"y"], type=pa.binary_view())})
    table = FakeSourceClient(table=viewed).read_arrow("SELECT * FROM t", database_id=DB)
    assert table.schema.field("blob").type == pa.binary()


def test_a_view_nested_in_a_struct_is_materialized() -> None:
    """Easier to miss than a top-level one, and just as unsupported."""
    inner = pa.struct([pa.field("label", pa.string_view())])
    viewed = pa.table({"meta": pa.array([{"label": "a"}, {"label": "b"}], type=inner)})
    table = FakeSourceClient(table=viewed).read_arrow("SELECT * FROM t", database_id=DB)
    assert table.schema.field("meta").type.field("label").type == pa.string()
    assert table.column("meta").to_pylist() == [{"label": "a"}, {"label": "b"}]


def test_a_schema_with_no_views_is_returned_unchanged() -> None:
    """No cast, no copy: the common case must not pay for the uncommon one."""
    client = FakeSourceClient()
    table = client.read_arrow("SELECT * FROM t", database_id=DB)
    assert table is client._table


def test_streamed_batches_are_materialized_as_well(
    streaming: Callable[..., FakeSourceClient],
) -> None:
    viewed = pa.table({"name": pa.array(["a", "b", "c"], type=pa.string_view())})
    batches = list(
        streaming(table=viewed).read_arrow_batches(
            "SELECT * FROM t", database_id=DB, batch_rows=2
        )
    )
    assert [b.num_rows for b in batches] == [2, 1]
    assert all(b.schema.field("name").type == pa.string() for b in batches)


# --- an unpersistable result must not read as "no rows" ----------------------
#
# A synchronous response carries `result_id: null` in two different situations:
# nothing to persist, and a result that fit inline but could not be persisted.
# The second has rows sitting in the response body, so collapsing both to "empty"
# loads an empty table for a query that returned data.


def test_a_result_that_could_not_be_persisted_raises_rather_than_reading_empty() -> None:
    client = FakeSourceClient(result_id=None, unpersisted_rows=42)
    with pytest.raises(UnverifiableReadError, match="could not be persisted"):
        client.read_arrow("SELECT * FROM public.t", database_id=DB)


def test_a_genuinely_rowless_statement_is_still_empty_not_an_error() -> None:
    """The other side of the same branch: no rows really does mean no rows."""
    client = FakeSourceClient(result_id=None, unpersisted_rows=0)
    assert client.read_arrow("SET x = 1", database_id=DB).num_rows == 0


def test_the_response_row_count_prefers_the_current_field_over_the_deprecated_alias() -> None:
    current = SimpleNamespace(preview_row_count=7, row_count=3, rows=[])
    assert _response_row_count(current) == 7
    # Older server: only the deprecated alias is populated.
    legacy = SimpleNamespace(preview_row_count=None, row_count=3, rows=[])
    assert _response_row_count(legacy) == 3
    # Neither: fall back to the body itself rather than assuming zero.
    neither = SimpleNamespace(preview_row_count=None, row_count=None, rows=[{"a": 1}])
    assert _response_row_count(neither) == 1


# --- list views cannot be cast ----------------------------------------------


def test_a_list_view_column_is_rebuilt_into_a_valid_array() -> None:
    """`cast` produces a corrupt array here, so this path must not use it.

    pyarrow accepts a list_view -> list cast and returns an array whose values
    compare correctly but whose offsets buffer is still sized for view
    semantics -- it fails validation only once placed in a table. Validating in
    full is what distinguishes a real conversion from that one.
    """
    viewed = pa.table(
        {
            "xs": pa.array([[1, 2], [3], None], type=pa.list_view(pa.int64())),
            "n": pa.array([1, 2, 3], type=pa.int64()),
        }
    )
    table = FakeSourceClient(table=viewed).read_arrow("SELECT * FROM t", database_id=DB)
    table.validate(full=True)
    assert table.schema.field("xs").type == pa.list_(pa.int64())
    assert table.column("xs").to_pylist() == [[1, 2], [3], None]
    assert table.column("n").to_pylist() == [1, 2, 3]


def test_a_list_view_nested_in_a_struct_is_rebuilt_too() -> None:
    inner = pa.struct([pa.field("xs", pa.list_view(pa.int64()))])
    viewed = pa.table({"meta": pa.array([{"xs": [1, 2]}], type=inner)})
    table = FakeSourceClient(table=viewed).read_arrow("SELECT * FROM t", database_id=DB)
    table.validate(full=True)
    assert table.schema.field("meta").type.field("xs").type == pa.list_(pa.int64())
    assert table.column("meta").to_pylist() == [{"xs": [1, 2]}]


def test_a_string_view_still_takes_the_fast_cast_path() -> None:
    """Only list views need rebuilding; strings must not pay for it."""
    viewed = pa.table({"a": pa.array(["x", "y"], type=pa.string_view())})
    assert not _has_list_view(viewed.schema.field("a").type)
    table = FakeSourceClient(table=viewed).read_arrow("SELECT * FROM t", database_id=DB)
    table.validate(full=True)
    assert table.schema.field("a").type == pa.string()


# --- the REAL submit path ----------------------------------------------------
#
# Everything above fakes `_submit` itself, which twice hid a defect in it: first
# that an assertion inside the retry wrapper lost its type, then that `_poll`
# reads `obj.status` on the model while the header-bearing variant puts it at
# `.data.status`. These tests patch the SDK classes the module imports, so the
# real `_submit` and `_await_result` run.


class _FakeQueryApi:
    response: ClassVar[object] = None

    def __init__(self, api: object) -> None:
        pass

    def query(self, request: object, x_database_id: str | None = None) -> object:
        return type(self).response


class _FakeResultsApi:
    """Reports `processing` for a while, then `ready`, then serves the header."""

    statuses: ClassVar[list[str]] = ["ready"]
    total: ClassVar[int] = 5
    limits_seen: ClassVar[list[int | None]] = []

    def __init__(self, api: object) -> None:
        pass

    def get_result(
        self, id: str, x_database_id: str | None = None, limit: int | None = None
    ) -> object:
        type(self).limits_seen.append(limit)
        status = type(self).statuses.pop(0) if len(type(self).statuses) > 1 else type(
            self
        ).statuses[0]
        return SimpleNamespace(status=status, error_message=None, row_count=0, rows=[])

    def get_result_with_http_info(
        self, id: str, x_database_id: str | None = None, limit: int | None = None
    ) -> object:
        return _FakeStatusResponse({"X-Total-Row-Count": str(type(self).total)})


def _query_response(*, result_id: str | None, preview_row_count: int) -> object:
    """A real QueryResponse instance -- `_submit` branches on isinstance."""
    return QueryResponse.model_construct(
        result_id=result_id,
        preview_row_count=preview_row_count,
        row_count=preview_row_count,
        total_row_count=None,
        truncated=result_id is not None,
        rows=[],
        columns=[],
        nullable=[],
        query_run_id="qr_1",
        execution_time_ms=1,
        warning=None,
    )


@pytest.fixture
def real_submit(monkeypatch: pytest.MonkeyPatch) -> Callable[..., HotdataSourceClient]:
    def build(*, result_id: str | None, preview_rows: int, total: int = 5) -> HotdataSourceClient:
        _FakeQueryApi.response = _query_response(
            result_id=result_id, preview_row_count=preview_rows
        )
        _FakeResultsApi.statuses = ["processing", "ready"]
        _FakeResultsApi.total = total
        _FakeResultsApi.limits_seen = []
        monkeypatch.setattr(source_client, "QueryApi", _FakeQueryApi)
        monkeypatch.setattr(source_client, "ResultsApi", _FakeResultsApi)
        client = HotdataSourceClient.__new__(HotdataSourceClient)
        client._max_retries = 1
        client._retry_backoff_seconds = 0.0
        client._runtime = SimpleNamespace(api=None)
        return client

    return build


def test_the_real_submit_waits_for_ready_and_returns_the_total(
    real_submit: Callable[..., HotdataSourceClient],
) -> None:
    client = real_submit(result_id="rslt_9", preview_rows=10_000, total=25_000)
    assert client._submit("SELECT 1", database_id=DB) == ("rslt_9", 25_000)
    # It polled through `processing` before reading the header.
    assert len(_FakeResultsApi.limits_seen) >= 2


def test_the_readiness_poll_asks_for_no_rows(
    real_submit: Callable[..., HotdataSourceClient],
) -> None:
    """limit=0 on every poll.

    The parameter defaults to unbounded, so polling without it downloads the
    entire result body on each pass just to read a status field.
    """
    client = real_submit(result_id="rslt_9", preview_rows=1)
    client._submit("SELECT 1", database_id=DB)
    assert set(_FakeResultsApi.limits_seen) == {0}


def test_the_real_submit_refuses_an_unpersistable_result(
    real_submit: Callable[..., HotdataSourceClient],
) -> None:
    client = real_submit(result_id=None, preview_rows=42)
    with pytest.raises(UnverifiableReadError, match="could not be persisted"):
        client._submit("SELECT 1", database_id=DB)


def test_the_real_submit_returns_none_for_a_rowless_statement(
    real_submit: Callable[..., HotdataSourceClient],
) -> None:
    client = real_submit(result_id=None, preview_rows=0)
    assert client._submit("SET x = 1", database_id=DB) is None


# --- an empty result still has a schema --------------------------------------


def test_an_empty_result_still_yields_one_batch_carrying_the_schema(
    streaming: Callable[..., FakeSourceClient],
) -> None:
    """Zero rows is not zero information.

    An Arrow IPC stream for an empty result carries the schema in its header and
    then ends, so iterating it yields no batches. Passing that straight on would
    hand dlt no items at all -- it would never learn the columns, so it would
    neither create nor truncate the destination table, while `read_arrow` on the
    same query returns a schemaful empty table.
    """
    empty = CANNED.schema.empty_table()
    batches = list(streaming(table=empty).read_arrow_batches("SELECT * FROM t", database_id=DB))
    assert len(batches) == 1
    assert batches[0].num_rows == 0
    assert batches[0].schema.names == ["id", "name"]


def test_a_non_empty_result_does_not_get_a_padding_batch(
    streaming: Callable[..., FakeSourceClient],
) -> None:
    batches = list(streaming().read_arrow_batches("SELECT * FROM t", database_id=DB))
    assert all(b.num_rows > 0 for b in batches)


# --- dictionary-encoded view columns -----------------------------------------


def test_a_dictionary_encoded_string_view_is_materialized() -> None:
    """`dictionary<values=string_view>` is rejected by dlt just like a bare view.

    The encoding is fine; the value layout inside it is not. Recursing only
    through lists and structs left this schema looking unchanged, so nothing was
    cast and dlt got a type it refuses.
    """
    dict_type = pa.dictionary(pa.int32(), pa.string_view())
    viewed = pa.table(
        {"tag": pa.array(["a", "b", "a"], type=pa.string_view()).dictionary_encode()}
    )
    assert pa.types.is_dictionary(viewed.schema.field("tag").type)
    table = FakeSourceClient(table=viewed, total=3).read_arrow(
        "SELECT * FROM t", database_id=DB
    )
    table.validate(full=True)
    result_type = table.schema.field("tag").type
    assert pa.types.is_dictionary(result_type)
    assert result_type.value_type == pa.string()
    assert table.column("tag").to_pylist() == ["a", "b", "a"]
    # Sanity: the unfixed shape is genuinely one dlt refuses.
    assert dict_type.value_type == pa.string_view()


def test_a_dictionary_of_plain_strings_is_left_alone() -> None:
    plain = pa.table({"tag": pa.array(["a", "b"], type=pa.string()).dictionary_encode()})
    client = FakeSourceClient(table=plain, total=2)
    assert client.read_arrow("SELECT * FROM t", database_id=DB) is plain


# --- round-4 edge cases ------------------------------------------------------


def test_an_empty_select_whose_result_was_not_persisted_raises() -> None:
    """Zero rows is not "no result" when the query had a column shape.

    An empty SELECT still has columns that dlt needs in order to create or
    truncate the destination table. Classifying it as "nothing to read" would
    leave the table untouched while the run reported success -- so the columns,
    not the row count, decide whether a response describes a result.
    """
    assert _describes_a_result(
        _query_response_with(columns=["a"], preview_row_count=0)
    )
    assert not _describes_a_result(
        _query_response_with(columns=[], preview_row_count=0)
    )


def _query_response_with(*, columns: list[str], preview_row_count: int) -> object:
    return QueryResponse.model_construct(
        result_id=None,
        columns=columns,
        preview_row_count=preview_row_count,
        row_count=preview_row_count,
        rows=[],
        nullable=[],
        total_row_count=None,
        truncated=False,
        query_run_id="qr_1",
        execution_time_ms=1,
        warning="could not persist",
    )


def test_an_empty_batch_is_built_for_a_layout_from_pylist_cannot_construct() -> None:
    """`from_pylist` has no converter for a dictionary of string_view, even empty.

    So the padding batch must be built in the already-materialised schema, and
    per field rather than from Python rows. This is the ordering the zero-row
    path depends on.
    """
    wire = pa.schema([pa.field("tag", pa.dictionary(pa.int32(), pa.string_view()))])
    with pytest.raises(pa.lib.ArrowNotImplementedError):
        pa.RecordBatch.from_pylist([], schema=wire)
    batch = _empty_batch(_materialized_schema(wire))
    batch.validate(full=True)
    assert batch.num_rows == 0
    assert batch.schema.field("tag").type.value_type == pa.string()


def test_a_list_childs_name_and_nullability_survive_normalization() -> None:
    """The normaliser must not rewrite a schema it was not asked to change.

    `pa.list_(value_type)` renames the child to `item` and makes it nullable, so
    a list with a named or non-null child would come back altered even though it
    held no view layout at all.
    """
    named = pa.list_(pa.field("elem", pa.int64(), nullable=False))
    assert _materialized(named) == named

    viewed = pa.list_(pa.field("elem", pa.string_view(), nullable=False))
    out = _materialized(viewed)
    assert out.value_field.name == "elem"
    assert out.value_field.nullable is False
    assert out.value_field.type == pa.string()


def test_a_schema_with_nothing_to_change_is_returned_untouched() -> None:
    plain = pa.schema(
        [
            pa.field("xs", pa.list_(pa.field("elem", pa.int64(), nullable=False))),
            pa.field("s", pa.string()),
            # A view-free map is the case a rebuild cannot round-trip: `pa.map_`
            # always names the entries field `entries`, and field names count
            # towards Arrow type equality -- so rebuilding one would make it
            # compare as changed and send the column through a needless cast.
            pa.field(
                "m",
                pa.map_(
                    pa.field("key", pa.string(), nullable=False),
                    pa.field("value", pa.int64()),
                ),
            ),
        ]
    )
    assert _materialized_schema(plain).equals(plain)


def test_a_view_free_map_is_returned_as_the_same_type_object() -> None:
    """Identity, not just equality.

    `pa.map_` also refuses a nullable key field outright, so a map that was only
    passing through must never reach the rebuild.
    """
    plain = pa.map_(
        pa.field("key", pa.string(), nullable=False), pa.field("value", pa.int64())
    )
    assert _materialized(plain) is plain


# --- the asynchronous submit path --------------------------------------------
#
# `_FakeQueryApi.response` is a QueryResponse everywhere above, so none of it
# reaches AsyncQueryResponse or `_await_run_result_id` -- and that is the path a
# long-running read comes back on.


class _FakeQueryRunsApi:
    result_id: ClassVar[str | None] = "rslt_async"
    statuses: ClassVar[list[str]] = ["succeeded"]

    def __init__(self, api: object) -> None:
        pass

    def get_query_run(self, query_run_id: str, x_database_id: str | None = None) -> object:
        status = (
            type(self).statuses.pop(0)
            if len(type(self).statuses) > 1
            else type(self).statuses[0]
        )
        return SimpleNamespace(
            status=status, error_message=None, result_id=type(self).result_id
        )


@pytest.fixture
def async_submit(monkeypatch: pytest.MonkeyPatch) -> Callable[..., HotdataSourceClient]:
    def build(*, run_result_id: str | None, total: int = 5) -> HotdataSourceClient:
        _FakeQueryApi.response = AsyncQueryResponse.model_construct(
            query_run_id="qr_async", status="running", status_url="/v1/query-runs/qr_async"
        )
        _FakeQueryRunsApi.result_id = run_result_id
        _FakeQueryRunsApi.statuses = ["running", "succeeded"]
        _FakeResultsApi.statuses = ["ready"]
        _FakeResultsApi.total = total
        _FakeResultsApi.limits_seen = []
        monkeypatch.setattr(source_client, "QueryApi", _FakeQueryApi)
        monkeypatch.setattr(source_client, "QueryRunsApi", _FakeQueryRunsApi)
        monkeypatch.setattr(source_client, "ResultsApi", _FakeResultsApi)
        client = HotdataSourceClient.__new__(HotdataSourceClient)
        client._max_retries = 1
        client._retry_backoff_seconds = 0.0
        client._runtime = SimpleNamespace(api=None)
        return client

    return build


def test_an_async_run_is_awaited_then_its_result_read(
    async_submit: Callable[..., HotdataSourceClient],
) -> None:
    client = async_submit(run_result_id="rslt_async", total=1234)
    assert client._submit("SELECT 1", database_id=DB) == ("rslt_async", 1234)


def test_a_succeeded_async_run_naming_no_result_raises(
    async_submit: Callable[..., HotdataSourceClient],
) -> None:
    """Not knowably rowless, so not silently empty.

    The synchronous branch can tell "nothing to return" from "could not be
    persisted" by looking at the columns. Here there are none, so returning None
    would assert something nothing established -- and under
    write_disposition="replace" an empty read truncates the destination table and
    reports success.
    """
    client = async_submit(run_result_id=None)
    with pytest.raises(UnverifiableReadError, match="named no result"):
        client._submit("SELECT 1", database_id=DB)


def test_an_unrecognised_response_type_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The branch an SDK bump lands in.

    `pyproject.toml` caps the SDK partly because of query-path model churn. If
    `query()` ever returns a third model, falling through to "no rows" would make
    every read silently empty, with no test failing to say so.
    """
    _FakeQueryApi.response = SimpleNamespace(something="else")
    monkeypatch.setattr(source_client, "QueryApi", _FakeQueryApi)
    client = HotdataSourceClient.__new__(HotdataSourceClient)
    client._max_retries = 1
    client._retry_backoff_seconds = 0.0
    client._runtime = SimpleNamespace(api=None)
    with pytest.raises(UnverifiableReadError, match="unrecognised response"):
        client._submit("SELECT 1", database_id=DB)


def test_a_map_or_fixed_size_list_view_is_materialized() -> None:
    """Both have their own Arrow type ids, so `is_list` is False for them.

    Without their own arms they fell straight through unconverted and reached dlt
    with a view still inside -- the same gap struct and dictionary were each
    added to close.
    """
    mapped = pa.map_(
        pa.field("key", pa.string_view(), nullable=False), pa.field("value", pa.int64())
    )
    out = _materialized(mapped)
    assert pa.types.is_map(out)  # not rebuilt into a plain list
    assert out.key_field.type == pa.string()
    assert out.key_field.nullable is False

    fsl = pa.list_(pa.field("elem", pa.string_view()), 3)
    out2 = _materialized(fsl)
    assert pa.types.is_fixed_size_list(out2)
    assert out2.list_size == 3
    assert out2.value_field.type == pa.string()


def test_only_the_list_view_columns_take_the_python_rebuild() -> None:
    """A normaliser must not reshape a column it was not asked to touch.

    The rebuild is also the slow path, so applying it table-wide because one
    column needs it spends that on every other column for nothing.
    """
    viewed = pa.table(
        {
            "xs": pa.array([[1, 2]], type=pa.list_view(pa.int64())),
            "ts": pa.array([1_234_567_891_123_456_789], type=pa.timestamp("ns")),
        }
    )
    out = FakeSourceClient(table=viewed, total=1).read_arrow("SELECT * FROM t", database_id=DB)
    out.validate(full=True)
    assert out.schema.field("xs").type == pa.list_(pa.int64())
    # The untouched column keeps its exact type and value.
    assert out.schema.field("ts").type == pa.timestamp("ns")
    assert out.column("ts")[0].value == 1_234_567_891_123_456_789


def test_a_non_numeric_total_header_is_an_unverifiable_read() -> None:
    """Still a refusal, but one a caller can recognise.

    A bare ValueError from int() escapes the two errors this module documents, so
    a caller distinguishing "could not verify" from a real fault would miss it.
    """
    with pytest.raises(UnverifiableReadError, match="not a row count"):
        _total_from_headers({"X-Total-Row-Count": "1,024"}, result_id="rslt_1")


def test_a_map_of_string_view_is_materialized_through_the_read_path() -> None:
    """Drives a real map ARRAY, not just its type.

    `_has_list_view` is False for `map<string_view, int64>`, so this column takes
    `data.cast(schema)` rather than the Python rebuild -- and that cast is
    between two map types whose `entries` struct differs. Measured: pyarrow
    implements it, so the cast path is correct for maps and they do not belong
    with the list views. This test is what says so.
    """
    mapped = pa.map_(
        pa.field("key", pa.string_view(), nullable=False), pa.field("value", pa.int64())
    )
    viewed = pa.table({"m": pa.array([[("a", 1), ("b", 2)]], type=mapped)})
    out = FakeSourceClient(table=viewed, total=1).read_arrow("SELECT * FROM t", database_id=DB)
    out.validate(full=True)
    result = out.schema.field("m").type
    assert pa.types.is_map(result)
    assert result.key_field.type == pa.string()
    assert result.key_field.nullable is False
    assert out.column("m").to_pylist() == [[("a", 1), ("b", 2)]]


def test_a_fixed_size_list_of_string_view_is_materialized_through_the_read_path() -> None:
    """Same as above for fixed-size lists, including that the width survives."""
    sized = pa.list_(pa.field("item", pa.string_view()), 2)
    viewed = pa.table({"f": pa.array([["x", "y"], ["z", "w"]], type=sized)})
    out = FakeSourceClient(table=viewed, total=2).read_arrow("SELECT * FROM t", database_id=DB)
    out.validate(full=True)
    result = out.schema.field("f").type
    assert pa.types.is_fixed_size_list(result)
    assert result.list_size == 2
    assert result.value_field.type == pa.string()
    assert out.column("f").to_pylist() == [["x", "y"], ["z", "w"]]


def test_a_list_view_inside_a_map_takes_the_rebuild_path() -> None:
    """The nesting that does need rebuilding, so the two paths stay distinguished."""
    inner = pa.map_(
        pa.field("key", pa.string(), nullable=False),
        pa.field("value", pa.list_view(pa.int64())),
    )
    assert _has_list_view(inner)
    assert _materialized(inner).item_field.type == pa.list_(pa.int64())
