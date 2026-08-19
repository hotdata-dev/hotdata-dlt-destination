"""Read transport for using Hotdata as a dlt *source*.

Turns a SQL string into every row the query produced, or raises. The
"or raises" is the whole point of this module, and it is why the read does not
use the obvious response body.

WHY NOT THE INLINE RESPONSE. ``POST /v1/query`` answers synchronously with a
**bounded preview** — the server caps it (10k rows / 8 MiB by default) and there
is no request parameter to lift the cap. A capped response is not an error: it
carries ``truncated: true`` and a ``result_id`` naming the full result, which was
streamed to storage in its entirety. Reading rows out of that body therefore
yields the head of a result while looking exactly like a complete one. Every read
here goes to the persisted result instead, where ``limit`` is unbounded by
default.

WHY THE ROW COUNT IS ASSERTED. A short read that does not raise is
indistinguishable from a complete one, and a caller that records progress from it
marks rows as read that were never seen — silently, permanently, and reported as
success. ``X-Total-Row-Count`` on the result response is the authoritative total
(taken from the stored parquet footer, independent of any offset/limit), so the
comparison is cheap and exact. It is a header, not a body field: the JSON
``row_count`` is the count of rows in *that slice* — 0 for ``limit=0`` — so it
cannot stand in.

WHY THE SCHEMA IS REWRITTEN. The engine returns Arrow *view* layouts —
``string_view`` for text, and the binary/list equivalents. They are a wire-format
optimisation, not a different type, and dlt rejects them outright
(``UnsupportedArrowTypeException``), as do some other Arrow consumers. Every read
therefore casts view layouts to their materialised equivalents before handing
anything back, so a caller never has to know the distinction existed. Only the
layout changes; values and nullability are untouched.

WHAT IT COSTS. Going to the persisted result means waiting for it to be ready,
and readiness is polled. Measured against a live workspace, a 50,000-row read is
about 2.5s end to end: ~1.5s submit-and-wait, ~0.3s for the row count, ~0.75s for
the Arrow body. Reading the same rows out of the inline body is roughly 100ms —
but only while the result fits under the preview cap, above which that read does
not return the rows at all. Slower and complete beats faster and silently short.
The count is a second request rather than folded into the readiness poll because
the poll lives in the shared client, and duplicating its sync/async branching to
save ~12% of the latency would risk the two drifting.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

import pyarrow as pa
import pyarrow.types as pat
from hotdata.api.query_api import QueryApi
from hotdata.api.query_runs_api import QueryRunsApi
from hotdata.api.results_api import ResultsApi
from hotdata.arrow import ResultsApi as ArrowResultsApi
from hotdata.models.async_query_response import AsyncQueryResponse
from hotdata.models.query_request import QueryRequest
from hotdata.models.query_response import QueryResponse
from hotdata_framework.managed_client import ManagedDatabaseClient

if TYPE_CHECKING:
    from collections.abc import Iterator

# Rows per Arrow batch handed to dlt. Bounds peak memory for one batch; it does
# not bound the query, whose size the caller controls with its own LIMIT.
DEFAULT_BATCH_ROWS = 50_000

# Table and RecordBatch both carry `.schema` and `.cast`, and the normaliser
# returns whichever it was given.
T = TypeVar("T", pa.Table, pa.RecordBatch)


class IncompleteReadError(RuntimeError):
    """A read returned fewer rows than the result holds.

    Raised rather than returned so a partial result cannot be mistaken for the
    whole one by a caller that only checks for exceptions.
    """

    def __init__(self, *, result_id: str, expected: int, received: int) -> None:
        super().__init__(
            f"read of result {result_id} returned {received} row(s) but the result "
            f"holds {expected}; a partial result cannot be distinguished from a "
            "complete one by its contents, so it is refused rather than returned"
        )
        self.result_id = result_id
        self.expected = expected
        self.received = received


class UnverifiableReadError(RuntimeError):
    """The result's authoritative row count was absent, so completeness is unknown.

    Distinct from :class:`IncompleteReadError`, which means the check ran and
    failed. This one means it could not run — and it is an error rather than a
    pass, because defaulting to "assume complete" would turn the transport's one
    guarantee into a silent no-op the day the header stops being sent.
    """

    def __init__(self, reason: str, *, result_id: str | None = None) -> None:
        where = f"result {result_id}: " if result_id else ""
        super().__init__(f"{where}{reason}; completeness cannot be established")
        self.result_id = result_id


class HotdataSourceClient(ManagedDatabaseClient):
    """Runs read-only SQL against a managed database and returns every row.

    Unlike the destination's client this holds no database of its own: the
    database id is an argument to each read, because a pipeline commonly reads
    one database and writes another. Nothing here creates, declares, or mutates
    anything.
    """

    def read_arrow(self, sql: str, *, database_id: str) -> pa.Table:
        """The query's full result as one Arrow table.

        Materialises the whole result. Prefer :meth:`read_arrow_batches` when the
        result may be large; use this when the caller wants one table.

        Each REQUEST is retried; the completeness check is not inside a retry
        wrapper. The base client maps everything raised under
        ``_request_with_retry`` through ``classify_sdk_error``, so an assertion
        raised in there would reach the caller as a generic transport error and
        be indistinguishable from a network fault — and would be retried, which
        turns a refusal into a delay.
        """
        ready = self._submit(sql, database_id=database_id)
        if ready is None:
            # A statement that produced no out-of-band result (DDL, or a query
            # the engine did not persist). An empty table, not an error.
            return pa.table({})
        result_id, expected = ready
        table = self._request_with_retry(
            lambda: self._fetch_result_arrow(result_id, database_id=database_id)
        )
        _require_complete(result_id=result_id, expected=expected, received=table.num_rows)
        return _materialize_views(table)

    def read_arrow_batches(
        self, sql: str, *, database_id: str, batch_rows: int = DEFAULT_BATCH_ROWS
    ) -> Iterator[pa.RecordBatch]:
        """The query's full result, as Arrow batches of at most ``batch_rows``.

        The STREAM is deliberately not retried. A retry would restart it, and
        batches already yielded have been handed to the consumer — so a
        mid-stream failure has to surface rather than silently double-deliver.
        The requests before it are retried, which is where a transient failure is
        actually recoverable.
        """
        ready = self._submit(sql, database_id=database_id)
        if ready is None:
            return
        result_id, expected = ready
        received = 0
        batches = 0
        arrow = ArrowResultsApi(self._runtime.api)
        with arrow.stream_result_arrow(result_id, database_id) as reader:
            for batch in reader:
                received += batch.num_rows
                batches += 1
                yield from (_materialize_views(piece) for piece in _sliced(batch, batch_rows))
            if batches == 0:
                # An empty result is a schema with no batches: the IPC stream
                # carries the schema in its header and then ends. Yielding
                # nothing would hand dlt no items at all, so it would never
                # learn the columns and would neither create nor truncate the
                # destination table -- while `read_arrow` on the same query
                # returns a schemaful empty table. One zero-row batch keeps the
                # two paths saying the same thing.
                #
                # Built in the MATERIALISED schema, not the wire one, and not
                # normalised afterwards: pyarrow cannot construct an empty array
                # for every view layout it can describe (a dictionary of
                # string_view raises ArrowNotImplementedError), so converting the
                # schema first is the only order that works for all of them.
                yield _empty_batch(_materialized_schema(reader.schema))
        # After the stream is exhausted, never per batch: a short read is only
        # visible as a total, and raising mid-stream would leave the consumer
        # holding a prefix it believes is whole.
        _require_complete(result_id=result_id, expected=expected, received=received)

    def read_rows(self, sql: str, *, database_id: str) -> list[dict[str, Any]]:
        """The query's full result as plain dicts.

        For callers that transform row by row and do not want Arrow. Holds the
        whole result in memory, so the caller's query should carry its own
        ``LIMIT``.
        """
        return self.read_arrow(sql, database_id=database_id).to_pylist()

    # --- internals --------------------------------------------------------

    def _submit(self, sql: str, *, database_id: str) -> tuple[str, int] | None:
        """``(result_id, total_rows)`` for a ready result, or ``None``.

        ``None`` means the statement genuinely produced no result — and ONLY
        that. This is why the submit is spelled out here rather than delegated to
        the shared client's equivalent: that one collapses two different outcomes
        into ``None``. A synchronous response carries ``result_id: null`` either
        when there was nothing to persist OR when a result that fit inline could
        not be persisted for later retrieval. The second case has rows — they are
        sitting in the response body — and treating it as "no rows" loads an empty
        table for a query that returned data, which is precisely the silent loss
        this module exists to prevent. So the row count in the response decides
        which case it is, and the second one raises.
        """
        response = self._request_with_retry(
            lambda: QueryApi(self._runtime.api).query(
                QueryRequest(sql=sql), x_database_id=database_id
            )
        )
        if isinstance(response, QueryResponse):
            if response.result_id is not None:
                return self._await_result(response.result_id, database_id=database_id)
            if _describes_a_result(response):
                # A SELECT whose result was not persisted. Refused rather than
                # read from the inline body: that body is JSON, so its types are
                # the API's rendering rather than the engine's, and for a
                # truncated response it is a prefix. Refusing covers the
                # zero-row case too -- an empty SELECT still has a column shape
                # dlt needs, and returning "no result" there would leave the
                # destination table uncreated while the run reported success.
                raise UnverifiableReadError(
                    "the query produced a result but it could not be persisted, so it "
                    "cannot be read back in full"
                )
            return None
        if isinstance(response, AsyncQueryResponse):
            result_id = self._await_run_result_id(
                response.query_run_id, database_id=database_id
            )
            if result_id is None:
                return None
            return self._await_result(result_id, database_id=database_id)
        return None

    def _await_result(self, result_id: str, *, database_id: str) -> tuple[str, int]:
        """Wait for a result to be ready, and take its row count off the way out.

        RETRY IS PER REQUEST, NOT PER WAIT. Wrapping the whole wait would be
        wrong in a way that is easy to miss: ``_poll`` raises ``TimeoutError`` at
        its own 300s bound, ``classify_sdk_error`` calls that transient, and the
        retry budget would then multiply a five-minute wait by the number of
        attempts. Retrying each poll REQUEST gives a flaky 429 or 503 the budget
        it should have while leaving the overall wait bounded once.

        ``limit=0`` on every poll so the readiness check carries no rows — the
        default is unbounded, which would otherwise download the entire result
        body on each pass just to read a status field.

        The count comes from a second request rather than from the poll's own
        response because the total is a HEADER: reading it means the
        ``_with_http_info`` variant, whose wrapper puts the model one level down
        at ``.data``, and ``_poll`` inspects ``obj.status`` directly to spot a
        failed or cancelled result. Passing the wrapper in breaks that check, so
        the poll keeps the plain model and the header is fetched once, after
        ready.
        """
        results = ResultsApi(self._runtime.api)
        self._poll(
            lambda: self._request_with_retry(
                lambda: results.get_result(result_id, x_database_id=database_id, limit=0)
            ),
            is_ready=lambda r: r.status == "ready",
            describe=f"Result {result_id}",
        )
        response = self._request_with_retry(
            lambda: results.get_result_with_http_info(
                result_id, x_database_id=database_id, limit=0
            )
        )
        return result_id, _total_from_headers(response.headers, result_id=result_id)

    def _await_run_result_id(self, query_run_id: str, *, database_id: str) -> str | None:
        """The result id of an asynchronous query run, once it has succeeded.

        Same retry shape as :meth:`_await_result`, for the same reason.
        """
        runs = QueryRunsApi(self._runtime.api)
        run = self._poll(
            lambda: self._request_with_retry(
                lambda: runs.get_query_run(query_run_id, x_database_id=database_id)
            ),
            is_ready=lambda r: r.status == "succeeded",
            describe="Query",
        )
        return run.result_id


def _materialized(field_type: pa.DataType) -> pa.DataType:
    """``field_type`` with any Arrow view layout replaced by its materialised form.

    Recurses through list and struct children, because a view layout nested
    inside a struct is just as unsupported as a top-level one and is easier to
    miss.
    """
    if pat.is_string_view(field_type):
        return pa.string()
    if pat.is_binary_view(field_type):
        return pa.binary()
    # The child FIELD is carried through, not just its type. Rebuilding with
    # `pa.list_(value_type)` renames the child to `item` and makes it nullable,
    # so a list whose child is named or non-nullable would come back altered even
    # when it held no view at all -- a normaliser that changes schemas it was not
    # asked to touch.
    if pat.is_list_view(field_type):
        return pa.list_(_materialized_field(field_type.value_field))
    if pat.is_large_list_view(field_type):
        return pa.large_list(_materialized_field(field_type.value_field))
    if pat.is_list(field_type):
        return pa.list_(_materialized_field(field_type.value_field))
    if pat.is_large_list(field_type):
        return pa.large_list(_materialized_field(field_type.value_field))
    if pat.is_struct(field_type):
        return pa.struct([_materialized_field(f) for f in field_type])
    if pat.is_dictionary(field_type):
        # A dictionary-encoded text column is `dictionary<values=string_view>`,
        # and dlt rejects it for the same reason it rejects a bare string_view.
        # The encoding is not the problem; the value layout inside it is.
        return pa.dictionary(
            field_type.index_type,
            _materialized(field_type.value_type),
            field_type.ordered,
        )
    return field_type


def _materialized_field(field: pa.Field) -> pa.Field:
    """``field`` with its type materialised, keeping its name and nullability."""
    return field.with_type(_materialized(field.type))


def _materialized_schema(schema: pa.Schema) -> pa.Schema:
    """``schema`` with no view layouts left in it."""
    return pa.schema([_materialized_field(f) for f in schema])


def _empty_batch(schema: pa.Schema) -> pa.RecordBatch:
    """A zero-row batch in ``schema``.

    Arrays are built per field rather than through ``from_pylist``, which has no
    converter for some layouts even at length zero.
    """
    return pa.RecordBatch.from_arrays(
        [pa.array([], type=f.type) for f in schema], schema=schema
    )


def _has_list_view(field_type: pa.DataType) -> bool:
    """True when ``field_type`` contains a list-view layout at any depth."""
    if pat.is_list_view(field_type) or pat.is_large_list_view(field_type):
        return True
    if pat.is_list(field_type) or pat.is_large_list(field_type):
        return _has_list_view(field_type.value_type)
    if pat.is_struct(field_type):
        return any(_has_list_view(f.type) for f in field_type)
    if pat.is_dictionary(field_type):
        return _has_list_view(field_type.value_type)
    return False


def _materialize_views(data: T) -> T:
    """``data`` with no Arrow view layouts left in its schema.

    A no-op — same object, no copy — when the schema holds none, which is the
    common case for a result of numbers and timestamps.

    TWO PATHS, because ``cast`` does not handle all of them. String and binary
    views cast cleanly. A LIST view does not: pyarrow accepts the cast and
    produces an array whose offsets buffer is still sized for view semantics, so
    it passes value comparison and then fails validation the moment it is put in
    a table — a corrupt array that looks correct. Those columns are rebuilt
    through Python instead, which is slow and is the only form measured to
    produce a valid array. The engine has not been observed to return list views;
    this exists so that if it starts, the result is right rather than subtly
    broken.
    """
    schema = _materialized_schema(data.schema)
    if schema.equals(data.schema):
        return data
    if not any(_has_list_view(f.type) for f in data.schema):
        return data.cast(schema)
    columns = [
        _rebuilt(data.column(i), schema.field(i).type) for i in range(data.num_columns)
    ]
    if isinstance(data, pa.RecordBatch):
        return pa.RecordBatch.from_arrays(columns, schema=schema)
    return pa.Table.from_arrays(columns, schema=schema)


def _rebuilt(column: object, target: pa.DataType) -> object:
    """``column`` as ``target``, built from Python values rather than cast."""
    if isinstance(column, pa.ChunkedArray):
        return pa.chunked_array(
            [pa.array(chunk.to_pylist(), type=target) for chunk in column.chunks], type=target
        )
    return pa.array(column.to_pylist(), type=target)


def _describes_a_result(response: QueryResponse) -> bool:
    """Whether a synchronous response describes a result set at all.

    Columns are the distinguisher, not rows. A statement with nothing to return
    (a SET, a DDL) has no columns; a SELECT has them whether or not it matched
    any row. Keying on the row count instead would classify an empty SELECT as
    "no result", which is the one case where the shape matters more than the
    contents.
    """
    return bool(getattr(response, "columns", None)) or _response_row_count(response) > 0


def _response_row_count(response: QueryResponse) -> int:
    """Rows the synchronous response reports carrying.

    ``preview_row_count`` is the current field; ``row_count`` is its deprecated
    alias and is read as a fallback so an older server still answers the
    question this is asked for.
    """
    for attribute in ("preview_row_count", "row_count"):
        value = getattr(response, attribute, None)
        if value is not None:
            return int(value)
    rows = getattr(response, "rows", None) or []
    return len(rows)


def _total_from_headers(headers: object, *, result_id: str) -> int:
    """The authoritative row count out of a result response's headers.

    Both casings are tried because header maps differ in whether they are
    case-insensitive, and a lookup that quietly misses would read as "no header"
    and fail an otherwise healthy read.
    """
    get = getattr(headers, "get", None)
    total = None
    if callable(get):
        total = get("X-Total-Row-Count") or get("x-total-row-count")
    if total is None:
        raise UnverifiableReadError(
            "the response carried no X-Total-Row-Count, so the number of rows the "
            "read should have produced is unknown",
            result_id=result_id,
        )
    return int(total)


def _require_complete(*, result_id: str, expected: int, received: int) -> None:
    if received != expected:
        raise IncompleteReadError(result_id=result_id, expected=expected, received=received)


def _sliced(batch: pa.RecordBatch, batch_rows: int) -> Iterator[pa.RecordBatch]:
    """``batch`` in pieces of at most ``batch_rows`` rows.

    The server chooses its own batch sizes; this only ever splits, never
    combines, so the row count the caller sees is unchanged.
    """
    if batch_rows <= 0 or batch.num_rows <= batch_rows:
        yield batch
        return
    for offset in range(0, batch.num_rows, batch_rows):
        yield batch.slice(offset, batch_rows)


__all__ = [
    "DEFAULT_BATCH_ROWS",
    "HotdataSourceClient",
    "IncompleteReadError",
    "UnverifiableReadError",
]
