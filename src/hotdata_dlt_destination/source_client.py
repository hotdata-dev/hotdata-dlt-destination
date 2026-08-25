"""Read transport for using Hotdata as a dlt *source*.

Turns a SQL string into every row the query produced, or raises. The
"or raises" is the whole point of this module, and it is why the read does not
use the obvious response body.

WHY THE INLINE RESPONSE IS NOT ENOUGH ON ITS OWN. ``POST /v1/query`` answers
synchronously with a **bounded preview** — the server caps it (10k rows / 8 MiB by
default) and there is no request parameter to lift the cap. A capped response is
not an error: it carries ``truncated: true`` and a ``result_id`` naming the full
result, which was streamed to storage in its entirety. Reading rows out of a
capped body therefore yields the head of a result while looking exactly like a
complete one.

So the body is used only when it says, affirmatively, that it is the whole result
AND its own row count agrees. :meth:`HotdataSourceClient.read_rows` does that and
falls back to the persisted result otherwise; :meth:`read_arrow` and
:meth:`read_arrow_batches` always use the persisted result, because their contract
is engine types that do not vary with the size of the answer. Neither ever trusts
a body it could not verify.

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
and readiness is polled — measured against a live workspace, four requests and
about 2.2s for a 10,000-row page against roughly 0.8s for the one-request body
route. That is why the body is preferred where it can be trusted, and why the
Arrow methods, which cannot use it, are the slower pair. Where the body cannot be
trusted the persisted result is taken regardless of cost: slower and complete
beats faster and silently short.
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
    """Runs read-only SQL against an instant database and returns every row.

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
        return self._read_ready(ready, database_id=database_id)

    def _read_ready(self, ready: tuple[str, int], *, database_id: str) -> pa.Table:
        """Fetch a ready result, refuse it if the row count disagrees, normalise it.

        One place, because the check is the correctness-critical part and both the
        Arrow path and :meth:`read_rows`' fallback need it. The retry boundary is
        the reason it cannot simply be inlined at each call site: the base client
        maps everything raised under ``_request_with_retry`` through
        ``classify_sdk_error``, so an assertion raised in there would reach the
        caller as a generic transport error, indistinguishable from a network
        fault — and would be retried, which turns a refusal into a delay.
        """
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

        TAKES THE RESPONSE BODY WHEN IT IS THE WHOLE RESULT, AND ONLY THEN.
        ``truncated`` is documented as "True when ``rows`` is a bounded preview of
        a larger result", so an affirmative ``False`` is the API saying the body is
        everything. That is checked as ``is False`` rather than for falsiness: the
        field was added in a later SDK than the endpoint, and a missing or null
        value read as "not truncated" would hand back the head of a result as the
        whole of it — the one outcome this module exists to prevent.

        The body's own ``total_row_count`` is then compared against the rows it
        carries. When that field is absent the body cannot be verified, and this
        method does NOT fall back on the boolean alone: it takes the persisted
        result instead. Both fields failing open at once is what would otherwise
        turn a response carrying no rows into a successful empty read, which under
        ``write_disposition="replace"`` truncates a destination table and reports
        success.

        Measured against a live workspace: the body route is one request and about
        0.8s for 10,000 rows, the persisted route four requests and about 2.2s. The
        persisted route is where the cost is earned — an inline answer is capped by
        a BYTE budget, so a wide table can cap at a few hundred rows while the same
        query on a narrow one returns ten thousand.

        THE TWO PATHS TYPE VALUES DIFFERENTLY, and that is the trade this method
        makes deliberately. The body is JSON, so a timestamp arrives as text; the
        persisted result is Arrow, so the same timestamp arrives as a datetime.
        Dicts are already a lossier representation than Arrow, and a caller asking
        for them has accepted that — but a caller that needs one stable set of
        types must use :meth:`read_arrow` or :meth:`read_arrow_batches`, which
        always go to the persisted result and never vary.
        """
        response = self._query(sql, database_id=database_id)
        if isinstance(response, QueryResponse) and response.truncated is False:
            rows = _rows_from_response(response)
            if rows is not None:
                return rows
        # Either the body is a preview, or it could not be verified. Both are
        # answered by the persisted result, which this response already names.
        ready = self._resolve(response, database_id=database_id)
        if ready is None:
            return []
        return self._read_ready(ready, database_id=database_id).to_pylist()

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
        return self._resolve(self._query(sql, database_id=database_id), database_id=database_id)

    def _query(self, sql: str, *, database_id: str) -> object:
        """Run the query and hand back the raw response, whatever shape it took."""
        return self._request_with_retry(
            lambda: QueryApi(self._runtime.api).query(
                QueryRequest(sql=sql), x_database_id=database_id
            )
        )

    def _resolve(self, response: object, *, database_id: str) -> tuple[str, int] | None:
        """A ready ``(result_id, total_rows)`` for a response, or ``None``.

        Split from :meth:`_query` so a caller that has already read the response —
        :meth:`read_rows`, deciding whether it needs the persisted result at all —
        can reach the persisted result without submitting the query a second time.
        """
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
                # A succeeded run naming no result. Unlike the synchronous branch
                # there are no columns here to tell "nothing to return" from "a
                # result that could not be persisted", so nothing establishes the
                # rowless case and the silent outcome has no basis.
                raise UnverifiableReadError(
                    "the query run succeeded but named no result, so whether it "
                    "produced rows cannot be established"
                )
            return self._await_result(result_id, database_id=database_id)
        # Neither response model. Reading this as "no rows" would let an SDK
        # change turn every read into a silent empty one — and under
        # `write_disposition="replace"` an empty read TRUNCATES the destination
        # and reports success. The pinned SDK cannot reach here; a version bump
        # is exactly what would, and it would do so with no test failing.
        raise UnverifiableReadError(
            f"the query returned an unrecognised response ({type(response).__name__}), "
            "so whether it produced rows cannot be established"
        )

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
    # Checked before the plain list arms: both have their own Arrow type ids, so
    # `is_list` is False for them and they would otherwise fall straight through
    # unconverted — the same gap struct and dictionary were each added to close.
    # Rebuilding a map as a plain list would change its type, not just its
    # layout, so it needs its own arm rather than sharing one.
    if pat.is_fixed_size_list(field_type):
        return pa.list_(
            _materialized_field(field_type.value_field), field_type.list_size
        )
    if pat.is_map(field_type):
        key_field = _materialized_field(field_type.key_field)
        item_field = _materialized_field(field_type.item_field)
        if key_field.equals(field_type.key_field) and item_field.equals(
            field_type.item_field
        ):
            # Nothing inside needed materialising, so hand back the original
            # rather than an equivalent. `pa.map_` cannot carry the entries field
            # through — it always names it `entries` — and field names take part
            # in Arrow type equality, so rebuilding a view-free map would make it
            # compare as changed and send the column through a cast it did not
            # need. It also refuses a nullable key field outright
            # (`TypeError: Map key field should be non-nullable`), which would
            # fail inside the normaliser for a map that was only passing through.
            return field_type
        return pa.map_(key_field, item_field, keys_sorted=field_type.keys_sorted)
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
    if pat.is_fixed_size_list(field_type):
        return _has_list_view(field_type.value_type)
    if pat.is_map(field_type):
        return _has_list_view(field_type.key_type) or _has_list_view(field_type.item_type)
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
    # Only the list-view columns take the Python rebuild; everything else casts.
    # A normaliser must not reshape a column it was not asked to touch, and the
    # rebuild is the slow path — applying it to a whole table because one column
    # needs it spends that cost on every other column for nothing.
    columns = [
        _rebuilt(data.column(i), schema.field(i).type)
        if _has_list_view(data.schema.field(i).type)
        else data.column(i).cast(schema.field(i).type)
        for i in range(data.num_columns)
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


def _rows_from_response(response: QueryResponse) -> list[dict[str, Any]] | None:
    """The rows of an uncapped response as dicts, or ``None`` if unverifiable.

    ``None`` means "do not use this body" — the caller has a correct route
    available (the persisted result) and should take it, so this reports rather
    than raises. The body is usable only when ``total_row_count`` is present and
    agrees with the rows carried; that is the same contract the persisted path
    holds against ``X-Total-Row-Count``, and leaving it unchecked would rest the
    whole guarantee on one boolean.

    A body whose count is present and DISAGREES is a different thing: the response
    contradicts itself, and no other route is going to resolve that, so it raises.

    The wire shape is positional — the SDK types ``rows`` as a list of lists and
    ``columns`` as a list of names — so ``zip`` is the real path and the dict
    branch covers servers or fixtures that pre-zip it. ``strict=True`` because a
    row of the wrong width means the two arrays do not describe each other, and
    silently dropping the tail would produce rows missing columns that the row
    COUNT cannot detect.
    """
    total = getattr(response, "total_row_count", None)
    if total is None:
        return None
    columns, rows = response.columns or [], response.rows or []
    out: list[dict[str, Any]] = (
        list(rows)
        if rows and isinstance(rows[0], dict)
        else [dict(zip(columns, row, strict=True)) for row in rows]
    )
    if len(out) != int(total):
        raise IncompleteReadError(
            result_id=response.result_id or "(unpersisted)",
            expected=int(total),
            received=len(out),
        )
    return out


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
    try:
        return int(total)
    except (TypeError, ValueError) as exc:
        # Same outcome as an absent header, and deliberately the same error: a
        # caller catching this module's two errors to tell "could not verify"
        # from a real fault would otherwise miss a bare ValueError from int().
        raise UnverifiableReadError(
            f"X-Total-Row-Count was {total!r}, which is not a row count",
            result_id=result_id,
        ) from exc


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
