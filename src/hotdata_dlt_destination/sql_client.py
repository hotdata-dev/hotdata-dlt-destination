"""dlt SQL client (read / dataset interface) for the Hotdata destination.

Adapts dlt's DB-API-shaped read stack (connection -> cursor -> ``.df()`` /
``.arrow()``) onto Hotdata's REST query surface. ``HotdataSqlClient`` wraps
``HotdataClient.execute_sql`` (submit SQL -> poll -> fetch Arrow); ``HotdataCursor``
exposes the resulting ``pyarrow.Table`` through the cursor contract dlt expects.

No embedded engine: Hotdata runs Apache DataFusion server-side, so we translate
rather than execute anything locally. See ``docs/sql-client-spec.md``.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, AnyStr, ClassVar

import pyarrow as pa
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
)
from dlt.destinations.typing import DBApi, DBTransaction

from hotdata_dlt_destination.errors import HotdataTerminalError, HotdataTransientError
from hotdata_dlt_destination.hotdata_client import HotdataClient

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dlt.common.destination.dataset import DBApiCursor

    from hotdata_dlt_destination.configuration import HotdataClientConfiguration

# Substrings that identify a "table/relation does not exist" error across the
# Postgres surface DataFusion presents (matched case-insensitively).
_UNDEFINED_RELATION_MARKERS = (
    "not found",
    "does not exist",
    "no such table",
    "no table named",
    "unknown table",
    "undefined table",
)


def _chain_messages(ex: Exception, limit: int = 6) -> str:
    """Join the messages of ``ex`` and its ``__cause__`` chain (lowercased).

    The SDK collapses the underlying ``ApiException`` to ``"400: Bad Request"``,
    so the descriptive engine message (e.g. ``table '...' not found``) only
    appears deeper in the cause chain — scan the whole chain, not just ``ex``.
    """
    parts: list[str] = []
    current: BaseException | None = ex
    seen = 0
    while current is not None and seen < limit:
        parts.append(str(current))
        current = current.__cause__
        seen += 1
    return " ".join(parts).lower()


class HotdataCursor(DBApiCursorImpl):
    """DB-API cursor over a ``pyarrow.Table``.

    ``DBApiCursorImpl`` provides ``.df()`` / ``.arrow()`` / ``iter_df`` /
    ``iter_arrow`` on top of a native cursor exposing ``description`` + ``fetch*``.
    We are that native cursor (``native_cursor is self``) and hand back the Arrow
    table directly, avoiding the base's row-tuple -> Arrow inference and preserving
    the engine's types. The ``fetch*`` surface backs the ``execute_sql`` / dataset
    helper path (e.g. ``row_counts``).
    """

    def __init__(self, table: pa.Table) -> None:
        self._table = table
        self._materialized_rows: list[tuple[Any, ...]] | None = None
        self._pos = 0
        super().__init__(self)  # native_cursor is self

    @property
    def _rows(self) -> list[tuple[Any, ...]]:
        # Row tuples back only the fetch* surface (execute_sql / row_counts). The
        # primary .df()/.arrow() path yields the Arrow table directly and never
        # touches these, so materialize lazily on first fetch to avoid the
        # column-major -> row-tuple copy when it isn't needed.
        if self._materialized_rows is None:
            self._materialized_rows = (
                list(zip(*(col.to_pylist() for col in self._table.columns), strict=True))
                if self._table.num_columns
                else []
            )
        return self._materialized_rows

    @property
    def description(self) -> list[tuple[Any, ...]]:
        return [(name, None, None, None, None, None, None) for name in self._table.column_names]

    # DB-API surface. These are defined (not inherited) so DBApiCursorImpl's
    # delegating wrappers resolve to real implementations instead of recursing
    # back into ``self.native_cursor`` (which is ``self``).
    def execute(self, *args: Any, **kwargs: Any) -> None:
        # The result is already materialized at construction; nothing to run.
        pass

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rows

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        end = len(self._rows) if size is None else self._pos + size
        rows = self._rows[self._pos : end]
        self._pos = min(end, len(self._rows))
        return rows

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def close(self, *args: Any, **kwargs: Any) -> None:
        pass

    # Arrow-native: exact engine types, no row -> Arrow inference.
    def iter_arrow(self, chunk_size: int | None = None) -> Iterator[pa.Table]:
        yield self._table

    def iter_df(self, chunk_size: int | None = None) -> Iterator[Any]:
        yield self._table.to_pandas()


class HotdataSqlClient(SqlClientBase[HotdataClient]):
    """Adapter exposing the Hotdata managed database through dlt's SQL client.

    A dlt "dataset" maps to the Hotdata **schema**; the managed **database** is a
    separate scoping dimension passed out-of-band on each query (not in the SQL).
    Table references qualify as ``"default"."<schema>"."<table>"``.
    """

    dbapi: ClassVar[DBApi] = None

    def __init__(
        self,
        managed_database: str,
        schema: str,
        capabilities: DestinationCapabilitiesContext,
        config: HotdataClientConfiguration,
    ) -> None:
        super().__init__(
            database_name=managed_database,  # only the execute_sql(database=) scope
            dataset_name=schema,  # "public" -> default.public.<table>
            staging_dataset_name=schema,  # no staging; mirror dataset_name
            capabilities=capabilities,
        )
        self._config = config
        self._client: HotdataClient | None = None

    # --- abstract methods -------------------------------------------------

    def open_connection(self) -> HotdataClient:
        self._client = HotdataClient(
            api_key=self._config.credentials.api_key,
            workspace_id=self._config.credentials.workspace_id,
            api_base_url=self._config.api_base_url,
            max_retries=self._config.max_retries,
            retry_backoff_seconds=self._config.retry_backoff_seconds,
        )
        return self._client

    def close_connection(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    @property
    def native_connection(self) -> HotdataClient | None:
        return self._client

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        assert isinstance(query, str)
        if args or kwargs:
            # Hotdata's query API takes a plain SQL string with no bind protocol.
            # The fluent/raw read path passes literal SQL (no args); base helpers
            # that would bind params are avoided by overriding has_dataset.
            raise DatabaseTerminalException(
                NotImplementedError("HotdataSqlClient does not support parameterized queries")
            )
        table = self._client.execute_sql(query, database=self.database_name)
        yield HotdataCursor(table)

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Sequence[Sequence[Any]] | None:
        with self.execute_query(sql, *args, **kwargs) as cursor:
            if cursor.description is None:
                return None
            return cursor.fetchall()

    @contextmanager
    def begin_transaction(self) -> Iterator[DBTransaction]:
        # DataFusion has no transactions (supports_ddl_transactions=False).
        yield self  # type: ignore[misc]

    @staticmethod
    def _make_database_exception(ex: Exception) -> Exception:
        if isinstance(
            ex, (DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation)
        ):
            return ex
        message = _chain_messages(ex)
        if any(marker in message for marker in _UNDEFINED_RELATION_MARKERS):
            return DatabaseUndefinedRelation(ex)
        if isinstance(ex, HotdataTransientError):
            return DatabaseTransientException(ex)
        if isinstance(ex, HotdataTerminalError):
            return DatabaseTerminalException(ex)
        return DatabaseTerminalException(ex)

    # --- overridden concrete methods -------------------------------------

    def catalog_name(self, quote: bool = True, casefold: bool = True) -> str | None:
        # Hotdata's catalog is always "default"; drives default.public.<table>.
        catalog = "default"
        if casefold:
            catalog = self.capabilities.casefold_identifier(catalog)
        if quote:
            catalog = self.capabilities.escape_identifier(catalog)
        return catalog

    def has_dataset(self) -> bool:
        # Check via the managed-DB API rather than the base's INFORMATION_SCHEMA.SCHEMATA
        # query (which binds %s params our query API can't take).
        try:
            self._client.list_managed_tables(self.database_name, schema=self.dataset_name)
            return True
        except (KeyError, HotdataTerminalError, HotdataTransientError):
            return False


__all__ = ["HotdataCursor", "HotdataSqlClient"]
