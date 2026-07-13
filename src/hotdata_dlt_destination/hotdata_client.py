from __future__ import annotations

import pyarrow as pa
from hotdata.arrow import ResultsApi as ArrowResultsApi
from hotdata_framework.databases import ManagedDatabase
from hotdata_framework.managed_client import ManagedDatabaseClient


class HotdataClient(ManagedDatabaseClient):
    """Managed-database client used by the dlt destination.

    Adds cross-run schema evolution on top of the shared ``hotdata_framework``
    client. The base client only creates a managed database with its initial
    tables; this override additionally reconciles tables on an already-existing
    database. When a later run requires a table that the database is missing,
    the table is declared in place via ``add_managed_table`` (the table is added
    empty and populated by the subsequent load) — no data is moved and existing
    tables, including dlt's ``_dlt_version`` / ``_dlt_loads`` /
    ``_dlt_pipeline_state`` bookkeeping, are left untouched.
    """

    def ensure_managed_database(
        self,
        name: str,
        *,
        schema: str,
        tables: list[str],
        create_if_missing: bool,
    ) -> ManagedDatabase:
        runtime = self._runtime

        # Resolve is called directly (not via _request_with_retry) so its KeyError
        # "not found" signal is preserved rather than mapped to a terminal error.
        try:
            db = runtime.resolve_managed_database(name)
        except KeyError:
            if not create_if_missing:
                raise
            return self._request_with_retry(
                lambda: runtime.create_managed_database(
                    description=name, schema=schema, tables=sorted(set(tables))
                )
            )

        existing = {
            managed_table.table
            for managed_table in self._request_with_retry(
                lambda: runtime.list_managed_tables(name, schema=schema)
            )
        }
        # Declare any newly-required tables additively, in place. dlt calls
        # ``initialize_storage`` with the full table set before any load job runs,
        # so by load time this is normally a no-op.
        for table in sorted(set(tables) - existing):
            self._add_managed_table(name, table, schema=schema)
        return db

    def _add_managed_table(self, name: str, table: str, *, schema: str) -> None:
        runtime = self._runtime
        self._request_with_retry(lambda: runtime.add_managed_table(name, table, schema=schema))

    def drop_managed_database(self, name: str) -> None:
        """Delete the managed database if it exists (used for dlt dev_mode / refresh)."""
        runtime = self._runtime
        try:
            db = runtime.resolve_managed_database(name)
        except KeyError:
            return
        self._request_with_retry(lambda: runtime.delete_managed_database(db.id))

    def resolve_managed_database(self, name: str) -> ManagedDatabase:
        """Resolve a managed database by display name to its record (carrying ``.id``).

        Delegates to the runtime client, preserving its ``KeyError`` "not found" signal.
        """
        return self._runtime.resolve_managed_database(name)

    def execute_sql(self, sql: str, *, database: str) -> pa.Table:
        """Run a SQL query scoped to ``database`` and return the result as Arrow.

        The read/dataset interface goes through here. The base client has no
        general query entrypoint of its own — only the private database-scoped
        submit + Arrow fetch that :meth:`fetch_table` uses — so this mirrors that
        dance for arbitrary SQL: resolve the managed database name to its id,
        submit the query, poll until the result is ready, and fetch it as a
        ``pyarrow.Table``. An empty table is returned when the query produces no
        out-of-band result (e.g. a statement with no result set).
        """

        def operation() -> pa.Table:
            db = self._runtime.resolve_managed_database(database)
            result_id = self._query_database_scoped(sql, database_id=db.id)
            if result_id is None:
                return pa.table({})
            # Results of database-scoped queries are database-scoped; the
            # hotdata 0.6.0 SDK requires the scope on the Arrow fetch.
            return ArrowResultsApi(self._runtime.api).get_result_arrow(
                result_id, x_database_id=db.id
            )

        return self._request_with_retry(operation)

    def list_managed_tables(self, database: str, *, schema: str) -> list:
        """List the managed tables in ``database``/``schema`` (used by ``has_dataset``)."""
        runtime = self._runtime
        return self._request_with_retry(
            lambda: runtime.list_managed_tables(database, schema=schema)
        )


__all__ = ["HotdataClient"]
