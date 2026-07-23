from __future__ import annotations

import pyarrow as pa
from hotdata.arrow import ResultsApi as ArrowResultsApi
from hotdata_framework.databases import ManagedDatabase
from hotdata_framework.managed_client import ManagedDatabaseClient

from hotdata_dlt_destination.errors import HotdataTerminalError


def _is_forbidden(exc: Exception) -> bool:
    """True when ``exc`` wraps a 403 (create-scoped keys can't read /databases)."""
    return getattr(getattr(exc, "__cause__", None), "status", None) == 403


class HotdataClient(ManagedDatabaseClient):
    """Managed-database client used by the dlt destination.

    Adds two things on top of the shared ``hotdata_framework`` client:

    * **Cross-run schema evolution** — when a later run requires a table the
      database is missing, the table is declared in place via
      ``add_managed_table`` (added empty, populated by the subsequent load); no
      data is moved and existing tables, including dlt's ``_dlt_version`` /
      ``_dlt_loads`` / ``_dlt_pipeline_state`` bookkeeping, are left untouched.
    * **Collision-safe, resolve-once addressing** — a database name is resolved
      to its record once per run (cached via :meth:`bind_run_cache`) and every
      subsequent operation addresses the database by id. Resolution raises on an
      ambiguous name instead of silently taking the first match.
    """

    # Run-scoped store bound via bind_run_cache(); resolution is cached on it so
    # the whole run reuses one resolved/created record.
    _run_cache: object | None = None

    def bind_run_cache(self, cache: object) -> None:
        """Bind a run-scoped store so a database resolves to its record once.

        ``cache`` is any object that tolerates a ``_hotdata_resolved_db``
        attribute — in practice the shared ``HotdataClientConfiguration``
        instance, which every client built for a run points at.
        """
        self._run_cache = cache

    # --- resolution -------------------------------------------------------

    def _collision_safe_resolve(self, name_or_id: str) -> ManagedDatabase:
        """Resolve a name/id to its record, raising on an ambiguous name.

        Hotdata database names are not unique. Taking the first match can read,
        write, or drop the wrong database, so a name that matches more than one
        database raises instead. An id (matched exactly) is unambiguous.
        """
        databases = self._request_with_retry(self._runtime.list_managed_databases)
        by_name = [db for db in databases if db.description == name_or_id]
        if len(by_name) > 1:
            raise HotdataTerminalError(
                f"Managed database name {name_or_id!r} is ambiguous: "
                f"{len(by_name)} databases share it (ids: {sorted(db.id for db in by_name)}). "
                "Address it by id to disambiguate."
            )
        if by_name:
            return by_name[0]
        by_id = [db for db in databases if db.id == name_or_id]
        if by_id:
            return by_id[0]
        raise KeyError(name_or_id)

    def _resolve(self, name_or_id: str) -> ManagedDatabase:
        """Resolve once per run, then serve the cached (id-addressable) record."""
        cache = self._run_cache
        if cache is not None:
            cached = getattr(cache, "_hotdata_resolved_db", None)
            if cached is not None and name_or_id in (
                cached.id,
                getattr(cached, "description", None),
            ):
                return cached
        db = self._collision_safe_resolve(name_or_id)
        self._cache_db(db)
        return db

    def _cache_db(self, db: ManagedDatabase | None) -> None:
        if self._run_cache is not None:
            self._run_cache._hotdata_resolved_db = db

    # --- lifecycle --------------------------------------------------------

    def ensure_managed_database(
        self,
        name: str,
        *,
        schema: str,
        tables: list[str],
        keys: dict[str, list[str]] | None = None,
        create_if_missing: bool,
    ) -> ManagedDatabase:
        # keys: table name -> key columns (enables delete/update/upsert on it)
        keys = keys or {}

        try:
            db = self._resolve(name)
        except KeyError:
            if not create_if_missing:
                raise
            return self._create_and_cache(name, schema=schema, tables=tables, keys=keys)
        except HotdataTerminalError as exc:
            # A create/upload/query-scoped key is forbidden from reading /databases,
            # so it can't check existence; attempt the create it is permitted to make.
            if not (create_if_missing and _is_forbidden(exc)):
                raise
            return self._create_and_cache(name, schema=schema, tables=tables, keys=keys)

        existing = {
            managed_table.table
            for managed_table in self._request_with_retry(
                lambda: self._runtime.list_managed_tables(db, schema=schema)
            )
        }
        # Declare any newly-required tables additively, in place, carrying their
        # key. dlt calls ``initialize_storage`` with the full table set before any
        # load job runs, so by load time this is normally a no-op.
        for table in sorted(set(tables) - existing):
            self._add_managed_table(db, table, schema=schema, key=keys.get(table))
        return db

    def _create_and_cache(
        self, name: str, *, schema: str, tables: list[str], keys: dict[str, list[str]]
    ) -> ManagedDatabase:
        db = self._request_with_retry(
            lambda: self._runtime.create_managed_database(
                description=name, schema=schema, tables=sorted(set(tables)), keys=keys
            )
        )
        self._cache_db(db)
        return db

    def _add_managed_table(
        self,
        database: str | ManagedDatabase,
        table: str,
        *,
        schema: str,
        key: list[str] | None = None,
    ) -> None:
        self._request_with_retry(
            lambda: self._runtime.add_managed_table(database, table, schema=schema, key=key)
        )

    def drop_managed_database(self, name: str) -> None:
        """Delete the managed database if it exists (used for dlt dev_mode / refresh)."""
        try:
            db = self._resolve(name)
        except KeyError:
            return
        self._request_with_retry(lambda: self._runtime.delete_managed_database(db))
        self._cache_db(None)

    def resolve_managed_database(self, name: str) -> ManagedDatabase:
        """Resolve a managed database by display name (or id) to its record.

        Raises ``KeyError`` when nothing matches and ``HotdataTerminalError`` when
        the name is shared by more than one database.
        """
        return self._resolve(name)

    def load_managed_table(self, database: str, table: str, **kwargs):
        """Load parquet into a managed table via the resolved database record.

        Passing the resolved ``ManagedDatabase`` (not its id) lets a create-scoped
        key load without a further read probe (framework passthrough)."""
        db = self._resolve(database)
        return super().load_managed_table(db, table, **kwargs)

    def execute_sql(self, sql: str, *, database: str) -> pa.Table:
        """Run a SQL query scoped to ``database`` and return the result as Arrow.

        Resolves the managed database to its id (once per run), submits the query,
        polls until the result is ready, and fetches it as a ``pyarrow.Table``. An
        empty table is returned when the query produces no out-of-band result.
        """

        def operation() -> pa.Table:
            db = self._resolve(database)
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
        db = self._resolve(database)
        return self._request_with_retry(
            lambda: self._runtime.list_managed_tables(db, schema=schema)
        )

    def table_is_synced(self, database: str, table: str, *, schema: str) -> bool:
        db = self._resolve(database)
        for managed_table in self._request_with_retry(
            lambda: self._runtime.list_managed_tables(db, schema=schema)
        ):
            if managed_table.table == table:
                return managed_table.synced
        return False

    def fetch_table(self, *, database: str, schema: str, table: str) -> pa.Table | None:
        def operation() -> pa.Table | None:
            db = self._resolve(database)
            if not self._table_is_synced_for(db, table, schema=schema):
                return None
            sql = f'SELECT * FROM "default"."{schema}"."{table}"'
            result_id = self._query_database_scoped(sql, database_id=db.id)
            if result_id is None:
                return None
            return self._fetch_result_arrow(result_id, database_id=db.id)

        return self._request_with_retry(operation)

    def _table_is_synced_for(self, db: ManagedDatabase, table: str, *, schema: str) -> bool:
        for managed_table in self._runtime.list_managed_tables(db, schema=schema):
            if managed_table.table == table:
                return managed_table.synced
        return False


__all__ = ["HotdataClient"]
