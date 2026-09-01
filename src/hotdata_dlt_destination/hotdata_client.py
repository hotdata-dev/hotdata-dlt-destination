from __future__ import annotations

from collections.abc import Collection
from typing import Any

import pyarrow as pa
from dlt.common import logger
from hotdata.api.databases_api import DatabasesApi
from hotdata.arrow import ResultsApi as ArrowResultsApi
from hotdata_framework.databases import ManagedDatabase, managed_database_from_detail
from hotdata_framework.managed_client import ManagedDatabaseClient

from hotdata_dlt_destination.errors import HotdataTerminalError


def _is_not_found(exc: Exception) -> bool:
    """True when ``exc`` or anything in its ``__cause__`` chain wraps a 404.

    The framework raises the mapped error ``from`` the underlying ``ApiException``,
    so today the 404 sits one level down -- but it walks the whole chain (mirroring
    ``sql_client._chain_messages``) so a dropped-id 404 is still recognised if the
    framework ever wraps ``get_database`` errors in an extra layer.
    """
    current: BaseException | None = exc
    for _ in range(6):
        if current is None:
            break
        if getattr(current, "status", None) == 404:
            return True
        current = current.__cause__
    return False


class HotdataClient(ManagedDatabaseClient):
    """Instant-database client used by the dlt destination.

    Addressing is **id-first**: a Hotdata database is identified by its id, never
    by name. The name (``description``) is only a display label supplied when a
    database is created — it is never used to look one up, because Hotdata names
    are not unique. Concretely:

    * **Bind an existing database by id.** When a ``database_id`` is configured,
      the record is fetched once via ``GET /databases/{id}`` and every operation
      addresses the database by that record. No listing, no name scan.
    * **Create on first run.** With no ``database_id`` and ``create_if_missing``,
      the database is created (labelled with ``database_name``) and its new id is
      logged so it can be pinned via ``database_id`` for subsequent runs.
    * **Cross-run schema evolution.** When binding an existing database, tables it
      is missing are declared in place via ``add_managed_table`` (added empty,
      populated by the subsequent load); no data is moved and existing tables,
      including dlt's ``_dlt_version`` / ``_dlt_loads`` / ``_dlt_pipeline_state``
      bookkeeping, are left untouched.

    The resolved record is cached once per run (via :meth:`bind_run_cache`) so the
    whole run reuses a single bind/create and create-scoped keys never issue a read
    they are not permitted to make.
    """

    # Run-scoped configuration bound via bind_run_cache(); the resolved database
    # and its provenance are cached on it so the whole run reuses one record.
    _config: object | None = None

    # Tables this client declared for the first time on the most recent
    # ensure_managed_database(). Declared at class level so reading it before that
    # call yields an empty set instead of AttributeError -- the consumer seeds from
    # it, and a missing attribute would silently skip seeding rather than fail.
    # ensure_managed_database rebinds a fresh instance set; this default is
    # immutable so an in-place update on a client that declared nothing cannot
    # leak across instances.
    newly_declared: Collection[str] = frozenset()

    def bind_run_cache(self, cache: object) -> None:
        """Bind the run's shared configuration.

        ``cache`` is the shared ``HotdataClientConfiguration`` instance every
        client built for a run points at. Its ``database_id`` / ``database_name``
        drive id-first resolution, and the resolved record is cached back on it
        (``_hotdata_db``) so a single run resolves the database exactly once.
        """
        self._config = cache

    # --- run cache --------------------------------------------------------

    def _cached_db(self) -> ManagedDatabase | None:
        return getattr(self._config, "_hotdata_db", None) if self._config is not None else None

    def _cache_db(self, db: ManagedDatabase | None, *, created: bool) -> None:
        if self._config is not None:
            self._config._hotdata_db = db
            self._config._hotdata_db_created = created
            if db is None:
                # The catalog belongs to the record; outliving it would leave a
                # stale name for whatever is resolved next.
                self._config._hotdata_catalog = None

    def _cache_catalog(self, catalog: str | None) -> None:
        if self._config is not None:
            self._config._hotdata_catalog = catalog

    @property
    def catalog(self) -> str:
        """Name this database's own catalog answers to inside its query scope.

        `default` unless the database was created with a catalog override, so it
        cannot be hardcoded: a database created as e.g. `mqtt_tf2` does not answer
        to `default`, and every qualified reference against it fails to resolve.
        The id-first bind reads the real name off the record, so this RESOLVES
        rather than reading a cache: returning the `default` fallback merely
        because nothing had been resolved yet is the whole bug, one caller
        further out. A database this run CREATED never carries an override, so
        `default` is correct there, and it stays the fallback.

        Raises ``KeyError`` when no database is configured or resolved -- the same
        signal ``_require_db`` gives, which callers already read as "nothing
        stored yet".
        """
        cached = getattr(self._config, "_hotdata_catalog", None)
        if cached:
            return cached
        self._require_db()
        return getattr(self._config, "_hotdata_catalog", None) or "default"

    def _was_created(self) -> bool:
        return bool(getattr(self._config, "_hotdata_db_created", False))

    # --- resolution (id-first, never by name) -----------------------------

    def _bind_by_id(self, database_id: str) -> ManagedDatabase:
        """Fetch a database record by id (``GET /databases/{id}``).

        Raises ``KeyError`` when the id does not exist (404); other API errors
        propagate as ``HotdataTerminalError``/``HotdataTransientError``.
        """
        try:
            detail = self._request_with_retry(
                lambda: DatabasesApi(self._runtime.api).get_database(database_id)
            )
        except HotdataTerminalError as exc:
            if _is_not_found(exc):
                raise KeyError(database_id) from exc
            raise
        # managed_database_from_detail drops default_catalog, and it is the only
        # place the real catalog name is reported.
        self._cache_catalog(getattr(detail, "default_catalog", None))
        return managed_database_from_detail(detail)

    def _configured_database_id(self) -> str | None:
        return getattr(self._config, "database_id", None)

    def _require_db(self) -> ManagedDatabase:
        """Resolve the run's database for an operation (id-first).

        Reuses the run cache, else binds the configured ``database_id``. Raises
        ``KeyError`` when no database has been resolved and none is configured —
        there is deliberately no by-name fallback.
        """
        db = self._cached_db()
        if db is not None:
            return db
        database_id = self._configured_database_id()
        if database_id:
            db = self._bind_by_id(database_id)
            self._cache_db(db, created=False)
            return db
        raise KeyError("no instant database resolved for this run (set database_id)")

    def resolved_database_id(self) -> str:
        """Return the id of the run's instant database (bound by id or created).

        Raises ``KeyError`` when none is configured and none was created this run.
        """
        return self._require_db().id

    # --- lifecycle --------------------------------------------------------

    def ensure_managed_database(
        self,
        *,
        schema: str,
        tables: list[str],
        keys: dict[str, list[str]] | None = None,
        partition_by: dict[str, list[Any]] | None = None,
        sorted_by: dict[str, list[Any]] | None = None,
        create_if_missing: bool,
    ) -> ManagedDatabase:
        # keys: table name -> key columns (enables delete/update/upsert on it)
        # partition_by / sorted_by: table name -> that table's layout keys, in
        # declaration order. Only reaches tables being CREATED — a layout is fixed
        # at creation, so an existing table gets a warning instead.
        keys = keys or {}
        partition_by = partition_by or {}
        sorted_by = sorted_by or {}
        # Which tables this call actually declared. A layout is only applied at
        # creation, and the caller needs to know which tables are new so it can
        # seed them — seeding an EXISTING table would empty it, since the seed is a
        # zero-row replace load. Rebound per call, so a client reused across loads
        # never reports a table the previous load created.
        self.newly_declared = set()

        db = self._cached_db()
        created = self._was_created()
        if db is None:
            database_id = self._configured_database_id()
            if database_id:
                try:
                    db = self._bind_by_id(database_id)
                except KeyError:
                    # A pinned id can't be recreated (ids are server-assigned), so
                    # on the create path a missing id is a clear terminal error, not
                    # a silent recreate. The probe path (create_if_missing=False, e.g.
                    # is_storage_initialized) still gets the KeyError -> "not there".
                    if create_if_missing:
                        raise HotdataTerminalError(
                            f"configured database_id {database_id!r} was not found "
                            "(it may have been dropped). An instant database cannot be "
                            "recreated with the same id -- unset database_id to create a "
                            "new one, or pin an existing id."
                        ) from None
                    raise
                created = False
                self._cache_db(db, created=False)
            elif create_if_missing:
                db = self._create(
                    schema=schema,
                    tables=tables,
                    keys=keys,
                    partition_by=partition_by,
                    sorted_by=sorted_by,
                )
                # A fresh database declared every table, so every one is new.
                self.newly_declared = set(tables)
                created = True
                self._cache_db(db, created=True)
            else:
                raise KeyError("no instant database resolved for this run (set database_id)")

        # A freshly created database already declared every table; only an
        # existing (bound) database needs additive, in-place schema evolution.
        if not created:
            self._reconcile_tables(
                db, schema=schema, tables=tables, keys=keys,
                partition_by=partition_by or {}, sorted_by=sorted_by or {},
            )
        return db

    def _create(
        self,
        *,
        schema: str,
        tables: list[str],
        keys: dict[str, list[str]],
        partition_by: dict[str, list[Any]] | None = None,
        sorted_by: dict[str, list[Any]] | None = None,
    ) -> ManagedDatabase:
        description = getattr(self._config, "database_name", None)
        db = self._request_with_retry(
            lambda: self._runtime.create_managed_database(
                description=description,
                schema=schema,
                tables=sorted(set(tables)),
                keys=keys,
                partition_by=partition_by or {},
                sorted_by=sorted_by or {},
            )
        )
        # Logged at WARNING (dlt's default level) so the new id is always visible:
        # without pinning it via database_id, the next run creates another database.
        logger.warning(
            "hotdata: created instant database %s (name=%r). Pin it for future runs by "
            "setting database_id=%s (HOTDATA_DATABASE_ID / [destination.hotdata] database_id).",
            db.id,
            description,
            db.id,
        )
        return db

    def _reconcile_tables(
        self,
        db: ManagedDatabase,
        *,
        schema: str,
        tables: list[str],
        keys: dict[str, list[str]],
        partition_by: dict[str, list[Any]] | None = None,
        sorted_by: dict[str, list[Any]] | None = None,
    ) -> None:
        existing = {
            managed_table.table
            for managed_table in self._request_with_retry(
                lambda: self._runtime.list_managed_tables(db, schema=schema)
            )
        }
        # Declare any newly-required tables additively, in place, carrying their
        # key. dlt calls ``initialize_storage`` with the full table set before any
        # load job runs, so by load time this is normally a no-op.
        self.newly_declared |= set(tables) - existing
        for table in sorted(set(tables) - existing):
            self._add_managed_table(
                db,
                table,
                schema=schema,
                key=keys.get(table),
                partition_by=(partition_by or {}).get(table),
                sorted_by=(sorted_by or {}).get(table),
            )
        # A table that already exists cannot gain a layout — it is fixed at
        # creation with no alter path. Read the layout it actually has before
        # saying anything: this runs on every load, and twice per load package
        # (dlt calls initialize_storage bare and again with truncate_tables), so a
        # pipeline whose table already carries the layout it declares would
        # otherwise warn forever about a difference that does not exist. Warn
        # rather than raise on a real mismatch: refusing would break a pipeline
        # that has been loading happily.
        for table in sorted(set(tables) & existing):
            declared_partition = (partition_by or {}).get(table) or []
            declared_sort = (sorted_by or {}).get(table) or []
            if not declared_partition and not declared_sort:
                continue
            if self._layout_already_matches(
                db,
                table,
                schema=schema,
                partition_by=declared_partition,
                sorted_by=declared_sort,
            ):
                continue
            logger.warning(
                "hotdata: %s.%s already exists and its stored layout differs from "
                "the one declared, so the declared layout was NOT applied — "
                "partition and sort keys are fixed when a table is created. Read "
                "the stored layout with HotdataClient.managed_table_layout.",
                schema,
                table,
            )

    def _layout_already_matches(
        self,
        db: ManagedDatabase,
        table: str,
        *,
        schema: str,
        partition_by: list[Any],
        sorted_by: list[Any],
    ) -> bool:
        """Whether ``table``'s stored layout already satisfies what was declared.

        Declared entries are the ``TablePartitionKey`` / ``TableSortKey`` values
        the layout resolver produces, compared against what the server reports.

        An unset sort ``direction`` / ``nulls`` means "whatever the server defaults
        to", so it must not be compared against the resolved value the server
        reports back — counting that as a difference is exactly what made this warn
        on every load of a correctly configured pipeline. A partition ``transform``
        needs no such tolerance: ``TablePartitionKey`` rejects an unset one, so
        "identity" and "unset" cannot both appear. Only case is folded.

        False when the layout cannot be read, so an unreachable API warns rather
        than going quiet about a mismatch that may be real.
        """
        try:
            current = self._request_with_retry(
                lambda: self._runtime.managed_table_layout(db, table, schema=schema)
            )
        except Exception as exc:
            # A missing method or a changed signature is our bug, not an API
            # failure, and swallowing it would silently restore the permanent
            # "NOT applied" warning this comparison exists to remove -- with an
            # extra request per table to earn it. _request_with_retry maps
            # everything through classify_sdk_error, so the shape error arrives as
            # the __cause__ of a mapped error rather than itself.
            if isinstance(exc.__cause__, (AttributeError, TypeError)):
                raise
            # Any real API failure: cannot confirm, so warn rather than go quiet.
            return False
        if [(k.column, k.transform.lower()) for k in current.partition_by] != [
            (k.column, k.transform.lower()) for k in partition_by
        ]:
            return False
        if len(current.sorted_by) != len(sorted_by):
            return False
        for got, want in zip(current.sorted_by, sorted_by, strict=True):
            if got.column != want.column:
                return False
            if want.direction is not None and got.direction != want.direction:
                return False
            if want.nulls is not None and got.nulls != want.nulls:
                return False
        return True

    def _add_managed_table(
        self,
        database: ManagedDatabase,
        table: str,
        *,
        schema: str,
        key: list[str] | None = None,
        partition_by: list[Any] | None = None,
        sorted_by: list[Any] | None = None,
    ) -> None:
        self._request_with_retry(
            lambda: self._runtime.add_managed_table(
                database,
                table,
                schema=schema,
                key=key,
                partition_by=partition_by or None,
                sorted_by=sorted_by or None,
            )
        )

    def drop_managed_database(self) -> None:
        """Delete the run's instant database if it exists (used for dlt dev_mode / refresh)."""
        db = self._cached_db()
        if db is None:
            database_id = self._configured_database_id()
            if not database_id:
                return
            try:
                db = self._bind_by_id(database_id)
            except KeyError:
                return
        self._request_with_retry(lambda: self._runtime.delete_managed_database(db))
        self._cache_db(None, created=False)

    # --- operations (addressed by the resolved record) --------------------

    def load_managed_table(self, table: str, **kwargs):
        """Load parquet into a managed table via the resolved database record.

        Passing the resolved ``ManagedDatabase`` (not its id) lets a create-scoped
        key load without a further read probe (framework passthrough)."""
        db = self._require_db()
        return super().load_managed_table(db, table, **kwargs)

    def execute_sql(self, sql: str) -> pa.Table:
        """Run a SQL query scoped to the run's database and return Arrow.

        Submits the query, polls until the result is ready, and fetches it as a
        ``pyarrow.Table``. An empty table is returned when the query produces no
        out-of-band result.
        """

        def operation() -> pa.Table:
            db = self._require_db()
            result_id = self._query_database_scoped(sql, database_id=db.id)
            if result_id is None:
                return pa.table({})
            # Results of database-scoped queries are database-scoped; the
            # hotdata 0.6.0 SDK requires the scope on the Arrow fetch.
            return ArrowResultsApi(self._runtime.api).get_result_arrow(
                result_id, x_database_id=db.id
            )

        return self._request_with_retry(operation)

    def list_managed_tables(self, *, schema: str) -> list:
        """List the managed tables in the run's database/``schema`` (used by ``has_dataset``)."""
        db = self._require_db()
        return self._request_with_retry(
            lambda: self._runtime.list_managed_tables(db, schema=schema)
        )

    def fetch_table(self, *, schema: str, table: str) -> pa.Table | None:
        def operation() -> pa.Table | None:
            db = self._require_db()
            if not self._table_is_synced_for(db, table, schema=schema):
                return None
            sql = f'SELECT * FROM "{self.catalog}"."{schema}"."{table}"'
            result_id = self._query_database_scoped(sql, database_id=db.id)
            if result_id is None:
                return None
            return self._fetch_result_arrow(result_id, database_id=db.id)

        return self._request_with_retry(operation)

    def fetch_table_rows(self, *, schema: str, table: str) -> list[dict]:
        result = self.fetch_table(schema=schema, table=table)
        return result.to_pylist() if result is not None else []

    def _table_is_synced_for(self, db: ManagedDatabase, table: str, *, schema: str) -> bool:
        for managed_table in self._runtime.list_managed_tables(db, schema=schema):
            if managed_table.table == table:
                return managed_table.synced
        return False


__all__ = ["HotdataClient"]
