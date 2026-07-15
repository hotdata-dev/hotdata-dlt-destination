"""``JobClientBase`` destination for Hotdata managed databases.

Implements dlt's complete destination contract: nested/child tables, dlt
internal columns, schema versioning, load tracking, and pipeline state sync
(``WithStateSync``) so incremental sources can restore their state from the
managed database. Backed by the shared ``hotdata_framework`` client.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from types import TracebackType
from typing import TYPE_CHECKING, Any

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    JobClientBase,
    PreparedTableSchema,
    RunnableLoadJob,
    StateInfo,
    StorageSchemaInfo,
    WithStateSync,
)
from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationTransientException,
)
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.typing import (
    LOADS_TABLE_NAME,
    PIPELINE_STATE_TABLE_NAME,
    VERSION_TABLE_NAME,
    TTableSchema,
)
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient

from hotdata_dlt_destination.configuration import HotdataClientConfiguration
from hotdata_dlt_destination.contracts import TableContract
from hotdata_dlt_destination.errors import HotdataTerminalError, HotdataTransientError
from hotdata_dlt_destination.hotdata_client import HotdataClient
from hotdata_dlt_destination.merge import (
    combine_tables,
    resolve_merge_strategy,
    resolve_primary_key,
    resolve_write_disposition,
)
from hotdata_dlt_destination.parquet import write_table_parquet

if TYPE_CHECKING:
    from hotdata_dlt_destination.sql_client import HotdataSqlClient

_INTERNAL_TABLE_NAMES = frozenset({VERSION_TABLE_NAME, LOADS_TABLE_NAME, PIPELINE_STATE_TABLE_NAME})


def _is_internal_table(table_name: str) -> bool:
    return table_name in _INTERNAL_TABLE_NAMES or table_name.startswith("_dlt_")


def _declared_tables(*, contract: TableContract, declared_tables: list[str] | None) -> list[str]:
    normalized = TableContract.declared_table_names(
        database_name=contract.database_name,
        schema=contract.schema,
        table_names=declared_tables or [],
    )
    return sorted({*normalized, contract.table_name, *_INTERNAL_TABLE_NAMES})


def _table_keys(*, schema: Schema, database_name: str, schema_name: str) -> dict[str, list[str]]:
    """Managed-table-name -> key columns, from each dlt table's primary_key."""
    keys: dict[str, list[str]] = {}
    for name, table_schema in schema.tables.items():
        if _is_internal_table(name):
            continue
        primary_key = resolve_primary_key(table_schema)
        if not primary_key:
            continue
        managed = TableContract.from_table_schema(
            table_schema, database_name=database_name, schema=schema_name
        ).table_name
        keys[managed] = primary_key
    return keys


def _is_missing_key_error(exc: Exception) -> bool:
    # No typed error crosses the HTTP boundary, so match the server's phrase.
    return "no declared key" in str(exc).lower()


@contextmanager
def _hotdata_api(config: HotdataClientConfiguration) -> Iterator[HotdataClient]:
    api = HotdataClient(
        api_key=config.credentials.api_key,
        workspace_id=config.credentials.workspace_id,
        api_base_url=config.api_base_url,
        max_retries=config.max_retries,
        retry_backoff_seconds=config.retry_backoff_seconds,
    )
    try:
        yield api
    finally:
        api.close()


@contextmanager
def _temp_parquet() -> Iterator[str]:
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as fh:
        path = fh.name
    try:
        yield path
    finally:
        with contextlib.suppress(OSError):
            os.unlink(path)


def _upload_table(
    api: HotdataClient,
    config: HotdataClientConfiguration,
    table_name: str,
    rows: list[dict[str, Any]],
) -> None:
    """Write rows to a parquet file and upload them to the managed table."""
    from dlt.common.libs.pyarrow import pyarrow

    arrow_table = pyarrow.Table.from_pylist(rows)
    with _temp_parquet() as parquet_path:
        write_table_parquet(arrow_table, parquet_path)
        upload_id = api.upload_parquet(parquet_path)
        api.load_managed_table(
            config.database_name,
            table_name,
            schema=config.schema,
            upload_id=upload_id,
        )


class HotdataLoadJob(RunnableLoadJob):
    def __init__(
        self,
        file_path: str,
        config: HotdataClientConfiguration,
        table_schema: TTableSchema,
    ) -> None:
        super().__init__(file_path)
        self._config = config
        self._table_schema = table_schema

    def run(self) -> None:
        from dlt.common.libs.pyarrow import pyarrow

        contract = TableContract.from_table_schema(
            self._table_schema,
            database_name=self._config.database_name,
            schema=self._config.schema,
        )
        disposition = resolve_write_disposition(self._table_schema, self._config.write_disposition)
        primary_key = resolve_primary_key(self._table_schema)
        # merge strategy is separate from write_disposition; insert-only must not
        # update existing rows, so keep it off the native upsert path.
        strategy = resolve_merge_strategy(self._table_schema) if disposition == "merge" else None
        is_insert_only = strategy == "insert-only"

        batch_table = pyarrow.parquet.read_table(self._file_path)
        from dlt.common import logger
        logger.info(
            "load: %s <- %s (%s rows)",
            contract.table_name,
            os.path.basename(self._file_path),
            f"{batch_table.num_rows:,}",
        )

        # Declare every table's key (mirrors initialize_storage). `_schema` is
        # set by the loader before run(); fall back to just this table if not.
        schema = getattr(self, "_schema", None)
        if schema is not None:
            keys = _table_keys(
                schema=schema,
                database_name=self._config.database_name,
                schema_name=self._config.schema,
            )
        else:
            keys = {contract.table_name: primary_key} if primary_key else {}

        def _apply(api: HotdataClient, upload_table, mode: str) -> None:
            with _temp_parquet() as parquet_path:
                write_table_parquet(upload_table, parquet_path)
                upload_id = api.upload_parquet(parquet_path)
                api.load_managed_table(
                    contract.database_name,
                    contract.table_name,
                    schema=contract.schema,
                    upload_id=upload_id,
                    mode=mode,
                )

        def _combine_and_replace(api: HotdataClient, combine_disposition: str) -> None:
            existing = api.fetch_table(
                database=contract.database_name,
                schema=contract.schema,
                table=contract.table_name,
            )
            combined = combine_tables(
                disposition=combine_disposition,
                existing=existing,
                incoming=batch_table,
                primary_key=primary_key,
                fallback_key="_dlt_id",
            )
            _apply(api, combined, "replace")

        try:
            with _hotdata_api(self._config) as api:
                api.ensure_managed_database(
                    contract.database_name,
                    schema=contract.schema,
                    tables=_declared_tables(
                        contract=contract,
                        declared_tables=list(self._config.declared_tables or []),
                    ),
                    keys=keys,
                    create_if_missing=self._config.create_database_if_missing,
                )

                # Native paths (no read-modify-write): append/replace as-is;
                # keyed merge -> upsert. Keyless merge or insert-only fall back
                # to a client-side combine + replace.
                if disposition in ("append", "replace"):
                    _apply(api, batch_table, disposition)
                elif disposition in ("merge", "upsert") and primary_key and not is_insert_only:
                    try:
                        _apply(api, batch_table, "upsert")
                    except HotdataTerminalError as exc:
                        # Pre-key-support tables reject upsert (no declared key)
                        # -> fall back to the combine + replace path.
                        if not _is_missing_key_error(exc):
                            raise
                        _combine_and_replace(api, "merge")
                else:
                    _combine_and_replace(api, "insert-only" if is_insert_only else disposition)
        except HotdataTerminalError as exc:
            raise DestinationTerminalException(str(exc)) from exc
        except HotdataTransientError as exc:
            raise DestinationTransientException(str(exc)) from exc


class HotdataJobClient(JobClientBase, WithStateSync, WithSqlClient):
    """dlt job client for the Hotdata managed-database destination."""

    def __init__(
        self,
        schema: Schema,
        config: HotdataClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: HotdataClientConfiguration = config
        self._sql_client: HotdataSqlClient | None = None

    @property
    def sql_client_class(self) -> type[SqlClientBase]:
        from hotdata_dlt_destination.sql_client import HotdataSqlClient

        return HotdataSqlClient

    @property
    def sql_client(self) -> HotdataSqlClient:
        # Advertised via WithSqlClient so dlt's dataset read API
        # (`pipeline.dataset()`) can query loaded data. Addressing mirrors the
        # write path (managed database_name + fixed schema), so reads return
        # what writes wrote.
        from hotdata_dlt_destination.sql_client import HotdataSqlClient

        if self._sql_client is None:
            self._sql_client = HotdataSqlClient(
                self.config.database_name,
                self.config.schema,
                self.capabilities,
                self.config,
            )
        return self._sql_client

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        internal = [
            self.schema.version_table_name,
            self.schema.loads_table_name,
            self.schema.state_table_name,
        ]
        user = list(self.config.declared_tables or [])
        schema_tables = [
            TableContract.from_table_schema(
                table_schema,
                database_name=self.config.database_name,
                schema=self.config.schema,
            ).table_name
            for name, table_schema in self.schema.tables.items()
            if not _is_internal_table(name)
        ]
        all_tables = sorted(set(internal + user + schema_tables))

        try:
            with _hotdata_api(self.config) as api:
                api.ensure_managed_database(
                    self.config.database_name,
                    schema=self.config.schema,
                    tables=all_tables,
                    keys=_table_keys(
                        schema=self.schema,
                        database_name=self.config.database_name,
                        schema_name=self.config.schema,
                    ),
                    create_if_missing=self.config.create_database_if_missing,
                )
        except HotdataTerminalError as exc:
            raise DestinationTerminalException(str(exc)) from exc

    def is_storage_initialized(self) -> bool:
        with _hotdata_api(self.config) as api:
            try:
                api.ensure_managed_database(
                    self.config.database_name,
                    schema=self.config.schema,
                    tables=[],
                    create_if_missing=False,
                )
                return True
            except (KeyError, HotdataTerminalError):
                return False

    def drop_storage(self) -> None:
        # dlt calls this on dev_mode / refresh to wipe the destination. Dropping the
        # whole managed database is the simplest correct reset; initialize_storage
        # recreates it on the next run.
        try:
            with _hotdata_api(self.config) as api:
                api.drop_managed_database(self.config.database_name)
        except HotdataTerminalError as exc:
            raise DestinationTerminalException(str(exc)) from exc

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
        force: bool = False,
    ) -> TSchemaTables | None:
        result = super().update_stored_schema(only_tables, expected_update, force)
        try:
            self._write_schema_to_storage()
        except HotdataTerminalError as exc:
            raise DestinationTerminalException(str(exc)) from exc
        return result

    def complete_load(self, load_id: str) -> None:
        now = datetime.now(UTC)
        new_row: dict[str, Any] = {
            "load_id": load_id,
            "schema_name": self.schema.name,
            "status": 0,
            "inserted_at": now,
            "schema_version_hash": self.schema.version_hash,
        }
        existing = self._fetch_internal_rows(LOADS_TABLE_NAME)
        try:
            with _hotdata_api(self.config) as api:
                _upload_table(api, self.config, LOADS_TABLE_NAME, [*existing, new_row])
        except HotdataTransientError as exc:
            raise DestinationTransientException(str(exc)) from exc
        except HotdataTerminalError as exc:
            raise DestinationTerminalException(str(exc)) from exc

    def create_load_job(
        self,
        table: PreparedTableSchema,
        file_path: str,
        load_id: str,
        restore: bool = False,
    ) -> HotdataLoadJob:
        return HotdataLoadJob(file_path, self.config, table)

    def get_stored_schema(self, schema_name: str = None) -> StorageSchemaInfo | None:
        rows = self._fetch_internal_rows(VERSION_TABLE_NAME)
        if not rows:
            return None
        if schema_name:
            rows = [r for r in rows if r.get("schema_name") == schema_name]
        if not rows:
            return None
        rows.sort(key=lambda r: r["inserted_at"], reverse=True)
        return StorageSchemaInfo.from_normalized_mapping(rows[0], self.schema.naming)

    def get_stored_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo | None:
        rows = self._fetch_internal_rows(VERSION_TABLE_NAME)
        for row in rows:
            if row.get("version_hash") == version_hash:
                return StorageSchemaInfo.from_normalized_mapping(row, self.schema.naming)
        return None

    def get_stored_state(self, pipeline_name: str) -> StateInfo | None:
        state_rows = self._fetch_internal_rows(PIPELINE_STATE_TABLE_NAME)
        loads_rows = self._fetch_internal_rows(LOADS_TABLE_NAME)
        if not state_rows or not loads_rows:
            return None

        completed_load_ids = {r["load_id"] for r in loads_rows if r.get("status") == 0}
        matching = [
            r
            for r in state_rows
            if r.get("pipeline_name") == pipeline_name
            and r.get("_dlt_load_id") in completed_load_ids
        ]
        if not matching:
            return None

        # load_id is a monotonically increasing timestamp string -- sort descending
        matching.sort(key=lambda r: r["_dlt_load_id"], reverse=True)
        return StateInfo.from_normalized_mapping(matching[0], self.schema.naming)

    def __enter__(self) -> HotdataJobClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass

    def _fetch_internal_rows(self, table_name: str) -> list[dict[str, Any]]:
        with _hotdata_api(self.config) as api:
            try:
                table = api.fetch_table(
                    database=self.config.database_name,
                    schema=self.config.schema,
                    table=table_name,
                )
                return table.to_pylist() if table is not None else []
            except (KeyError, HotdataTerminalError):
                return []

    def _write_schema_to_storage(self) -> None:
        schema_dict = self.schema.to_dict()
        schema_str = json.dumps(schema_dict)
        now = datetime.now(UTC)
        new_row: dict[str, Any] = {
            "version": schema_dict["version"],
            "engine_version": schema_dict["engine_version"],
            "inserted_at": now,
            "schema_name": schema_dict["name"],
            "version_hash": schema_dict["version_hash"],
            "schema": schema_str,
        }
        existing = self._fetch_internal_rows(VERSION_TABLE_NAME)
        try:
            with _hotdata_api(self.config) as api:
                _upload_table(api, self.config, VERSION_TABLE_NAME, [*existing, new_row])
        except HotdataTransientError as exc:
            raise DestinationTransientException(str(exc)) from exc
        except HotdataTerminalError as exc:
            raise DestinationTerminalException(str(exc)) from exc
