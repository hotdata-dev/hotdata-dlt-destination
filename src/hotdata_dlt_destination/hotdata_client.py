from __future__ import annotations

import contextlib
import os
import tempfile

import pyarrow as pa
from hotdata_framework.databases import ManagedDatabase
from hotdata_framework.managed_client import ManagedDatabaseClient

from hotdata_dlt_destination.parquet import write_table_parquet


class HotdataClient(ManagedDatabaseClient):
    """Managed-database client used by the dlt destination.

    Adds cross-run schema evolution on top of the shared ``hotdata_framework``
    client. Managed-database tables can only be declared at creation time, so
    when an existing database is missing a required table the database must be
    recreated. To avoid losing data (including dlt's ``_dlt_version`` /
    ``_dlt_loads`` / ``_dlt_pipeline_state`` bookkeeping, which powers state
    sync), every existing table is snapshotted, the database is recreated with
    the union of existing and required tables, and the snapshots are reloaded.
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
        if not set(tables) - existing:
            return db

        # Snapshot existing data before the destructive recreate so no rows are
        # lost when a new table is added on a later run.
        all_tables = sorted(existing | set(tables))
        snapshots: dict[str, pa.Table] = {}
        for table in sorted(existing):
            data = self.fetch_table(database=name, schema=schema, table=table)
            if data is not None and data.num_rows:
                snapshots[table] = data

        self._request_with_retry(lambda: runtime.delete_managed_database(db.id))
        new_db = self._request_with_retry(
            lambda: runtime.create_managed_database(
                description=name, schema=schema, tables=all_tables
            )
        )
        for table, data in snapshots.items():
            self._reload_table(name, table, schema=schema, data=data)
        return new_db

    def drop_managed_database(self, name: str) -> None:
        """Delete the managed database if it exists (used for dlt dev_mode / refresh)."""
        runtime = self._runtime
        try:
            db = runtime.resolve_managed_database(name)
        except KeyError:
            return
        self._request_with_retry(lambda: runtime.delete_managed_database(db.id))

    def _reload_table(self, database: str, table: str, *, schema: str, data: pa.Table) -> None:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as handle:
            path = handle.name
        try:
            write_table_parquet(data, path)
            upload_id = self.upload_parquet(path)
            self.load_managed_table(database, table, schema=schema, upload_id=upload_id)
        finally:
            with contextlib.suppress(OSError):
                os.unlink(path)


__all__ = ["HotdataClient"]
