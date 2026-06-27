from __future__ import annotations

from hotdata_framework.databases import ManagedDatabase
from hotdata_framework.managed_client import ManagedDatabaseClient


class HotdataClient(ManagedDatabaseClient):
    """Managed-database client used by the dlt destination.

    Adds cross-run schema evolution on top of the shared
    ``hotdata_framework`` client: managed-database tables can only be declared
    at creation time, so when an existing database is missing any required
    table the database is recreated with the union of existing and required
    tables. (Ported from the first-party dlt destination.)
    """

    def ensure_managed_database(
        self,
        name: str,
        *,
        schema: str,
        tables: list[str],
        create_if_missing: bool,
    ) -> ManagedDatabase:
        def operation() -> ManagedDatabase:
            runtime = self._runtime
            try:
                db = runtime.resolve_managed_database(name)
            except KeyError:
                if not create_if_missing:
                    raise
                return runtime.create_managed_database(
                    description=name,
                    schema=schema,
                    tables=sorted(set(tables)),
                )

            existing = {
                managed_table.table
                for managed_table in runtime.list_managed_tables(name, schema=schema)
            }
            missing = set(tables) - existing
            if not missing:
                return db

            # Tables can only be declared at creation time -- recreate with the union.
            all_tables = sorted(existing | set(tables))
            runtime.delete_managed_database(db.id)
            return runtime.create_managed_database(
                description=name,
                schema=schema,
                tables=all_tables,
            )

        return self._request_with_retry(operation)


__all__ = ["HotdataClient"]
