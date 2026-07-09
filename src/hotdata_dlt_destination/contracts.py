from __future__ import annotations

import re
from dataclasses import dataclass

from dlt.common.schema import TTableSchema

IDENTIFIER_RE = re.compile(r"[^a-zA-Z0-9_]")


def normalize_identifier(value: str) -> str:
    # Replace invalid characters and lowercase, but preserve leading/trailing
    # underscores: dlt's internal tables and columns (`_dlt_id`, `_dlt_load_id`,
    # `_dlt_pipeline_state`, ...) rely on the leading underscore, and stripping it
    # breaks state-sync round-tripping and bookkeeping table lookups.
    normalized = IDENTIFIER_RE.sub("_", value).lower()
    return normalized or "unnamed"


@dataclass(frozen=True)
class TableContract:
    database_name: str
    schema: str
    table_name: str

    @property
    def qualified_target(self) -> str:
        return f"{self.database_name}.{self.schema}.{self.table_name}"

    @classmethod
    def from_table_schema(
        cls,
        table: TTableSchema,
        *,
        database_name: str,
        schema: str,
    ) -> TableContract:
        # dlt has already applied the naming convention, so ``name`` is the fully
        # composed table name (e.g. ``orders__items`` for a nested table). Do NOT
        # re-prepend ``parent`` -- that double-prefixes child tables.
        #
        # ``database_name`` and ``schema`` are opaque API addresses (a managed-DB
        # name or a ``dbid...`` id), not SQL identifiers — they must pass through
        # VERBATIM. Every other write-path call (``initialize_storage``,
        # ``update_stored_schema``, ``complete_load``) addresses the API with the
        # raw config values; rewriting them here made a load job with a
        # hyphenated database name mint a snake_cased twin database and load the
        # data there, leaving the declared tables in the original DB empty.
        return cls(
            database_name=database_name,
            schema=schema,
            table_name=normalize_identifier(table["name"]),
        )

    @classmethod
    def declared_table_names(
        cls,
        *,
        database_name: str,
        schema: str,
        table_names: list[str],
    ) -> list[str]:
        return sorted(
            {
                cls.from_table_schema(
                    {"name": table_name},
                    database_name=database_name,
                    schema=schema,
                ).table_name
                for table_name in table_names
            }
        )
