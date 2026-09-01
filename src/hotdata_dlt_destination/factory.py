from __future__ import annotations

import os
import typing as t
import warnings

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import (
    escape_postgres_identifier,
    escape_postgres_literal,
)
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.wei import EVM_DECIMAL_PRECISION

from hotdata_dlt_destination.configuration import (
    HotdataClientConfiguration,
    HotdataCredentials,
)

if t.TYPE_CHECKING:
    from hotdata_dlt_destination.job_client import HotdataJobClient


class hotdata(Destination[HotdataClientConfiguration, "HotdataJobClient"]):
    """dlt destination for Hotdata instant databases.

    Supports nested/child tables, dlt internal columns, schema versioning,
    load tracking, and pipeline state sync (``WithStateSync``).
    """

    spec = HotdataClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        # "sequential", not "table-sequential": the managed-database load API
        # locks at the CATALOG level, so two tables of the same database
        # loading in parallel 409 each other. table-sequential only serializes
        # jobs per table and still runs different tables concurrently — any
        # multi-table pipeline then races itself and fails once the 409s
        # outlast the retry budget. Override via
        # config.loader_parallelism_strategy if that trade-off is ever wanted.
        caps.loader_parallelism_strategy = "sequential"
        caps.max_table_nesting = 1000
        caps.naming_convention = "snake_case"
        caps.has_case_sensitive_identifiers = False
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.supports_ddl_transactions = False
        caps.supported_merge_strategies = ["upsert", "insert-only"]
        caps.supported_replace_strategies = ["truncate-and-insert"]
        # dlt maps Decimal/wei columns without explicit precision hints onto parquet
        # numeric types using these caps; left unset (None), normalize crashes in
        # get_py_arrow_numeric ("'NoneType' object is not subscriptable"). Use dlt's
        # default numeric precision — DataFusion presents a Postgres numeric surface.
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (EVM_DECIMAL_PRECISION, 0)
        caps.sqlglot_dialect = "postgres"
        caps.escape_identifier = escape_postgres_identifier
        caps.escape_literal = escape_postgres_literal
        return caps

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: HotdataClientConfiguration,
        naming: NamingConvention | None,
    ) -> DestinationCapabilitiesContext:
        caps = super().adjust_capabilities(caps, config, naming)
        if config.max_table_nesting is not None:
            caps.max_table_nesting = config.max_table_nesting
        if config.loader_parallelism_strategy is not None:
            caps.loader_parallelism_strategy = config.loader_parallelism_strategy
        return caps

    @property
    def client_class(self) -> type[HotdataJobClient]:
        from hotdata_dlt_destination.job_client import HotdataJobClient

        return HotdataJobClient

    def __init__(
        self,
        credentials: HotdataCredentials | dict[str, t.Any] | str | None = None,
        workspace_id: str | None = None,
        database_id: str | None = None,
        database_name: str = None,
        schema: str = None,
        write_disposition: str = None,
        declared_tables: list[str] | None = None,
        create_database_if_missing: bool = None,
        api_base_url: str = None,
        max_retries: int = None,
        retry_backoff_seconds: float = None,
        max_state_files: int = None,
        max_table_nesting: int = None,
        loader_parallelism_strategy: str = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: t.Any,
    ) -> None:
        # A workspace_id nested in a credentials dict is hoisted to the param
        # (deprecated). Rebuild the dict without it rather than mutating the caller's.
        if isinstance(credentials, dict) and "workspace_id" in credentials:
            if workspace_id is None:
                workspace_id = credentials["workspace_id"]
            credentials = {k: v for k, v in credentials.items() if k != "workspace_id"}
            warnings.warn(
                "Passing workspace_id inside credentials is deprecated; pass it as "
                "hotdata(workspace_id=...) instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        # The API key (a secret) may come from the environment; the workspace is a
        # routing param with no env fallback — pass it to hotdata(workspace_id=...).
        if credentials is None and os.environ.get("HOTDATA_API_KEY") is not None:
            credentials = HotdataCredentials(api_key=os.environ["HOTDATA_API_KEY"])

        super().__init__(
            credentials=credentials,
            workspace_id=workspace_id,
            database_id=database_id,
            database_name=database_name,
            schema=schema,
            write_disposition=write_disposition,
            declared_tables=declared_tables,
            create_database_if_missing=create_database_if_missing,
            api_base_url=api_base_url,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            max_state_files=max_state_files,
            max_table_nesting=max_table_nesting,
            loader_parallelism_strategy=loader_parallelism_strategy,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


hotdata.register()
