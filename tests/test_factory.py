from hotdata_dlt_destination import hotdata
from hotdata_dlt_destination.job_client import HotdataJobClient


def test_capabilities_use_parquet_and_nesting() -> None:
    caps = hotdata().capabilities()
    assert caps.preferred_loader_file_format == "parquet"
    assert caps.max_table_nesting == 1000
    assert caps.naming_convention == "snake_case"
    # Loads lock at the catalog (database) level: parallel loads of DIFFERENT
    # tables in one database 409 each other, so the loader must be fully
    # sequential by default.
    assert caps.loader_parallelism_strategy == "sequential"
    assert caps.supported_merge_strategies == ["upsert", "insert-only"]
    assert caps.supported_replace_strategies == ["truncate-and-insert"]


def test_loader_parallelism_strategy_override() -> None:
    caps = hotdata(loader_parallelism_strategy="table-sequential").capabilities()
    assert caps.loader_parallelism_strategy == "table-sequential"


def test_max_table_nesting_override() -> None:
    caps = hotdata(max_table_nesting=2).capabilities()
    assert caps.max_table_nesting == 2


def test_client_class_is_job_client() -> None:
    assert hotdata().client_class is HotdataJobClient
