from hotdata_dlt_destination.contracts import TableContract, normalize_identifier


def test_normalize_identifier() -> None:
    assert normalize_identifier("Orders-Events") == "orders_events"


def test_table_contract_mapping() -> None:
    table = {"name": "LineItems", "resource": "Orders"}
    contract = TableContract.from_table_schema(
        table,
        database_name="linear",
        schema="public",
    )
    assert contract.database_name == "linear"
    assert contract.schema == "public"
    assert contract.table_name == "lineitems"
    assert contract.qualified_target == "linear.public.lineitems"


def test_normalize_identifier_preserves_leading_underscore() -> None:
    # dlt internal tables/columns rely on the leading underscore; stripping it
    # breaks state-sync round-tripping (regression).
    assert normalize_identifier("_dlt_pipeline_state") == "_dlt_pipeline_state"
    assert normalize_identifier("_dlt_id") == "_dlt_id"


def test_table_contract_does_not_double_prefix_nested_tables() -> None:
    # dlt already composes the nested name into `name`; the contract must not
    # re-prepend `parent` (regression: produced orders__orders__items).
    table = {"name": "orders__items", "parent": "orders"}
    contract = TableContract.from_table_schema(table, database_name="db", schema="public")
    assert contract.table_name == "orders__items"


def test_table_contract_preserves_internal_table_name() -> None:
    table = {"name": "_dlt_pipeline_state"}
    contract = TableContract.from_table_schema(table, database_name="db", schema="public")
    assert contract.table_name == "_dlt_pipeline_state"


def test_declared_table_names_normalizes_identifiers() -> None:
    names = TableContract.declared_table_names(
        database_name="Linear",
        schema="public",
        table_names=["LineItems", "Customers"],
    )
    assert names == ["customers", "lineitems"]


def test_table_contract_keeps_database_and_schema_verbatim() -> None:
    # database_name/schema are opaque API addresses (a name or a dbid), not SQL
    # identifiers. Rewriting them split one pipeline's writes across two
    # databases: the load jobs (which address via the contract) minted a
    # snake_cased twin, while schema/state/bookkeeping writes (which address via
    # the raw config) stayed on the original (regression, 2026-07-09).
    contract = TableContract.from_table_schema(
        {"name": "spans"},
        database_name="my-hyphen-db",
        schema="Public-Schema",
    )
    assert contract.database_name == "my-hyphen-db"
    assert contract.schema == "Public-Schema"
