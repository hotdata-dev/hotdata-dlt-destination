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


def test_declared_table_names_normalizes_identifiers() -> None:
    names = TableContract.declared_table_names(
        database_name="Linear",
        schema="public",
        table_names=["LineItems", "Customers"],
    )
    assert names == ["customers", "lineitems"]
