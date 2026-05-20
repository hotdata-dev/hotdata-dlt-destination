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
    assert contract.staging_table_name == "_dlt_staging_lineitems"
    assert contract.qualified_target == "linear.public.lineitems"
    assert contract.qualified_staging == "linear.public._dlt_staging_lineitems"
