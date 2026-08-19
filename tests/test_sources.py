"""Unit tests for hotdata_table / hotdata_query.

The SQL builder and the resource hints are checked directly; the read itself is
covered by tests/test_source_client.py, so nothing here touches the network.
"""

from __future__ import annotations

import pytest

from hotdata_dlt_destination.sources import _select, hotdata_query, hotdata_table

CREDS = {
    "database_id": "dbid_source",
    "workspace_id": "ws_1",
    "credentials": {"api_key": "secret"},
}


# --- SQL construction -------------------------------------------------------


def test_a_plain_table_read_selects_everything() -> None:
    assert _select(schema="public", table="orders", included_columns=None, limit=None) == (
        "SELECT * FROM public.orders"
    )


def test_named_columns_become_the_projection() -> None:
    assert _select(
        schema="analytics", table="orders", included_columns=["id", "amount"], limit=None
    ) == "SELECT id, amount FROM analytics.orders"


def test_a_limit_bounds_the_read() -> None:
    assert _select(schema="public", table="t", included_columns=None, limit=10) == (
        "SELECT * FROM public.t LIMIT 10"
    )


@pytest.mark.parametrize(
    "field,kwargs",
    [
        ("schema", {"schema": "pub;drop", "table": "t", "included_columns": None, "limit": None}),
        ("table", {"schema": "public", "table": "a.b", "included_columns": None, "limit": None}),
        (
            "table",
            {"schema": "public", "table": '"quoted"', "included_columns": None, "limit": None},
        ),
        (
            "included_columns",
            {"schema": "public", "table": "t", "included_columns": ["a", "b c"], "limit": None},
        ),
        ("table", {"schema": "public", "table": "", "included_columns": None, "limit": None}),
    ],
)
def test_anything_that_is_not_a_plain_identifier_is_refused(
    field: str, kwargs: dict[str, object]
) -> None:
    """Refused, not escaped.

    These names are interpolated, so accepting a quoted or dotted string would
    let `table=` compose the statement rather than name part of it. The error
    names the field so the caller knows which argument to fix.
    """
    with pytest.raises(ValueError, match=field):
        _select(**kwargs)  # type: ignore[arg-type]


def test_a_limit_that_is_not_a_number_is_refused_by_int_conversion() -> None:
    with pytest.raises(ValueError):
        _select(  # type: ignore[arg-type]
            schema="public", table="t", included_columns=None, limit="1; DROP TABLE t"
        )


# --- resource identity and hints -------------------------------------------


def test_a_table_resource_is_named_for_its_table_by_default() -> None:
    assert hotdata_table("orders", **CREDS).name == "orders"


def test_an_explicit_name_overrides_the_table_name() -> None:
    assert hotdata_table("orders", name="raw_orders", **CREDS).name == "raw_orders"


def test_resource_hints_reach_the_resource() -> None:
    resource = hotdata_table(
        "orders", primary_key="order_id", write_disposition="merge", **CREDS
    )
    assert resource.write_disposition == "merge"
    assert resource.compute_table_schema()["columns"]["order_id"]["primary_key"] is True


def test_a_query_resource_requires_a_name() -> None:
    """A query has no table to be named after.

    A resource's name decides its destination table and the state it keeps, so a
    default would silently give two different queries one identity.
    """
    with pytest.raises(TypeError, match="name"):
        hotdata_query("select 1", **CREDS)  # type: ignore[call-arg]


def test_a_query_is_passed_through_as_written() -> None:
    """No wrapping, no rewriting.

    The caller owns the statement -- including any LIMIT that bounds it -- so the
    source must not silently change what runs.
    """
    resource = hotdata_query("select a from t where a > 0", name="positives", **CREDS)
    assert resource.name == "positives"


def test_the_dlt_columns_hint_is_not_consumed_as_a_projection() -> None:
    """`columns` reaches dlt; `included_columns` builds the SELECT.

    dlt's `columns` hint describes the DESTINATION schema. If the projection
    argument were also called `columns`, a caller passing a schema hint would
    silently get a SELECT built from its keys instead -- a wrong read that still
    succeeds.
    """
    resource = hotdata_table(
        "orders",
        columns={"amount": {"data_type": "decimal", "precision": 12, "scale": 2}},
        **CREDS,
    )
    column = resource.compute_table_schema()["columns"]["amount"]
    assert column["data_type"] == "decimal"
    assert column["precision"] == 12


def test_projection_and_the_columns_hint_coexist() -> None:
    resource = hotdata_table(
        "orders",
        included_columns=["order_id", "amount"],
        columns={"amount": {"data_type": "double"}},
        **CREDS,
    )
    assert resource.compute_table_schema()["columns"]["amount"]["data_type"] == "double"


def test_a_missing_api_key_fails_while_the_resource_is_built() -> None:
    """Named at construction, not as a 401 from inside a load."""
    with pytest.raises(ValueError, match="missing an api_key"):
        hotdata_table(
            "orders",
            database_id="dbid_source",
            workspace_id="ws_1",
            credentials={"api_key": ""},
        )


def test_the_source_reads_a_database_of_its_own() -> None:
    """The read database is an argument, not shared with the destination.

    A pipeline commonly reads one database and writes another, so binding the
    source to the destination's database would make the common case impossible
    to express.
    """
    resource = hotdata_table(
        "orders",
        database_id="dbid_other",
        workspace_id="ws_1",
        credentials={"api_key": "k"},
    )
    assert resource.name == "orders"


def test_a_bare_string_projection_is_refused() -> None:
    """`included_columns="order_id"` must not become `SELECT o, r, d, ...`.

    A string is itself a sequence of one-character strings, and every one of
    those is a valid SQL identifier -- so the per-column check passes and the
    join produces wrong SQL that runs successfully. The only defence is refusing
    the type.
    """
    with pytest.raises(ValueError, match="bare string"):
        _select(schema="public", table="t", included_columns="order_id", limit=None)  # type: ignore[arg-type]


def test_a_one_column_projection_works_as_a_list() -> None:
    assert _select(
        schema="public", table="t", included_columns=["order_id"], limit=None
    ) == "SELECT order_id FROM public.t"


def test_the_bare_string_refusal_reaches_the_resource_constructor() -> None:
    with pytest.raises(ValueError, match="bare string"):
        hotdata_table("orders", included_columns="order_id", **CREDS)  # type: ignore[arg-type]
