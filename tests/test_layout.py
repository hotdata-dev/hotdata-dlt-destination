"""Storage layout: resolving it from a dlt schema, and getting it to the API.

A layout is fixed when a table is created and there is no alter path, so the two
failure modes worth testing are a hint that silently does not arrive, and a seed
that empties a table it should not have touched.
"""
from __future__ import annotations

import pytest
from dlt.common.schema.typing import TTableSchema

from hotdata_dlt_destination.layout import (
    PARTITION_HINT,
    SORT_HINT,
    LayoutError,
    declares_layout,
    missing_layout_columns,
    resolve_partition_by,
    resolve_sorted_by,
)


def _col(name: str, **hints) -> dict:
    return {"name": name, "data_type": "text", **hints}


def _table(**over) -> TTableSchema:
    base = {"name": "files", "columns": {}}
    base.update(over)
    return base  # type: ignore[return-value]


# --- the adapter, which exists because order is unrepairable ------------------


def test_adapter_preserves_key_order():
    """The whole reason the adapter exists. A per-column boolean cannot say
    "event_time THEN tag_mac", and a table created with them the wrong way round
    cannot be corrected."""
    t = _table(
        columns={n: _col(n) for n in ("event_date", "event_time", "tag_mac")},
        **{
            PARTITION_HINT: [("event_date", "identity")],
            SORT_HINT: ["event_time", ("tag_mac", "asc", "last")],
        },
    )
    assert [(k.column, k.transform) for k in resolve_partition_by(t)] == [
        ("event_date", "identity")
    ]
    assert [(k.column, k.direction, k.nulls) for k in resolve_sorted_by(t)] == [
        ("event_time", None, None),
        ("tag_mac", "asc", "last"),
    ]


def test_plain_hints_take_order_from_the_schema():
    """Documenting the limitation rather than pretending it away: with per-column
    hints the key order is whatever order the columns appear in, which is why
    anything that cares must use the adapter."""
    forward = _table(
        columns={"event_time": _col("event_time", sort=True), "tag_mac": _col("tag_mac", sort=True)}
    )
    reversed_ = _table(
        columns={"tag_mac": _col("tag_mac", sort=True), "event_time": _col("event_time", sort=True)}
    )
    assert [k.column for k in resolve_sorted_by(forward)] == ["event_time", "tag_mac"]
    assert [k.column for k in resolve_sorted_by(reversed_)] == ["tag_mac", "event_time"]


def test_adapter_beats_plain_hints():
    """Both present: the adapter is the more specific statement, and the only one
    that can carry a transform."""
    t = _table(
        columns={"a": _col("a", partition=True), "b": _col("b")},
        **{PARTITION_HINT: [("b", "day")]},
    )
    assert [(k.column, k.transform) for k in resolve_partition_by(t)] == [("b", "day")]


def test_plain_partition_hint_means_identity():
    t = _table(columns={"event_date": _col("event_date", partition=True)})
    assert [(k.column, k.transform) for k in resolve_partition_by(t)] == [
        ("event_date", "identity")
    ]


# --- validation happens at definition time -----------------------------------


@pytest.mark.parametrize(
    "entry",
    [("c", "weekly"), ("c", "fortnight"), {"column": "c", "transform": "quarter"}],
)
def test_unknown_partition_transform_is_rejected(entry):
    with pytest.raises(LayoutError, match="transform"):
        resolve_partition_by(_table(columns={"c": _col("c")}, **{PARTITION_HINT: [entry]}))


@pytest.mark.parametrize("entry", [("c", "sideways"), ("c", "asc", "middle")])
def test_bad_sort_direction_or_nulls_is_rejected(entry):
    with pytest.raises(LayoutError):
        resolve_sorted_by(_table(columns={"c": _col("c")}, **{SORT_HINT: [entry]}))


def test_entry_without_a_column_is_rejected():
    with pytest.raises(LayoutError, match="column"):
        resolve_partition_by(_table(**{PARTITION_HINT: [{"transform": "identity"}]}))


# --- the pre-upload guard ----------------------------------------------------


def test_layout_naming_an_absent_column_is_reported():
    """The server rejects this too, but only once the declaration is made — after
    a pipeline has extracted, normalised and uploaded."""
    t = _table(columns={"a": _col("a")}, **{PARTITION_HINT: ["missing_col"]})
    assert missing_layout_columns(t) == ["missing_col"]


def test_a_layout_on_present_columns_reports_nothing():
    t = _table(columns={"a": _col("a", partition=True), "b": _col("b", sort=True)})
    assert missing_layout_columns(t) == []


# --- declares_layout drives the replace seed ---------------------------------


def test_declares_layout_is_true_for_either_half():
    assert declares_layout(_table(columns={"a": _col("a", partition=True)}))
    assert declares_layout(_table(columns={"a": _col("a", sort=True)}))
    assert declares_layout(_table(columns={"a": _col("a")}, **{SORT_HINT: ["a"]}))
    assert not declares_layout(_table(columns={"a": _col("a")}))
