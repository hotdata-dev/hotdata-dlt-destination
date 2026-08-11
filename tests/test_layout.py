"""Storage layout: resolving it from a dlt schema, and getting it to the API.

A layout is fixed when a table is created and there is no alter path, so the two
failure modes worth testing are a hint that silently does not arrive, and a seed
that empties a table it should not have touched.
"""
from __future__ import annotations

import pathlib
import tempfile

import dlt
import pytest
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.common.storages import SchemaStorage, SchemaStorageConfiguration
from hotdata_framework import TablePartitionKey, TableSortKey

from hotdata_dlt_destination.layout import (
    PARTITION_HINT,
    SORT_HINT,
    LayoutError,
    declares_layout,
    hotdata_adapter,
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


# --- the explicit hint values, which exist because order is unrepairable -------


def test_explicit_hint_values_preserve_key_order():
    """The whole reason the explicit hints exist. A per-column boolean cannot say
    "event_time THEN tag_mac", and a table created with them the wrong way round
    cannot be corrected.

    This covers the stored hint values the resolvers read, not `hotdata_adapter`
    itself — the adapter's own round trip is tested further down.
    """
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


def test_explicit_hints_beat_plain_column_hints():
    """Both present: the explicit hint is the more specific statement, and the only
    one that can carry a transform."""
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


# --- hotdata_adapter itself, end to end --------------------------------------
#
# Everything above hand-builds the hint keys, so none of it exercises what
# `hotdata_adapter` actually stores. That gap hid a bug where the adapter stored
# `TablePartitionKey` / `TableSortKey` objects that the resolvers could not parse:
# the headline feature raised LayoutError on the first load and no test noticed.


@dlt.resource(name="readings")
def _readings():
    yield {"event_date": "2026-01-01", "event_time": 1, "tag_mac": "a"}


def _adapted(**kwargs) -> TTableSchema:
    """A real resource put through the adapter, as a user would write it."""
    return hotdata_adapter(_readings.with_name("readings"), **kwargs).compute_table_schema()


def test_the_adapter_resolves_on_the_in_memory_schema():
    """The first load, which is the only load where declaring a layout matters.
    Nothing has been persisted yet, so the resolvers read exactly what the adapter
    put in `additional_table_hints`."""
    t = _adapted(
        partition_by=[("event_date", "day")],
        sorted_by=["event_time", ("tag_mac", "desc", "last")],
    )
    assert [(k.column, k.transform) for k in resolve_partition_by(t)] == [
        ("event_date", "day")
    ]
    assert [(k.column, k.direction, k.nulls) for k in resolve_sorted_by(t)] == [
        ("event_time", None, None),
        ("tag_mac", "desc", "last"),
    ]


def test_the_adapter_accepts_framework_key_objects():
    """A caller holding TablePartitionKey / TableSortKey can pass them straight in,
    rather than converting back to tuples."""
    t = _adapted(
        partition_by=[TablePartitionKey(column="event_date", transform="month")],
        sorted_by=[TableSortKey(column="event_time", direction="asc", nulls="first")],
    )
    assert [(k.column, k.transform) for k in resolve_partition_by(t)] == [
        ("event_date", "month")
    ]
    assert [(k.column, k.direction, k.nulls) for k in resolve_sorted_by(t)] == [
        ("event_time", "asc", "first")
    ]


def test_adapter_hints_are_json_safe_so_they_survive_dlt_storing_the_schema():
    """dlt persists the schema after a run and restores it on the next one. A hint
    holding a live SDK object does not survive that, so the layout would silently
    stop being declared on run two."""
    schema = Schema("probe")
    schema.update_table(_adapted(partition_by=[("event_date", "identity")]))
    storage = SchemaStorage(
        SchemaStorageConfiguration(schema_volume_path=tempfile.mkdtemp()), makedirs=True
    )
    storage.save_schema(schema)
    restored = storage.load_schema("probe").tables["readings"]
    assert [(k.column, k.transform) for k in resolve_partition_by(restored)] == [
        ("event_date", "identity")
    ]


def test_an_exported_schema_has_no_sdk_class_paths_and_reimports():
    """`export_schema_path` / `import_schema_path` is the documented way to
    hand-edit a schema, and it goes through yaml.safe_load. A model object exports
    as `!!python/object:hotdata.models...`, which safe_load then refuses — and it
    pins a user's checked-in schema file to an SDK class path we could not move."""
    export = tempfile.mkdtemp()
    schema = Schema("probe")
    # BOTH halves: they are stored by separate code paths, so a test covering only
    # one lets a regression through on the other.
    schema.update_table(
        _adapted(
            partition_by=[("event_date", "day")],
            sorted_by=[("event_time", "desc", "last")],
        )
    )
    SchemaStorage(
        SchemaStorageConfiguration(
            schema_volume_path=tempfile.mkdtemp(),
            export_schema_path=export,
            import_schema_path=export,
        ),
        makedirs=True,
    ).save_schema(schema)

    exported = next(pathlib.Path(export).glob("*.yaml")).read_text()
    assert "python/object" not in exported, exported

    reimported = SchemaStorage(
        SchemaStorageConfiguration(
            schema_volume_path=tempfile.mkdtemp(), import_schema_path=export
        ),
        makedirs=True,
    ).load_schema("probe")
    readings = reimported.tables["readings"]
    assert [(k.column, k.transform) for k in resolve_partition_by(readings)] == [
        ("event_date", "day")
    ]
    assert [
        (k.column, k.direction, k.nulls) for k in resolve_sorted_by(readings)
    ] == [("event_time", "desc", "last")]


def test_the_adapter_rejects_a_bad_transform_at_definition_time():
    """Validation belongs before extract, not at the request the server rejects."""
    with pytest.raises(LayoutError):
        _adapted(partition_by=[("event_date", "fortnight")])
