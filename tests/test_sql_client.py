"""Unit tests for HotdataSqlClient / HotdataCursor.

These drive the client + cursor with a fake HotdataClient whose ``execute_sql``
returns a canned ``pyarrow.Table`` -- no network, no engine.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)

from hotdata_dlt_destination import hotdata
from hotdata_dlt_destination.configuration import HotdataClientConfiguration, HotdataCredentials
from hotdata_dlt_destination.errors import HotdataTerminalError, HotdataTransientError
from hotdata_dlt_destination.sql_client import HotdataCursor, HotdataSqlClient

CANNED = pa.table(
    {
        "model": ["claude-opus-4-8", "claude-sonnet-5", "claude-opus-4-8"],
        "latency_ms": [812, 240, 590],
        "ok": [True, True, False],
    }
)


_MISSING = object()


class FakeHotdataClient:
    """Stands in for HotdataClient on the read path."""

    def __init__(self, table: pa.Table | None = None, tables: object = _MISSING) -> None:
        self._table = table if table is not None else CANNED
        # tables=None explicitly models a missing database (list raises).
        self._tables = ["spans"] if tables is _MISSING else tables
        self.calls: list[tuple[str, str]] = []
        self.closed = False

    def execute_sql(self, sql: str, *, database: str) -> pa.Table:
        self.calls.append((sql, database))
        return self._table

    def list_managed_tables(self, database: str, *, schema: str):
        if self._tables is None:
            raise HotdataTerminalError("database not found")
        return list(self._tables)

    def close(self) -> None:
        self.closed = True


def _client(fake: FakeHotdataClient | None = None) -> HotdataSqlClient:
    caps = hotdata().capabilities()
    cfg = HotdataClientConfiguration(
        credentials=HotdataCredentials(api_key="k", workspace_id="w"),
        database_name="db",
        schema="public",
    )
    sc = HotdataSqlClient("db", "public", caps, cfg)
    # Bypass open_connection (which would build a real HotdataClient).
    sc._client = fake if fake is not None else FakeHotdataClient()
    return sc


# --- cursor ---------------------------------------------------------------


def test_cursor_description_reflects_columns() -> None:
    cur = HotdataCursor(CANNED)
    assert [d[0] for d in cur.description] == ["model", "latency_ms", "ok"]
    assert all(len(d) == 7 for d in cur.description)


def test_cursor_fetch_pagination() -> None:
    cur = HotdataCursor(CANNED)
    assert cur.fetchone() == ("claude-opus-4-8", 812, True)
    assert cur.fetchmany(1) == [("claude-sonnet-5", 240, True)]
    assert cur.fetchall() == [("claude-opus-4-8", 590, False)]
    # exhausted
    assert cur.fetchone() is None
    assert cur.fetchmany(5) == []
    assert cur.fetchall() == []


def test_cursor_fetchmany_default_returns_rest() -> None:
    cur = HotdataCursor(CANNED)
    assert len(cur.fetchmany()) == 3


def test_cursor_df_and_arrow() -> None:
    df = HotdataCursor(CANNED).df()
    assert list(df.columns) == ["model", "latency_ms", "ok"]
    assert df.shape == (3, 3)

    tbl = HotdataCursor(CANNED).arrow()
    assert tbl.num_rows == 3
    assert tbl.column_names == ["model", "latency_ms", "ok"]
    # Arrow-native: identical table handed straight back.
    assert tbl.equals(CANNED)


def test_cursor_empty_table() -> None:
    empty = CANNED.schema.empty_table()
    cur = HotdataCursor(empty)
    assert cur.fetchall() == []
    assert [d[0] for d in cur.description] == ["model", "latency_ms", "ok"]


# --- client ---------------------------------------------------------------


def test_execute_query_yields_cursor() -> None:
    fake = FakeHotdataClient()
    sc = _client(fake)
    with sc.execute_query('SELECT * FROM "default"."public"."spans"') as cur:
        assert cur.df().shape == (3, 3)
    # SQL passed through verbatim, scoped by managed database_name.
    assert fake.calls == [('SELECT * FROM "default"."public"."spans"', "db")]


def test_execute_sql_returns_rows() -> None:
    sc = _client()
    rows = sc.execute_sql('SELECT * FROM "default"."public"."spans"')
    assert rows == [
        ("claude-opus-4-8", 812, True),
        ("claude-sonnet-5", 240, True),
        ("claude-opus-4-8", 590, False),
    ]


def test_execute_query_rejects_params() -> None:
    sc = _client()
    with pytest.raises(DatabaseTerminalException), sc.execute_query("SELECT %s", "x"):
        pass


def test_catalog_and_qualified_name() -> None:
    sc = _client()
    assert sc.catalog_name() == '"default"'
    assert sc.make_qualified_table_name("spans") == '"default"."public"."spans"'


def test_begin_transaction_is_noop() -> None:
    sc = _client()
    with sc.begin_transaction() as tx:
        assert tx is sc


def test_has_dataset_uses_list_managed_tables() -> None:
    sc = _client(FakeHotdataClient(tables=["spans"]))
    assert sc.has_dataset() is True

    missing = _client(FakeHotdataClient(tables=None))  # raises HotdataTerminalError
    assert missing.has_dataset() is False


def test_close_connection_closes_client() -> None:
    fake = FakeHotdataClient()
    sc = _client(fake)
    assert sc.native_connection is fake
    sc.close_connection()
    assert fake.closed is True
    assert sc.native_connection is None


# --- exception mapping ----------------------------------------------------


@pytest.mark.parametrize(
    "message",
    [
        "table 'spans' not found",
        'relation "spans" does not exist',
        "No table named spans",
    ],
)
def test_make_exception_maps_undefined_relation(message: str) -> None:
    ex = HotdataSqlClient._make_database_exception(HotdataTerminalError(message))
    assert isinstance(ex, DatabaseUndefinedRelation)


def test_make_exception_finds_undefined_relation_in_cause_chain() -> None:
    # Reality: the SDK collapses the message to "400: Bad Request"; the engine's
    # "table ... not found" only lives in the underlying ApiException. The mapper
    # must scan the cause chain, not just str(ex).
    api_error = Exception("(400) ... table 'default.public.nope' not found ...")
    terminal = HotdataTerminalError("400: Bad Request")
    terminal.__cause__ = api_error
    ex = HotdataSqlClient._make_database_exception(terminal)
    assert isinstance(ex, DatabaseUndefinedRelation)


def test_make_exception_maps_transient() -> None:
    ex = HotdataSqlClient._make_database_exception(HotdataTransientError("503: overloaded"))
    assert isinstance(ex, DatabaseTransientException)


def test_make_exception_maps_terminal() -> None:
    ex = HotdataSqlClient._make_database_exception(HotdataTerminalError("400: bad request"))
    assert isinstance(ex, DatabaseTerminalException)


def test_make_exception_passthrough() -> None:
    original = DatabaseUndefinedRelation(Exception("boom"))
    assert HotdataSqlClient._make_database_exception(original) is original
