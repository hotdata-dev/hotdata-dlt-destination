from types import SimpleNamespace

from hotdata_dlt_destination.hotdata_client import HotdataClient


def test_upload_and_load_managed_table() -> None:
    class FakeRuntime:
        def __init__(self) -> None:
            self.upload_calls = 0
            self.load_calls = 0

        def upload_parquet(self, path: str) -> str:
            self.upload_calls += 1
            assert path.endswith(".parquet")
            return "upload_1"

        def load_managed_table(
            self,
            database: str,
            table: str,
            *,
            schema: str,
            upload_id: str,
        ) -> SimpleNamespace:
            self.load_calls += 1
            assert database == "dlt"
            assert table == "orders"
            assert schema == "public"
            assert upload_id == "upload_1"
            return SimpleNamespace(
                connection_id="conn_1",
                schema_name=schema,
                table_name=table,
                row_count=1,
                full_name=f"{database}.{schema}.{table}",
            )

        def close(self) -> None:
            return None

    client = HotdataClient(
        api_key="k",
        workspace_id="ws_1",
        api_base_url="https://api.hotdata.dev",
        max_retries=2,
        retry_backoff_seconds=0.0,
    )
    fake_runtime = FakeRuntime()
    client._runtime = fake_runtime  # noqa: SLF001

    upload_id = client.upload_parquet("/tmp/batch.parquet")
    loaded = client.load_managed_table(
        "dlt",
        "orders",
        schema="public",
        upload_id=upload_id,
    )

    assert upload_id == "upload_1"
    assert loaded.full_name == "dlt.public.orders"
    assert fake_runtime.upload_calls == 1
    assert fake_runtime.load_calls == 1
    client.close()


def test_fetch_table_rows_skips_unsynced_tables() -> None:
    class FakeRuntime:
        def list_managed_tables(self, database: str, *, schema: str):
            assert database == "dlt"
            assert schema == "public"
            return [SimpleNamespace(table="orders", synced=False)]

        def close(self) -> None:
            return None

    client = HotdataClient(
        api_key="k",
        workspace_id="ws_1",
        api_base_url="https://api.hotdata.dev",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = FakeRuntime()  # noqa: SLF001

    rows = client.fetch_table_rows(database="dlt", schema="public", table="orders")
    assert rows == []
    client.close()


def test_fetch_table_rows_reads_synced_table() -> None:
    class FakeRuntime:
        def list_managed_tables(self, database: str, *, schema: str):
            return [SimpleNamespace(table="orders", synced=True)]

        def execute_sql(self, sql: str):
            assert sql == 'SELECT * FROM "dlt"."public"."orders"'
            return SimpleNamespace(
                to_records=lambda: [{"id": 1, "name": "alpha"}],
            )

        def close(self) -> None:
            return None

    client = HotdataClient(
        api_key="k",
        workspace_id="ws_1",
        api_base_url="https://api.hotdata.dev",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = FakeRuntime()  # noqa: SLF001

    rows = client.fetch_table_rows(database="dlt", schema="public", table="orders")
    assert rows == [{"id": 1, "name": "alpha"}]
    client.close()


def test_fetch_table_rows_returns_empty_when_table_missing() -> None:
    class FakeRuntime:
        def list_managed_tables(self, database: str, *, schema: str):
            return []

        def close(self) -> None:
            return None

    client = HotdataClient(
        api_key="k",
        workspace_id="ws_1",
        api_base_url="https://api.hotdata.dev",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = FakeRuntime()  # noqa: SLF001

    rows = client.fetch_table_rows(database="dlt", schema="public", table="orders")
    assert rows == []
    client.close()
