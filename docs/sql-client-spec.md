# Tech Spec — Hotdata dlt SQL Client (read / dataset interface)

Status: **Implemented** · Target: this repo (`hotdata-dlt-destination`) @ v0.13.1

## 1. Summary

The Hotdata destination writes via Parquet upload and exposes dlt's **dataset
read interface**, so users can query loaded data through dlt's own tooling:

```python
pipeline.dataset().table("spans").df()          # pandas
pipeline.dataset()("SELECT ... FROM spans").df() # raw SQL
pipeline.dataset().table("spans").arrow()        # pyarrow
```

This is the one gap dlt flagged when it reviewed Hotdata as a first-party destination (dlt PR
[#4013](https://github.com/dlt-hub/dlt/pull/4013), closed with "moving work to this repo"): its
feature table lists **"Dataset read API ❌"** as the sole missing capability. Closing it is the
prerequisite for Hotdata becoming a *verified* dlt destination, eventually upstreamed as
`dlt.destinations.hotdata`.

The mechanism is a thin **adapter**: dlt's read stack speaks the DB-API contract
(connections -> cursors -> `fetchall`/`.df()`); Hotdata speaks REST (submit SQL
-> poll -> fetch an **Arrow** result). The SQL client translates between them.
No new storage, no new engine: Hotdata already runs **Apache DataFusion**
server-side, and the adapter exposes it through dlt's interface.

## 2. Scope

**In scope**
- `HotdataClient.execute_sql(sql) -> pyarrow.Table` (submit -> poll -> fetch
  Arrow), mirroring the existing `fetch_table`.
- A `SqlClientBase` implementation (`HotdataSqlClient`) wrapping that `execute_sql`.
- A DB-API cursor over the returned `pyarrow.Table`.
- Wiring the job client as readable (`WithSqlClient`).
- Declaring the SQL dialect capability (`postgres`).
- Unit tests, offline e2e tests, and a runnable round-trip demo.

**Out of scope** (explicitly deferred)
- SCD2 and `delete-insert` merge strategies.
- Track 2 (querying external lakes) — not a Hotdata-managed-data concern.
- Staging datasets, DDL transactions — Hotdata has neither.
- General Hotdata-as-source resources. `pipeline.dataset()` is destination
  readback for loaded data, not a source connector.

### Coverage against dlt PR [#4013](https://github.com/dlt-hub/dlt/pull/4013)

That PR (the original first-party submission, closed with *"moving work to this repo"*) listed the
whole destination as ✅ **except** five items. All ✅ items are the write path and already live here.
This spec resolves the ❌ list:

| PR #4013 ❌ | PR's stated reason | This work |
|---|---|---|
| **Dataset read API** | "Requires `SqlJobClientBase`" | ✅ **Delivered** — and the stated reason is wrong: it only needs `WithSqlClient` (§3). This is the entire spec. |
| SCD2 | "Requires server-side SQL" | Deferred, documented (§12). Still ❌. |
| Staging area | "No Hotdata staging concept" | N/A — Hotdata has no staging (out of scope, above). |
| Type mapper | "Not needed; Parquet carries its own types" | N/A — confirmed: the read path is pure type-passthrough (§7), no destination-side type logic. |
| Clone table | "No API endpoint" | N/A — no endpoint exists. |

No reviewer feedback was left on the PR beyond the closing note, so the feature table above is the
complete reconciliation.

## 3. Why NOT `SqlJobClientBase`

`SqlClientBase` and `SqlJobClientBase` are **different layers**, not alternatives:

| Type | What it is | Do we need it? |
|---|---|---|
| `SqlClientBase` | The query/connection object (runs SQL, returns cursors) | **Yes** — we build `HotdataSqlClient` |
| `WithSqlClient` | 2-property mixin (`sql_client`, `sql_client_class`) that advertises a SqlClientBase | **Yes** — add to the job client |
| `SqlJobClientBase` | `WithSqlClient + JobClientBase + WithStateSync` **plus** SQL-DDL/SQL-load machinery | **No** |

Reasons `SqlJobClientBase` is the wrong base:

1. **The read API doesn't require it.** dlt's `get_dataset_sql_client` gates purely on
   `isinstance(client, WithSqlClient)` (`dlt/dataset/dataset.py:491`) — not on `SqlJobClientBase`.
2. **It assumes SQL-native writes.** `SqlJobClientBase` generates `CREATE TABLE`/`ALTER TABLE`,
   queries `INFORMATION_SCHEMA`, and loads via SQL `INSERT`/`COPY`. Hotdata loads via Parquet upload
   and manages schema via the managed-DB API. Adopting it would mean overriding/no-oping most of it.
3. **The ecosystem precedent is clear.** Non-SQL stores that expose the read API — **`lance`,
   `lancedb`, `filesystem`** — all use `JobClientBase + WithStateSync + WithSqlClient`, *not*
   `SqlJobClientBase`. `lance`/`lancedb` are the closest analogs to Hotdata (managed columnar stores,
   not SQL engines). We follow their **composition**.

> Note on the internals template: `lance`/`filesystem` implement their sql_client by embedding an
> in-memory **DuckDB** that scans local files (`WithTableScanners`). That does **not** apply to us —
> Hotdata's engine is remote (DataFusion, behind REST). We do **not** embed DuckDB. For the
> `execute_query`/cursor *internals* we mirror the DB-API shape of `DuckDbSqlClient` (a real
> remote-engine client), minus the driver/connection/transaction machinery.

## 4. Classes: what we have vs. what we add

**Existing (unchanged unless noted):**

| Class | File | Role |
|---|---|---|
| `hotdata(Destination)` | `factory.py` | Destination + capabilities, including the SQL dialect for dataset reads. |
| `HotdataJobClient(JobClientBase, WithStateSync, WithSqlClient)` | `job_client.py` | Lifecycle, state sync, and SQL-client exposure. |
| `HotdataLoadJob` | `job_client.py` | Parquet write job. |
| `HotdataClient(ManagedDatabaseClient)` | `hotdata_client.py` | SDK wrapper for managed-table loads, table fetches, and SQL execution. |
| `TableContract` | `contracts.py` | Name mapping. Reused. |
| `HotdataTerminalError` / `HotdataTransientError` | `errors.py` | Error classes. Reused for query errors. |

**Read adapter:**

| Class | File | Role |
|---|---|---|
| `HotdataSqlClient(SqlClientBase[HotdataClient])` | `sql_client.py` | The adapter over `HotdataClient.execute_sql`. |
| `HotdataCursor(DBApiCursorImpl)` | `sql_client.py` | DB-API cursor over a `pyarrow.Table`. |

## 5. File layout

```
src/hotdata_dlt_destination/
├── factory.py            # destination capabilities
├── hotdata_client.py     # managed-table and SQL API wrapper
├── job_client.py         # HotdataJobClient + HotdataLoadJob
├── sql_client.py         # HotdataSqlClient + HotdataCursor
└── ...
tests/
├── test_sql_client.py
└── test_e2e_inmemory.py
scripts/ (or pipelines/)
└── roundtrip_demo.py     # dlt -> hotdata -> dlt live demo
```

## 6. `HotdataSqlClient` — method by method

`class HotdataSqlClient(SqlClientBase[HotdataClient])`

Construction — dlt "dataset" maps to the Hotdata **schema**; the managed **database** is separate
scoping (see §8):

```python
def __init__(self, managed_database, schema, capabilities, config):
    super().__init__(
        database_name=managed_database,     # display label only; scoping resolves by id from config
        dataset_name=schema,                # "public" -> drives default.public.<table>
        staging_dataset_name=schema,        # no staging; mirror dataset_name
        capabilities=capabilities,
    )
    self._config = config
    self._client: HotdataClient | None = None
```

### Abstract methods (must implement)

| Method | Behavior | Used by |
|---|---|---|
| `open_connection() -> HotdataClient` | Construct a `HotdataClient` from config, store on `self._client`, return it. No socket. | dlt opens the client when a dataset is materialized. |
| `close_connection() -> None` | `self._client.close()`; clear it. | Dataset context exit. |
| `native_connection -> HotdataClient` (property) | Return `self._client`. | Base `__getattr__` delegation; ibis backend. |
| `execute_query(query, *args, **kwargs) -> ContextManager[DBApiCursor]` | `@contextmanager`; run `self._client.execute_sql(sql)` (the database is resolved by id from the bound config), wrap the returned `pyarrow.Table` in `HotdataCursor`, `yield` it. Map SDK errors via `@raise_database_error`. | **Every read** — `Relation.to_sql()` → here. |
| `execute_sql(query, *args, **kwargs) -> Sequence[Sequence] \| None` | `with self.execute_query(...) as c: return None if c.description is None else c.fetchall()`. | Base helpers, direct SQL. |
| `begin_transaction() -> ContextManager[DBTransaction]` | No-op: `yield self`. Hotdata/DataFusion has no transactions (`supports_ddl_transactions=False`). | dlt may wrap ops in a txn. |
| `_make_database_exception(ex) -> Exception` (static) | Map undefined-relation → `DatabaseUndefinedRelation`; transient → transient; else terminal. **Scan the whole `__cause__` chain**, not just `str(ex)`: the SDK's `classify_sdk_error` collapses the `ApiException` to `"400: Bad Request"`, so the engine's descriptive `"table … not found"` only appears deeper in the chain (in the underlying `hotdata.exceptions.BadRequestException`). | `@raise_database_error`. |

### Overridden concrete methods

| Method | Behavior | Why |
|---|---|---|
| `catalog_name(quote, casefold) -> str` | Return the database's own catalog, quoted as needed. | `default` only when the database was created WITHOUT a catalog override; otherwise it answers to the override, and a `default`-qualified reference does not resolve. Makes `make_qualified_table_name` emit `<catalog>.public.<table>`. |
| `has_dataset() -> bool` | Override to check via `self._client.list_managed_tables(...)` rather than the base's `INFORMATION_SCHEMA.SCHEMATA` query. | Don't depend on DataFusion's information_schema shape; avoids param-bound SQL. |

### Inherited, unused (documented, not called on the read path)

`create_dataset` / `drop_dataset` / `truncate_tables` / `drop_tables` — emit DDL; the read path never
calls them and storage lifecycle stays in the job client. Leave inherited; if ever called they raise,
which is acceptable (and honest).

### The `WithSqlClient` wiring (in `job_client.py`)

`WithSqlClient`, `SqlClientBase`, and `DBApiCursorImpl` all import from `dlt.destinations.sql_client`.

```python
class HotdataJobClient(JobClientBase, WithStateSync, WithSqlClient):   # + WithSqlClient
    @property
    def sql_client_class(self) -> type[SqlClientBase]:
        return HotdataSqlClient

    @property
    def sql_client(self) -> HotdataSqlClient:
        if self._sql_client is None:
            self._sql_client = HotdataSqlClient(
                self.config.database_id or self.config.database_name,
                self.config.schema, self.capabilities, self.config
            )
        return self._sql_client
```

## 7. The cursor

dlt's `DBApiCursorImpl` already provides `.df()`, `.arrow()`, `iter_df`, `iter_arrow` on top of a
native cursor exposing `description` + `fetch*`. Our native result is a **`pyarrow.Table`** (what
`execute_sql` returns), so we wrap that directly and override `iter_arrow`/`iter_df` to hand back the
Arrow table with **no row-tuple round-trip** — higher type fidelity than the base's
`row_tuples_to_arrow` inference. The `fetch*` surface is still provided for the `execute_sql`/
`fetchall` helper path (e.g. `row_counts`).

```python
class HotdataCursor(DBApiCursorImpl):
    def __init__(self, table: pyarrow.Table):
        self._table = table
        # materialize row tuples once for the fetch* surface
        self._rows = list(zip(*(col.to_pylist() for col in table.columns))) if table.num_columns else []
        self._pos = 0
        super().__init__(self)                       # native_cursor == self

    # DB-API surface consumed by DBApiCursorImpl (fetch* / execute_sql path)
    @property
    def description(self):
        return [(name, None, None, None, None, None, None) for name in self._table.column_names]
    def fetchall(self):
        rows = self._rows[self._pos:]; self._pos = len(self._rows); return rows
    def fetchmany(self, size=None):
        end = len(self._rows) if size is None else self._pos + size
        rows = self._rows[self._pos:end]; self._pos = min(end, len(self._rows)); return rows
    def fetchone(self):
        if self._pos >= len(self._rows): return None
        r = self._rows[self._pos]; self._pos += 1; return r
    def close(self): pass

    # Arrow-native: no row->arrow inference, exact engine types preserved
    def iter_arrow(self, chunk_size=None):
        yield self._table
    def iter_df(self, chunk_size=None):
        yield self._table.to_pandas()
```

Two properties worth stating explicitly:
1. **Row tuples are materialized lazily.** Only the `fetch*` path (`execute_sql` / `row_counts`) needs
   them; `.df()`/`.arrow()` yield the Arrow table directly and never pay the column-major→row-tuple
   copy. (Implemented as a lazy `_rows` property, not eager work in `__init__`.)
2. **We never inspect or coerce values.** Type fidelity is entirely DataFusion → Arrow IPC → pyarrow
   (→ pandas). There is no destination-side type logic — so there is nothing type-specific to unit-test
   in this class; the type round-trip coverage (§13) validates the *pipeline*, not the cursor.

## 8. Name mapping & addressing (the scoping seam)

A single dlt "location" splits into **two independent mechanisms**:

1. **Table path — goes into the SQL.** `make_qualified_table_name("spans")` →
   `"default"."public"."spans"` because `catalog_name()="default"` (this database took no
   catalog override) and `dataset_name="public"`.
2. **Database scoping — goes into the request, not the SQL.** Query scoping is by database **id**
   (`_query_database_scoped(database_id=...)` → `X-Database-Id` header). `HotdataClient.execute_sql`
   resolves the run's database **by id** from the bound config (`database_id`, or the record created
   this run) — never by name — exactly as `fetch_table` does. Instant databases are addressed by id
   only; names are not unique and are not used to look one up.

**Decision — mirror the write path:** address by the run's instant database
**id** + the fixed schema, exactly as writes do. This guarantees reads return
what writes wrote without changing the write path. The dlt pipeline
`dataset_name` is not the instant-database addressing key; whether to make it map
to a Hotdata schema or database later is a product/API decision.

### Result endpoints are database-scoped: `X-Database-Id`

Hotdata query submit and result-fetch requests are scoped to an instant database.
`HotdataClient` resolves the database once and carries that scope through both
SQL reads and table fetches. The table-fetch path matters beyond dataset reads:
fallback merge paths and `WithStateSync` also fetch managed-table contents.

Offline tests can validate SQL generation and cursor behavior, but a live smoke
test is still useful because it exercises API scoping, async query polling, and
Arrow result fetching end to end.

## 9. Parameter handling

- **Fluent/raw read path passes literal SQL** — `Relation` calls `execute_query(self.to_sql())` with
  **no** bound args (`dlt/dataset/relation.py:246`). So the primary path needs no param binding.
- **Base helper methods** (e.g. the default `has_dataset`) use `%s` + args. The Hotdata query API takes
  a plain SQL string (no bind protocol), so we avoid this by **overriding `has_dataset`** (§6) and, for
  completeness, having `execute_query` reject/inline `*args` safely. We do **not** advertise
  parameterized queries as a feature.

## 10. Capability changes

`factory.py::_raw_capabilities`:

```python
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_postgres_literal

caps.sqlglot_dialect  = "postgres"                    # engine is DataFusion, Postgres-compatible
caps.escape_identifier = escape_postgres_identifier   # quotes "default"."public"."t"
caps.escape_literal    = escape_hotdata_literal      # NOT postgres: it emits E'...'
```

- **`sqlglot_dialect`**: sqlglot/dlt have no `datafusion` dialect; `postgres` is the documented-compatible
  and only supported option. Governs how fluent/ibis queries — *and even "raw" SQL* — are rendered
  (dlt transpiles everything through sqlglot). Risk: DataFusion ≠ byte-for-byte Postgres; a few generated
  constructs may need adjustment, covered by the M2 verification tests.
- **`escape_identifier` / `escape_literal`**: required, and **not** auto-populated by setting the dialect
  in dlt 1.28.1 — they default to `None`. `make_qualified_table_name` calls
  `capabilities.escape_identifier(...)` to quote each name, so without these the first qualified name on
  the read path raises `TypeError: 'NoneType' object is not callable`. Every SQL destination sets them
  explicitly. `escape_identifier` mirrors `dlt.destinations.impl.postgres`; `escape_literal` deliberately
  does **not** — the Postgres one emits the extended form `E'...'`, which this engine's parser rejects
  outright (`Expected: an expression, found: E'...'`), so it would fail every predicate carrying a
  literal. `escape_hotdata_literal` (in `escape.py`) keeps dlt's shape without the prefix. The write path
  never needed either, which is why they were absent before this work.

### Three front-ends, one execution path

Raw SQL, dlt's fluent API, and ibis expressions are three ways to *author* a read; all compile (via
sqlglot, `postgres` dialect) to the **same** SQL against `"default"."public".<table>` and execute
through the **same** `execute_query`. The same query, three ways, compiles to:

```sql
-- raw:    ds("SELECT span_id, model, latency_ms FROM spans WHERE ok ORDER BY latency_ms LIMIT 2")
-- fluent: ds.table("spans").select(...).where("ok").order_by("latency_ms").limit(2)
-- ibis:   t.filter(t.ok).select(...).order_by("latency_ms").limit(2)
SELECT "…"."span_id", "…"."model", "…"."latency_ms"
FROM "default"."public"."spans"
WHERE "…"."ok" ORDER BY "latency_ms" [ASC] LIMIT 2
```

Differences across the three are cosmetic only: the table alias (`"spans"` vs ibis's positional `"t0"`),
explicit `ASC`, and ibis layering an aggregate into a subquery (`SELECT … FROM (SELECT … GROUP BY …)
ORDER BY …`) — all semantically identical, all optimized away by DataFusion. ibis also reaches past the
fluent API (e.g. `group_by`/`aggregate`, which dlt's fluent surface doesn't expose). The only ibis mode
that does **not** route through this path is the live backend (`dataset().ibis()`, §16) — it connects to
the engine through the `ibis.hotdata` backend rather than the sql_client.

## 11. Version linking (why this package pins both dlt *and* the Hotdata SDK)

This adapter sits **between two independently-versioned dependencies** and uses
version-sensitive API surfaces from both. The package pins both sides so changes
surface as dependency or CI failures instead of runtime surprises:

```
   dlt -> hotdata-dlt-destination <- hotdata / hotdata-framework
```

The public contracts of dlt (the destination/capabilities API) and of Hotdata (write: upload + load)
are stable. The **read** adapter also depends on dlt SQL-client base classes and
Hotdata query/result SDK models. A minor bump on either side can change those
shapes while the install still resolves. Caps convert that silent runtime break
into a loud resolution or CI failure.

**Why cap dlt — `dlt>=1.28.1,<1.29`.** We subclass dlt classes that are **not** part of its stable
public API: `dlt.destinations.sql_client.{SqlClientBase, DBApiCursorImpl, WithSqlClient}`. Their shape
moves between dlt **minors** — method signatures, the cursor's `iter_arrow`/`iter_df` contract, even the
import location of `WithSqlClient`. A minor bump can change the base-class contract out from under us.
Built/verified against `dlt==1.28.1`.

**Why cap the Hotdata SDKs — `hotdata>=0.9.0,<0.10`,
`hotdata-framework>=0.13.0,<0.14`.** The read path uses query/result API models,
managed-table layout metadata, and the framework load helpers that carry native
append/delete/update/upsert support. These SDKs move quickly, so the destination
tracks one tested minor at a time.

The framework floor sits at 0.13 because every load on the `append`/`replace`
path goes out as `mode="append"`, and an earlier framework runs an append at most
once — leaving the loads this package issues most, dlt's `_dlt_pipeline_state` /
`_dlt_loads` / `_dlt_version` bookkeeping among them, outside the caller's retry
budget. Widening this cap is also what lets a consumer adopt a newer framework at
all: a transitive cap here bounds them whatever their own pin says.

**How — in `pyproject.toml`:**

```toml
dependencies = [
    "dlt>=1.28.1,<1.29",          # subclass dlt internals -> cap the minor
    "hotdata>=0.9.0,<0.10",        # generated client: query/results APIs
    "hotdata-framework>=0.13.0,<0.14",  # managed-table load and layout helpers
    ...
]
```

Caps are on the **minor** because the coupling is to internal, not public, surface (`<2` would be the
absolute floor if a looser cap is ever wanted).

- **CI canary (follow-up):** a job that installs the upper-bound-latest of each dep and runs the read
  tests, so a dependency bump fails loudly in CI instead of silently breaking users at runtime.
- **Upstream note:** if contributed into dlt as `dlt.destinations.impl.hotdata`, the dlt-version
  coupling disappears (it lives in-tree and moves with dlt); the Hotdata SDK caps remain.

## 12. SCD2 (deferred — reviewer follow-up)

dlt's SCD2 is a **merge strategy** adding validity columns (`_dlt_valid_from`/`_dlt_valid_to`) and
requires the destination to *close out* superseded row versions — normally via server-side SQL
`UPDATE`. Hotdata has no server-side update yet (merge is client-side re-upload), so PR #4013 marked
SCD2 ❌. DuckLake's "versions" are **table-level time-travel**, a different mechanism — not dlt SCD2.
Options to evaluate *after* the read path lands: (a) emulate SCD2 client-side (compute validity in
Arrow, re-upload the full table — like today's merge), or (b) wait for server-side merge (#782/#869).
Not blocking this spec.

## 13. Test plan

### 13a. Unit tests (`test_sql_client.py`)
Drive `HotdataSqlClient`/`HotdataCursor` with a fake `HotdataClient` whose `execute_sql` returns a
canned `pyarrow.Table`:
- `execute_query` yields a cursor; `fetchall/fetchmany/fetchone` paginate correctly.
- `description` reflects columns; `.df()` and `.arrow()` produce the right shape/values.
- `catalog_name()` → the database's own catalog (`default` absent an override); with
  `default` that makes `make_qualified_table_name("t")` → `"default"."public"."t"`.
- `begin_transaction()` is a no-op context manager.
- `_make_database_exception` maps undefined-relation → `DatabaseUndefinedRelation`.
- `has_dataset()` uses `list_managed_tables`, not information_schema.

### 13b. Offline e2e (`test_e2e_inmemory.py`)
Extend the existing in-memory backend so its `query()` transport actually **executes SQL** over the
stored Arrow tables, then assert full round trips:
- `pipeline.run(...)` then `pipeline.dataset().table("t").df()` returns the loaded rows.
- Raw SQL, `.limit()/.select()/.where()/.order_by()`, `.arrow()`, `dataset.row_counts()`.
- **Execution engine for the fake backend:** add a **test-only** dependency to run the SQL. Prefer
  `datafusion` (PyPI) — it *is* the real engine, so dialect behavior matches exactly; `duckdb` is an
  acceptable fallback. (Neither is currently installed — add under `[dependency-groups] dev`.)

### 13c. Read-flow coverage matrix

| Flow | Method exercised | Tier |
|---|---|---|
| `dataset("SELECT …").df()` | `execute_query` → cursor `.df()` | unit + offline |
| `dataset.table("t").df()/.arrow()/.fetchall()` | name mapping + cursor | offline |
| `.limit()/.head()/.select()/.where()/.order_by()` | sqlglot → `postgres` → `execute_query` | offline + live |
| `dataset.row_counts()`, `dataset.tables()` | aggregate/catalog paths | offline |
| `has_dataset()` | override | unit |
| ibis **expressions** (`.table("t").to_ibis()` → compile → SQL) | sqlglot(postgres) → `execute_query` | offline + live |
| ibis **live backend** (`dataset().ibis()`) | wraps dlt's `create_ibis_backend` → `ibis.hotdata` | offline + live |

### 13d. Live e2e / manual — the round-trip demo

This is the "actually running it" test against a real Hotdata API endpoint:

1. Set `HOTDATA_API_KEY`.
2. Pass `--workspace-id <workspace_id>` to the demo.
3. Optionally set `HOTDATA_API_BASE_URL` when targeting a non-default Hotdata API
   endpoint.
4. Run `roundtrip_demo.py` (§14).

Tiers, high level: **13a** unit tests use canned `pyarrow.Table` values, **13b**
offline tests use a DataFusion stand-in, and **13d** live smoke tests exercise
the actual engine, instant-database catalog, auth, and async query polling.

## 14. Running & testing it for real — `roundtrip_demo.py`

`scripts/roundtrip_demo.py` is the source of truth for the live round-trip
example. It writes a small `spans` table, reads it back through
`pipeline.dataset()`, and then prints a manual Hotdata SDK query for comparison.

```bash
export HOTDATA_API_KEY=<api_key>
uv run python scripts/roundtrip_demo.py --workspace-id <workspace_id>
```

The first run creates an instant database and prints its id. Pass that id on later
runs to reuse the same database:

```bash
uv run python scripts/roundtrip_demo.py \
  --workspace-id <workspace_id> \
  --database-id <database_id>
```

**Acceptance:** the dlt dataset read prints rows and aggregates matching what
the write step loaded.

## 15. What the end-to-end flow looks like (dlt → hotdata → dlt)

```
                 WRITE (exists)                         READ (this spec)
 dlt.run(spans) ──normalize→parquet──▶ Hotdata     pipeline.dataset().table("spans").df()
                     upload_parquet     instant DB          │
                     load_managed_table (DataFusion)        ▼
                                                    Relation.to_sql()
                                                      "SELECT * FROM default.public.spans"
                                                            │
                                                    HotdataSqlClient.execute_query
                                                      → client.execute_sql(sql, database="roundtrip_demo")
                                                        (resolve name→id, submit, poll, fetch Arrow)
                                                      → pyarrow.Table
                                                            │
                                                    HotdataCursor → .df() → pandas.DataFrame
```

## 16. Milestones & acceptance criteria

| # | Deliverable | Done when |
|---|---|---|
| **M1** | `HotdataSqlClient` + `HotdataCursor` + `WithSqlClient` wiring | `dataset().table("t").df()` returns loaded rows against the in-memory backend |
| **M2** | `sqlglot_dialect="postgres"` + fluent queries | `.where()/.limit()/.order_by()`/aggregates run live on DataFusion; failures triaged |
| **M3** | ibis support | ibis **expressions** (`.to_ibis()` → compile → SQL) run through the sql_client — **works with no extra code** (rides the postgres dialect + `execute_query`). The live ibis **backend** (`dataset().ibis()`) is supported by wrapping dlt's `create_ibis_backend` (see below) |
| **M4** | Docs + capability matrix + version caps + CI canary | README matrix shows read ✅, transactions ❌; `roundtrip_demo.py` runs green live |

**How the live ibis backend is supported.** dlt's `create_ibis_backend` (`dlt/helpers/ibis.py`) is a closed `issubclass(destination.spec, …)` dispatch with no third-party hook; unknown destinations hit `else: raise NotImplementedError`. Rather than fork dlt, the package wraps that function (`ibis_backend.py`, installed on import): a Hotdata client gets a live `ibis.hotdata` connection — the out-of-tree `hotdata-ibis` backend, which speaks the same REST/Arrow query path as this sql_client — and every other destination is delegated to dlt's original dispatch unchanged. The connection binds the instant database by id. This became possible once `hotdata-ibis` moved to ibis 12 with id-only instant-database addressing; it ships behind the `[ibis]` extra.

## 17. Open questions

1. **`dataset_name` semantics** — keep it non-addressing, or make it map to a
   Hotdata schema/database later?
2. **DataFusion `information_schema`** — confirm shape; decide whether `has_dataset`/`tables()` use it
   or the managed-DB API.
3. **SCD2** — client-side emulation vs. wait for server-side merge (§12).
4. **Transactions** — confirm we declare unsupported (recommended).
