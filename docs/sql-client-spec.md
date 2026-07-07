# Tech Spec — Hotdata dlt SQL Client (read / dataset interface)

Status: **Draft** · Target: this repo (`hotdata-dlt-destination`), on `main` @ v0.6.0 · Owner: (you)

## 1. Summary

Today the Hotdata destination can **write** (Parquet upload) but not **read**. This spec adds the
dlt **dataset read interface** so users can query loaded data through dlt's own tooling:

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
(connections → cursors → `fetchall`/`.df()`); Hotdata speaks REST (submit SQL → poll → fetch an
**Arrow** result). The SQL client translates between them. No new storage, no new engine — Hotdata
already runs **Apache DataFusion** server-side; we just expose it through dlt's interface.

## 2. Scope

**In scope**
- A new `HotdataClient.execute_sql(sql, *, database) -> pyarrow.Table` (submit → poll → fetch Arrow),
  mirroring the existing `fetch_table`.
- A `SqlClientBase` implementation (`HotdataSqlClient`) wrapping that `execute_sql`.
- A DB-API cursor over the returned `pyarrow.Table`.
- Wiring the job client as readable (`WithSqlClient`).
- Declaring the SQL dialect capability (`postgres`).
- Unit tests, offline e2e tests, and a runnable round-trip demo.

**Out of scope** (owned elsewhere / explicitly deferred)
- Native/server-side merge & row updates — runtimedb epic (#782/#869); merge stays client-side.
- SCD2 — needs server-side SQL (see §12).
- Track 2 (querying external lakes) — not a Hotdata-managed-data concern.
- Staging datasets, DDL transactions — Hotdata has neither.

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
| `hotdata(Destination)` | `factory.py` | Destination + capabilities. **Change:** set `sqlglot_dialect`. |
| `HotdataJobClient(JobClientBase, WithStateSync)` | `job_client.py` | Lifecycle. **Change:** add `WithSqlClient` + 2 properties. |
| `HotdataLoadJob` | `job_client.py` | Parquet write job. Unchanged. |
| `HotdataClient(ManagedDatabaseClient)` | `hotdata_client.py` | SDK wrapper (`fetch_table`, uploads). **Change:** add `execute_sql(sql, *, database) -> pa.Table` — the SDK has no query entrypoint of its own (only the private `_query_database_scoped` + Arrow fetch). |
| `TableContract` | `contracts.py` | Name mapping. Reused. |
| `HotdataTerminalError` / `HotdataTransientError` | `errors.py` | Error classes. Reused for query errors. |

**New:**

| Class | File | Role |
|---|---|---|
| `HotdataSqlClient(SqlClientBase[HotdataClient])` | `sql_client.py` (new) | The adapter over `HotdataClient.execute_sql`. |
| `HotdataCursor(DBApiCursorImpl)` | `sql_client.py` (new) | DB-API cursor over a `pyarrow.Table`. |

## 5. File layout

```
src/hotdata_dlt_destination/
├── factory.py            # + caps.sqlglot_dialect = "postgres"
├── hotdata_client.py     # + execute_sql(sql, *, database) -> pa.Table  (submit -> poll -> Arrow)
├── job_client.py         # HotdataJobClient: add WithSqlClient + sql_client / sql_client_class
├── sql_client.py         # NEW: HotdataSqlClient + HotdataCursor
└── ... (unchanged)
tests/
├── test_sql_client.py    # NEW: unit tests for the client + cursor
└── test_e2e_inmemory.py  # extend in-memory backend to serve execute_sql; add read tests
scripts/ (or pipelines/)
└── roundtrip_demo.py     # NEW: dlt -> hotdata -> dlt live demo (see §10)
```

## 6. `HotdataSqlClient` — method by method

`class HotdataSqlClient(SqlClientBase[HotdataClient])`

Construction — dlt "dataset" maps to the Hotdata **schema**; the managed **database** is separate
scoping (see §8):

```python
def __init__(self, managed_database, schema, capabilities, config):
    super().__init__(
        database_name=managed_database,     # used only as the execute_sql(database=) scope
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
| `execute_query(query, *args, **kwargs) -> ContextManager[DBApiCursor]` | `@contextmanager`; run `self._client.execute_sql(sql, database=self.database_name)`, wrap the returned `pyarrow.Table` in `HotdataCursor`, `yield` it. Map SDK errors via `@raise_database_error`. | **Every read** — `Relation.to_sql()` → here. |
| `execute_sql(query, *args, **kwargs) -> Sequence[Sequence] \| None` | `with self.execute_query(...) as c: return None if c.description is None else c.fetchall()`. | Base helpers, direct SQL. |
| `begin_transaction() -> ContextManager[DBTransaction]` | No-op: `yield self`. Hotdata/DataFusion has no transactions (`supports_ddl_transactions=False`). | dlt may wrap ops in a txn. |
| `_make_database_exception(ex) -> Exception` (static) | Map undefined-relation → `DatabaseUndefinedRelation`; transient → transient; else terminal. **Scan the whole `__cause__` chain**, not just `str(ex)`: the SDK's `classify_sdk_error` collapses the `ApiException` to `"400: Bad Request"`, so the engine's descriptive `"table … not found"` only appears deeper in the chain (in the underlying `hotdata.exceptions.BadRequestException`). | `@raise_database_error`. |

### Overridden concrete methods

| Method | Behavior | Why |
|---|---|---|
| `catalog_name(quote, casefold) -> str` | Return `"default"` (quoted as needed). | Hotdata's catalog is always `default`; makes `make_qualified_table_name` emit `default.public.<table>`. |
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
                self.config.database_name, self.config.schema, self.capabilities, self.config
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
   `"default"."public"."spans"` because `catalog_name()="default"` and `dataset_name="public"`.
2. **Database scoping — goes into the request, not the SQL.** The managed database is passed as
   `execute_sql(sql, database=self.database_name)`. Query scoping is by database **id**
   (`_query_database_scoped(database_id=...)` → `X-Database-Id` header), so our `HotdataClient.execute_sql`
   resolves **name → id** first via `resolve_managed_database(name).id` — exactly as `fetch_table`
   already does. (This is what the CLI's `-d` flag supplies manually as an id; the SqlClient passes the
   *name* and the id lookup lives in our client.)

**Decision — mirror the write path:** address by the destination's managed `database_name` + the fixed
`public` schema, exactly as writes do. Guarantees reads return what writes wrote, zero write-side
change. (Consequence: dlt's pipeline `dataset_name` is not the addressing key — same as today, which is
why load output shows `dataset None`.) Whether to make `dataset_name` idiomatic later is a joint
product decision with the runtimedb team ("dataset terminology reconciliation") — deferred.

### Result endpoints are database-scoped: `X-Database-Id` (found live, not offline)

The hosted API enforces the `X-Database-Id` header on the query **result** endpoints (`get_result`,
`get_result_arrow`), not only on the query *submit* — but the SDK sends it **only on submit**. Every
follow-up result fetch therefore 400s (`{"code":"BAD_REQUEST","message":"X-Database-Id header is
required: this endpoint is scoped to a database"}`). Fix: `HotdataClient` pins the header on the api
client (`api.set_default_header("X-Database-Id", db.id)`) right after resolving name→id, in **both**
`execute_sql` (read) and an overridden `fetch_table`.

The `fetch_table` override matters **beyond reads**: the write path's merge/upsert read-back and
`WithStateSync` (`_fetch_internal_rows → fetch_table`) hit the same result endpoints, so the same header
gap breaks merge writes and any 2nd-run state-sync on hosted. Pinning it there repairs those too — a
pre-existing hosted bug this work necessarily fixes. (The offline in-memory backend has no HTTP client,
so the pin is a guarded no-op; this class of bug is only observable against a live API, which is why the
local runtimedb / hosted gates in §13 matter and the offline suite alone can't catch it.)

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
caps.escape_literal    = escape_postgres_literal
```

- **`sqlglot_dialect`**: sqlglot/dlt have no `datafusion` dialect; `postgres` is the documented-compatible
  and only supported option. Governs how fluent/ibis queries — *and even "raw" SQL* — are rendered
  (dlt transpiles everything through sqlglot). Risk: DataFusion ≠ byte-for-byte Postgres; a few generated
  constructs may need adjustment, covered by the M2 verification tests.
- **`escape_identifier` / `escape_literal`**: required, and **not** auto-populated by setting the dialect
  in dlt 1.28.1 — they default to `None`. `make_qualified_table_name` calls
  `capabilities.escape_identifier(...)` to quote each name, so without these the first qualified name on
  the read path raises `TypeError: 'NoneType' object is not callable`. Every SQL destination sets them
  explicitly (we mirror `dlt.destinations.impl.postgres`). The write path never needed them, which is why
  they were absent before this work.

## 11. Version linking (why this package pins both dlt *and* the Hotdata SDK)

This adapter sits **between two independently-versioned dependencies and depends on the unstable
internal surface of both**. That is the whole reason it pins both — it is classic middle-of-the-stack
coupling:

```
   dlt ──(internal base classes we subclass)──▶  hotdata-dlt-destination  ◀──(internal query surface we call)── hotdata / hotdata-framework
```

The public contracts of dlt (the destination/capabilities API) and of Hotdata (write: upload + load)
are stable. But the **read** interface reaches past those public contracts on *both* sides — so a minor
bump on either side can break us silently at runtime while the install still "resolves". Caps convert
that silent runtime break into a loud resolution/CI failure.

**Why cap dlt — `dlt>=1.28.1,<1.29`.** We subclass dlt classes that are **not** part of its stable
public API: `dlt.destinations.sql_client.{SqlClientBase, DBApiCursorImpl, WithSqlClient}`. Their shape
moves between dlt **minors** — method signatures, the cursor's `iter_arrow`/`iter_df` contract, even the
import location of `WithSqlClient`. A minor bump can change the base-class contract out from under us.
Built/verified against `dlt==1.28.1`.

**Why cap the Hotdata SDKs — `hotdata>=0.5.0,<0.6`, `hotdata-framework>=0.6.0,<0.7`.** The read path
calls **internal** query surface, not a stable façade: `ManagedDatabaseClient._query_database_scoped`
(private), the `QueryApi`/`ResultsApi`/`ArrowResultsApi` shapes, the poll/status fields, and the
generated `QueryResponse` model. These SDKs move fast (0.4→0.6 in short order) and this is exactly the
surface that shifts. The `X-Database-Id` result-endpoint requirement (§8) is a concrete case: a
server/SDK contract detail our code tracks by hand — precisely the fragility the cap guards.

**How — in `pyproject.toml`:**

```toml
dependencies = [
    "dlt>=1.28.1,<1.29",          # subclass dlt internals -> cap the minor
    "hotdata>=0.5.0,<0.6",         # generated client: QueryResponse / results APIs
    "hotdata-framework>=0.6.0,<0.7",  # query path: _query_database_scoped, Arrow fetch, poll shape
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
- `catalog_name()` → `default`; `make_qualified_table_name("t")` → `"default"."public"."t"`.
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
| ibis over the client | `sql_client` + dialect | live (M3) |

### 13d. Live e2e / manual — the round-trip demo, against **local runtimedb**
This is the "actually running it" test, and the **primary integration target** — the real DataFusion
engine, run locally, no cloud creds or cost. Setup (see `../docs/local-cluster.md`):
1. Bring the stack up: from the `monopoly/` repo, `./local_cluster.sh up` (Docker Desktop kind
   cluster; runtimedb + flightdlt in a `workspace-<id>` namespace). First run: `rebuild`.
2. Get a **local** API key + workspace id from `http://app.localhost/` (`admin@hotdata.dev` /
   `hotdata-local-dev`). This is a *separate* account from hosted `api.hotdata.dev`.
3. Point the destination at the local API — the config already supports it:
   `hotdata(api_base_url="http://api.localhost", ...)` (or `HOTDATA_API_BASE_URL=http://api.localhost`).
4. Run `roundtrip_demo.py` (§14). runtimedb scales from zero on first query — expect a cold-start
   delay.

Tiers, high level: **13a** unit (canned `pyarrow.Table`) and **13b** offline datafusion stand-in run in
CI with no cluster; **13d** local runtimedb is the real-engine integration gate; a hosted
`api.hotdata.dev` smoke is optional. The local cluster is preferred over the datafusion stand-in
wherever it's available because it exercises the actual engine, the managed-DB catalog, auth, and
async query polling.

## 14. Running & testing it for real — `roundtrip_demo.py`

```python
"""dlt -> hotdata -> dlt round trip. Run: set -a; source .env; set +a; uv run python scripts/roundtrip_demo.py
Point at the LOCAL cluster with HOTDATA_API_BASE_URL=http://api.localhost (+ a local key/workspace),
or omit for hosted api.hotdata.dev."""
import dlt
from hotdata_dlt_destination import hotdata

@dlt.resource(name="spans", write_disposition="merge", primary_key="span_id")
def spans():
    yield [
        {"span_id": "a1", "model": "claude-opus-4-8", "latency_ms": 812, "ok": True},
        {"span_id": "a2", "model": "claude-sonnet-5", "latency_ms": 240, "ok": True},
        {"span_id": "a3", "model": "claude-opus-4-8", "latency_ms": 590, "ok": False},
    ]

pipe = dlt.pipeline(
    "roundtrip_demo",
    destination=hotdata(database_name="roundtrip_demo", declared_tables=["spans"]),
)

# 1) WRITE  (works today)
info = pipe.run(spans())
print(info)

# 2) READ   (unlocked by this spec)
ds = pipe.dataset()
print(ds.table("spans").df())                                          # full table
print(ds("SELECT model, avg(latency_ms) AS p FROM spans GROUP BY model").df())  # aggregate
print(ds.table("spans").where("ok = true").order_by("latency_ms").limit(2).df())
tbl = ds.table("spans").arrow()
print(tbl.schema)
```

**Acceptance:** step 2 prints DataFrames matching what step 1 wrote — with **no** database IDs, no
`-d` flag, no CLI — proving the same `pipeline` object writes and reads.

## 15. What the end-to-end flow looks like (dlt → hotdata → dlt)

```
                 WRITE (exists)                         READ (this spec)
 dlt.run(spans) ──normalize→parquet──▶ Hotdata     pipeline.dataset().table("spans").df()
                     upload_parquet     managed DB          │
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
| **M3** | ibis over the client | Hotdata's ibis backend reads through the sql_client end-to-end |
| **M4** | Docs + capability matrix + version caps + CI canary | README matrix shows read ✅, transactions ❌; `roundtrip_demo.py` runs green live |

## 17. Open questions

1. **`dataset_name` semantics** — keep it non-addressing (mirror write path), or make it map to a
   Hotdata schema/DB later? (Joint call with runtimedb team.)
2. **DataFusion `information_schema`** — confirm shape; decide whether `has_dataset`/`tables()` use it
   or the managed-DB API.
3. **SCD2** — client-side emulation vs. wait for server-side merge (§12).
4. **Transactions** — confirm we declare unsupported (recommended).
