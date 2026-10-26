# 00 — Architecture review: OMS order tracing (A → B → C)

Reviewed: [`docs/oms-order-tracing-architecture.md`](../oms-order-tracing-architecture.md)
(main, commit `af9a13c`, sections 1–11). Target platform: Deephaven Enterprise with Core+
workers (Python).

The goal of that document is right and its durability principles are right. Its
machinery — a resolver worker, a Barrage-published mapping table, hash-of-root sharding and a
PQ-managed pending buffer with a retry schedule — re-implements, outside the engine, two things
the engine and the Enterprise platform already do: incremental joins that heal when a late
parent arrives, and durable, replayable ingestion through a Data Import Server. The review below
is ranked by what it costs to leave as is. Every "change" row is carried into docs 01–10 as a
decision, so this document can be read on its own and the rest as the answer.

**Topology clarification (2026-10-25, after the first pass of this review):** OMS A holds the
root order; OMS B holds one parent order per root and many child orders; OMS C holds one parent
per B child (as many C parents as B children) and many child orders under each. Five levels,
four edges — the Raptor → algo → router shape the fix42 repo's doc 14 module was built for. The
clarification confirms F1 (fan-out is a fact, not a possibility), tightens F3 (root attribution
below the B parent needs root ids to flow between shard families), and adds F15.

---

## 1. Verdict in one screen

**Keep**

- The problem statement, the three outputs (drift by order, by symbol, by A order) and the
  attribution requirement (§1, §3.5).
- "The log and the partitions are the store; memory is the cache" (§11.1) — but applied to the
  raw per-system tables the DIS already writes, not to a second pending store.
- Unresolved references are counted and kept for forensics, never silently dropped (§8, §11.4).
- Alert TTL decoupled from data retention (§11.4).
- Validate id mapping on a sample and sanity-check key cardinality before trusting diffs (§8).
- Community prototype first, lifted into Core+ unchanged (§10). The
  `deephaven-fix42-dashboard` repo already has the substrate (its doc 14 module solves the same
  tree-and-reconcile problem for Raptor → algo → router).

**Change**

1. Reconcile **per edge, parent against the sum of its direct children**. The per-row
   `a_cum − b_cum` diff is only correct when every A order maps to exactly one B order and
   every B to exactly one C; with any fan-out it reports drift on healthy chains and
   double-counts in every rollup (§2, F1).
2. Delete the resolver worker, the mapping-table publisher and the pending buffer. A
   `natural_join` from C to B is null until B arrives and fills in the same update cycle B
   lands; "pending" is a column state (`UNRESOLVED`), not a place. The durable copy of a
   pending C row is the C intraday table the DIS wrote (F2).
3. If sharding is ever needed, shard **per edge by the parent's id** (four edge families).
   Every row is routable from its own fields at ingest; no lookup, no root resolution before
   routing, no cross-shard join for reconciliation. Root ids for the two deeper edges flow
   down between families as partitioned maps, replicated exactly once (doc 06 §4). §4.4 of
   the source doc is the better design and should be the main line (F3).
4. Use Enterprise ingestion as it actually works: a Core+ Kafka ingester running an
   in-worker DIS per system (checkpointed offsets, optional persisted lastBy), or the
   logger → tailer → DIS path for binary logs. The DIS does not write binary logs, and a
   "DIS tailer" is two different services (F5).
5. Re-do the sizing. Barrage replicates the whole table into every subscriber, string ids
   cost 5–10× what the estimates assume, and 100 workers × 32–64 GB for 1 M rows each is
   three orders of magnitude of over-provisioning. Start with one lineage worker and
   measure before sharding (F4, F6).

**Decide** (open, must be answered before the design is final — doc 01 §8)

- Is "100 M orders" per day, per system, or the lifetime total? Retention and resident
  state hinge on it.
- Fan-out cardinality at each hop, and whether C can ever reference A directly (DMA-style).
- Message semantics: full order state per message, or deltas (a delta feed needs a fold,
  which is a different design).
- Measured lag distribution of B-after-C (p99 and max), which sets the date window.

---

## 2. Findings, ranked

| # | Severity | Finding | Where | Consequence if unchanged | Change |
|---|---|---|---|---|---|
| F1 | **Critical** | Per-row triple diff assumes a strict 1:1:1 chain | §3.2, §6.5, §3.5 | False drift on every fanned-out order; rollups count the parent once per child | Per-edge reconciliation (doc 03 §6, doc 04 §3) |
| F2 | **Critical** | Resolver + mapping table + pending buffer re-implement the engine's join state outside the engine | §2, §4.3, §5, §11 | Three extra services, a 5-minute retry loop in the hot path, six new failure modes (§11.7), minutes of added latency | One lineage DAG with incremental joins; `UNRESOLVED` as a state (doc 04 §4) |
| F3 | **Major** | `hash(a_root_id)` sharding needs the root before routing, which is the whole reason F2 exists | §4.1–4.3 | Every C message blocks on a lookup; the lookup table is the system's largest table | Shard per edge by parent id; routing from each row's own fields (doc 06 §4) |
| F4 | **Major** | "One shared copy over Barrage, not replicated" is the opposite of how Barrage works | §3.3, §5.4 | Each subscriber holds a full copy; the producer builds a full snapshot per subscriber restart; snapshots of a 100 M-row table can hold the update-graph lock | No mapping table. Where tables cross workers, choose Barrage (current state only, small) or persisted tables (history) per the Enterprise pipelining guidance (doc 02 §6, doc 06 §4) |
| F5 | **Major** | DIS/tailer roles inverted; "DIS writes binary logs and intraday partitions" is neither path | §2, §4.3 step 1, §11.1–11.2 | The deployment cannot be built as drawn | Core+ Kafka ingester (in-worker DIS) per system, or logger → tailer → DIS (doc 05) |
| F6 | **Major** | Memory and shard arithmetic | §4.2, §7 | Under-sizes strings and join state, over-provisions workers by ~1000×, cites a Legacy-only lever | Sizing model with measured inputs; single worker until a gate trips (doc 06 §1–3) |
| F7 | Moderate | Legacy Groovy API (`db.t`) in a Core+ design; "no special API" claim | §11.6 | Scripts don't run on Core+ | `db.live_table` / `db.historical_table` in Core+ Python (doc 05 §6, doc 08 §5) |
| F8 | Moderate | "Time-window filter first to shrink the working set" | §6.2 | Drops resting orders that are open but quiet; a ticking time filter rescans the table | Window on the `Date` partition, last-by on order id, age only the rows with a gap (doc 04 §5) |
| F9 | Moderate | Deephaven → Kafka → Deephaven for shard routing, and Kafka or Barrage used interchangeably | §2, §4.3 step 5, §6.1 | A second Kafka dependency, serialization both ways, no exactly-once | Route at ingest (producer key / partition ranges); persist intermediates through a DIS when history matters (doc 05 §8, doc 06 §4) |
| F10 | Moderate | `tree(joined, id_col=c_order_id, parent_col=a_order_id)` over a flat lineage table is not a tree | §6.4 | Parents that aren't nodes become orphans; two-level trees only | One row per node (A, B, C merged) with `ParentKey`, `promote_orphans=True` (doc 04 §7) |
| F11 | Moderate | "Shard count from measured ingress rate and per-shard CPU" | §9 | The binding constraint is the single-threaded update graph and heap per worker, not cores | Phase gates on update-graph cycle time and heap (doc 06 §3) |
| F12 | Minor | Clock skew: "diff only comparable snapshots" | §8 | Transient propagation gaps page people | Gaps are aged: a difference counts only when neither side has moved for N seconds (doc 04 §5) |
| F13 | Minor | `symbol`, `side` "from A (authoritative)" | §3.2 | A child working the wrong instrument is invisible | Compare and flag `MISMATCH` (doc 03 §6) |
| F14 | Minor | "Mapping table publisher death: resolvers buffer briefly" | §8 | A design dependency that shouldn't exist | Gone with F2 |
| F15 | Moderate | "Identify exactly which executions were dropped" is not derivable from cumulative-quantity states | §1 goal line | Expectation gap with the business: states name the edge, the book and the quantity, not the execution | Designed as a second path (Q17 answered 2026-10-26): execution reports matched as a multiset on quantity, price and transaction time, lazily for broken edges only (doc 02 §10, doc 04 §10) |

### F1 — the chain fans out, so diff per edge

An A order of 1 000 shares split into B1 = 600 and B2 = 400, each filled to completion and each
with one C order, is perfectly reconciled. The §3.2 table has one row per C order carrying
`a_cum`, `b_cum`, `c_cum`:

```
c_order  a_cum  b_cum  c_cum  drift_ab (a_cum − b_cum)
C1       1000    600    600   400
C2       1000    400    400   600
                                 → drift_by_a_order = 1000 "dropped" shares on a healthy chain
```

Per edge, `A.cum − Σ(direct children cum) = 1000 − (600 + 400) = 0`. The doc never states the
cardinality it assumes, and OMS chains fan out at exactly these hops (algo slicing at B,
routing at C). The per-edge rule reduces to the per-row diff when fan-out is 1, so nothing is
lost by adopting it. `drift_ac` is redundant: summed over a complete tree, the edge gaps
telescope (`a_cum − Σc_cum = gap_ab + Σgap_bc`), which is also how to attribute: a positive gap
on the A–B edge is shares B never booked; on the B–C edge, shares C never booked. This is the
same rule the fix42 repo's doc 09 and doc 14 use, with the same proof.

### F2 — pending is a state, not a place

In one Deephaven update graph, `c.natural_join(b, on=["UpstreamRef=OrderId"], joins=[...])`
yields nulls for a C row whose B has not arrived, and when B arrives the join result for that
C row is modified in the same cycle. Nothing is retried, nothing is flushed, nothing is
scheduled. So:

- "C before B by seconds to hours" is handled by marking the row `ParentState = UNRESOLVED`
  and letting it heal. The unresolved-links table of §3.5 is `where(ParentState != LINKED)`
  with an age column.
- Durability: the C row was already written to `OMS_RAW.OrdersC` by the DIS at ingest. A
  lineage worker restart re-reads the intraday partitions and recomputes every join,
  including rows that were unresolved for six hours. The pending table of §3.4 is a second
  copy of data the platform already persists.
- Alerting on age (§11.4's one genuine requirement) is a filter and an aggregation over the
  lineage table, not a PQ with a retry budget.
- What the source design never explains is why a resolver should exist at all once the
  mapping is a join: §5's "one left-join against the mapping table" *is* the lineage join,
  only moved to a different process and made stale by a publish hop.

The one real limit the pending buffer addresses is memory for unresolved rows in a resolver
that is separate from the lineage worker. With no resolver there is no such population.

### F3 — route by the parent id, which every row already carries

Hashing on the root makes C unroutable until its root is known, so the design needs a
resolver, which needs the mapping table, which needs the pending buffer. Each edge of the
chain can instead be sharded on the parent's own id:

| Edge | Shard key | A rows routed by | B rows routed by | C rows routed by | Joins inside the shard |
|---|---|---|---|---|---|
| A–B | `hash(A.OrderId)` | `OrderId` | `UpstreamRef` (the A id) | — | A ⋈ B rollup, B → A link |
| B–C | `hash(B.OrderId)` | — | `OrderId` | `UpstreamRef` (the B id) | B ⋈ C rollup, C → B link; the root of a C row is its B parent's `UpstreamRef`, in the same shard |

No row needs a lookup to be routed. The B stream is read twice (once per edge sharding),
which is cheap because it is read from the DIS table, not re-published. With the clarified five-level chain there are four such families (`A-Bp`, `Bp-Bc`, `Bc-Cp`,
`Cp-Cc`), and reconciliation stays local in every one of them. What is not local is the **root**
of the parents in the two deeper families (a `B_CHILD`'s root is its B parent's A reference,
which lives in the `Bp-Bc` family). Doc 06 §4.2 handles it by having each family publish
`(child id → RootId)` sliced by the hash the next family shards on — every pair goes to exactly
one subscriber, so the fleet holds the two spines once, not N copies of a mapping table; a root
that has not arrived is a null column that fills, not a buffered message. The source doc's §4.4
is this design for one edge; applied to all four it needs no resolver on the message path and
no replicated lookup table. (The honest version of the source design — a spine worker holding
only link columns that publishes roots to root-sharded tree workers — is laid out as the
alternative in doc 06 §4.5, for the case where whole trees must be local to a shard.)

### F4 — Barrage replicates, and snapshots cost the producer

Deephaven's own guidance for splitting a workload across workers says a Barrage subscriber
must hold the entire table in memory, and the producer must have enough memory to build a
consistent snapshot of the complete table for each new subscriber. A 100 M-pair mapping
table with string ids is roughly 10 GB per copy, rebuilt as a snapshot every time any
resolver restarts, and large snapshots may need the update graph's exclusive lock. "One shared
copy" is only true if exactly one worker subscribes, in which case the table should live in
that worker. Barrage is the right tool for small, current-state tables (the drift aggregates
to a UI worker); persisted tables through a DIS are the right tool when history or replay
matters. Doc 02 §6 fixes which is used where.

### F5 — how Enterprise ingestion actually works

| What the source doc says | What Enterprise does |
|---|---|
| "DIS-A (tailer)... writes per-system binary logs **and** intraday partitions" | An application (or logger) writes binary logs; the **Data Tailer** ships them; the **DIS** writes intraday partitions and serves them. Two services, neither writes binlogs *and* partitions. |
| Kafka feeds go through "DIS tailers" | For Kafka, Core+ runs a **Kafka ingester inside a worker that embeds a DIS** (`dis.consume(opts)`), persisting to that DIS's private storage with Kafka offsets in the checkpoint record (exactly-once resume), optionally with a persisted `lastBy` view keyed on the order id. No tailer, no binary log. The DIS is named in `routing.yml` or created with `dhconfig dis add`, and claims its tables. |
| "Dumb ingestion... no joins" | Correct, and keep it: ingestion PQs run on the merge dispatcher and do nothing but consume, validate (`on_reject`) and persist. |

Doc 05 is the ingestion design; it also covers the binary-log path for an OMS that can write
Deephaven logs directly, and the Derived Table Writer for persisting derived tables.

### F6 — sizing

- A Java `String` of 16–24 characters costs 56–80 bytes before the column that holds it. Three
  string ids per lineage row is ~200 bytes of ids alone; §7's "4–8 GB for 100 M orders" is
  what *long* ids cost. Doc 06 §1 gives the per-row model for both.
- `last_by`, `natural_join` and aggregation state cost tens of bytes per key on top of the
  data; at 100 M keys that is several GB per operator.
- "100 M / 100 shards = 1 M rows per shard... comfortable on a 32–64 GB worker" asks for
  3–6 TB of RAM to hold a few hundred MB per shard. One Core+ worker with a 64–128 GB heap
  holds the entire working set for the long-id case and probably the string-id case; measure
  on a 1 M-row sample (the fix42 repo has the harness) and extrapolate before anything is
  sharded.
- "Direct memory 2× heap for the data buffer pool" is a Legacy-worker setting. A Core+ worker
  is a JVM with the heap the PQ configuration gives it.

---

## 3. Section-by-section notes on the source document

| § | Note |
|---|---|
| 1 | Keep, and write the topology down: five levels, four edges, 1 : 1 at the cross-system hops and 1 : n inside B and C (doc 01 §1). "100 M unique orders" must get a unit (nodes, per day?). "Identify exactly which executions" needs the F15 caveat. |
| 2 | Replace the diagram with doc 02 §2. Of the six components, three (resolver, mapping publisher, pending-buffer PQ) are removed, one (reconciliation PQ) becomes a filter, and DIS-A/B/C become Kafka-ingester PQs. |
| 3.1 | Keep, rename to Enterprise column conventions (doc 03 §2). Add `Status` (needed to tell a canceled parent from a lagging one) and the Kafka offset for forensics. |
| 3.2 | Replace with one row per **node** at each of the five levels carrying its parent link, `RootId`, child rollups and per-edge gaps (doc 03 §4). A "one row per C" view is a filter over it. The raw contract needs the in-system parent id column that tells B's and C's parents from their children (doc 03 §2). |
| 3.3 | The mapping table is a derived, in-worker join index at most; as an audit trail it is `where(ParentState != LINKED)`. Nothing publishes it. |
| 3.4 | Removed. The §11 arguments for durability apply to `OMS_RAW.OrdersC`, which already exists. |
| 3.5 | Keep the four outputs; definitions move to per-edge semantics (doc 03 §5–6). |
| 4 | Replace with doc 06 §4 (edge sharding) and the phase gates in doc 06 §3. §4.4 is the right direction. |
| 5 | "Do not traverse the tree table at runtime" — agreed, and nobody needs to: four `natural_join`s, top-down, resolve every node's root incrementally and carry it along (doc 04 §2). |
| 6 | Keep the pipeline shape; step 2's time filter becomes a `Date`-partition window (F8); step 4's tree is built over merged nodes (F10); step 5's diffs become edge gaps (F1). |
| 7 | Replace with doc 06 §1. |
| 8 | Keep every guard; "mapping table publisher death" goes away; "clock skew" becomes gap aging (F12). |
| 9 | "Decide shard count from ingress rate and CPU" → phase gates on heap and update-graph cycle (F11). The 1 M-row prototype is the first milestone of doc 10. |
| 10 | Agreed, with one correction: Barrage is not "mapping-table distribution"; it is how a UI worker subscribes to small result tables. |
| 11 | The principle (11.1) is right and is applied to the raw tables. 11.2 is right (the DIS is a writer and server, not a query engine). 11.3's "you need a PQ to read persisted data" is not accurate — any worker, console or client reads `db.live_table` / `db.historical_table`; PQs are how long-running work survives sessions. 11.4–11.7 are removed with the pending buffer. |

---

## 4. What was verified, and against what

The review was checked against the official Deephaven Enterprise documentation reachable at
review time (the site itself could not be fetched from the review environment, so wording is
paraphrased from search excerpts and should be confirmed on the pages for the installed release,
**Deephaven Enterprise 2026.01.054** per doc 01 Q10):

- Core+ Kafka ingestion runs a Data Import Server inside the worker; the ingester resumes from
  Kafka offsets stored in the checkpoint record, which is updated atomically with the table
  size; a `lastBy` view keyed on chosen columns persists its last-row state in the checkpoint;
  the page recommends a "Live Query - Merge Server" persistent query for production
  (`data-guide/streaming/coreplus-kafka/`, `coreplus-lastby/`).
- The Data Tailer monitors binary logs and pushes them to a DIS; the DIS uses checkpoint
  records to truncate and resume (`sys-admin/configuration/data-tailer/`, `data-guide/dis/`).
- Routing: a DIS entry has `filters` or `claims` (not both); `tableDataServices` compose
  sources; the `dataImportServers` source includes DISes added with `dhconfig dis`
  (`sys-admin/configuration/yaml/`).
- Pipelining a workload across workers: persistent tables (binary logs or an embedded DIS,
  exactly-once, resumable) versus Barrage tables (subscriber holds the whole table; producer
  builds a consistent snapshot per subscriber); `where()` splits across JVMs make every JVM
  read the filter columns of the whole data set (`performance/pipeline-your-workload/`).
- The Derived Table Writer persists add-only or blink tables to a DIS with
  `sequenceColumn`, `transactionsEnabled`, `lastBy` and `onReject` options
  (`data-guide/streaming/coreplus-dtw/`).
- Partitioned user tables: `add_partitioned_table_schema`, `add_table_partition`,
  `append_live_table`, `update_partitioned_table_schema`; read with `db.historical_table` /
  `db.live_table` (`deephaven-database/user-tables/`).
- Persisted Core+ input tables are keyed only (`deephaven-database/input-tables/`).
- Core+ Python client in a worker: `connect_to_persistent_query(...)` then
  `barrage_subscribe(...)`, documented as in-worker use only
  (`clients/python/coreplus-python-client/`).
- PQ types Live Query (Script), Batch Query (RunAndDone), Data Merge; heap defaults and
  dispatcher caps; replicas and spares cost `(R + S) × heap`
  (`sys-admin/pq-controller/`, `resources/faq/memory-tuning/`, `performance/pq-redundancy/`).

Not verified and therefore not relied on: a `pq://` URI scheme or a `remote_table` helper in
Core+ Python (the Core+ client path above is used instead), exact Python signatures of the
Kafka ingester options, and a Python wrapper for the Derived Table Writer (the Groovy/Java
names are used with a note).
