# 06 — Sizing, scaling and sharding

Replaces sections 4 and 7 of the source document (doc 00 F3, F4, F6, F11). The rule: **one
lineage worker until a measured gate trips; then shard per edge by the parent's id, with root
ids flowing down between shard families as partitioned maps.** Numbers marked *est.* are
engineering estimates to be replaced by the 1 M-node measurement in doc 10 M1.

---

## 1. Where the bytes are

### 1.1 What is on heap and what is not

| Data | Lives | Heap cost in `oms-lineage` |
|---|---|---|
| Raw messages (`OMS_RAW.*`, the day's history) | DIS storage; paged through the Table Data Service with an off-heap page cache | ~0 per row |
| `last_by("OrderId")` result (one per raw table) | heap | key hash table + one redirection per order |
| Role split `where` and adapter `view` | a row set per level; lazy formulas | small |
| `natural_join` state (four links, four rollup joins) | heap | right-side hash table per join, left-side redirection |
| `agg_by` rollups (four edges) | heap | state per parent key + result columns |
| `update_view` columns (`FillGap`, `ParentState`, `RootId`, …) | nothing (lazy) | 0 |
| `update`/`select` columns | heap, materialized | **avoid** on the big tables (doc 02 D8) |
| `gaps`, `unresolved_links`, aggregates | heap | ∝ breaks, not nodes |

The heap scales with **distinct nodes in the window**, not with messages; message history costs
start-up CPU only (one `last_by` pass per raw table).

### 1.2 Per-node heap, by id type (*est.*)

| Component (per node) | String ids (~20 chars) | long ids |
|---|---|---|
| `last_by` key + hash entry + redirection | ~110 B | ~40 B |
| parent-side join hash entry (every level but `C_CHILD` is someone's parent) | ~90 B | ~40 B |
| child-side `agg_by` state (every level but `A_ROOT` is someone's child) | ~90 B | ~40 B |
| `RootId` carried by the link joins (a reference, the string exists once) | ~10 B | ~8 B |
| engine row-set / modified-set overhead (~20 %) | ~60 B | ~25 B |
| **total** | **~360 B** | **~155 B** |

Five levels do not change the per-node cost: each node is still one last-by key, one parent
hash entry and one child aggregation entry. What five levels change is the **count** of nodes
for a given number of client orders: with fan-out *f* at `Bp-Bc` (up to a few hundred, Q2) and
*g* at `Cp-Cc`, one A order is `1 + 1 + f + f + f·g` nodes. At f = 100 and g = 1 that is ~300
nodes per client order; at f = 300 and g = 3, ~1 500. Q1's total of ~100 M nodes per day is
therefore a few hundred thousand A orders and B parents, tens of millions of B children and C
parents, and the rest in C children (the most, as Q1 says).

### 1.3 Heap at the confirmed volume

| Case | Nodes in window | String ids | long ids |
|---|---|---|---|
| **confirmed** (Q1: ~100 M nodes per day across all systems; 2-day window ≈ 1.5 days resident after the daily restart) | ~150 M | ~54 GB | ~23 GB |
| contingency (volume doubles) | ~300 M | ~108 GB | ~47 GB |

Plus a fixed ~4–8 GB for the JVM, Barrage buffers and Python. Ids are alphanumeric (Q19), so the
string column is the one that applies: the confirmed case fits **one worker of 96–128 GB**; the
contingency needs §4. The long-id column is kept only to show what the ids cost. §4 is designed,
not built, until a gate trips.

### 1.4 Levers before sharding

1. ~~Numeric ids~~ — ruled out: the ids are alphanumeric (Q19), and a numeric surrogate assigned
   at ingest would have to be assigned to the *references* too (`UpstreamRef`, `ParentOrderId`),
   which needs a lookup of the parent's surrogate before a child can be written — the
   mapping-table problem the review removed (doc 00 F2/F3). Strings it is; the heap is bought,
   not engineered away.
2. **Window**: Q7 decides. If p99 lag at every hop is minutes, a 1-day window halves resident
   state; rows past the window are reported as unresolved, not lost.
3. **Closed-order pruning**: nodes whose edges reconcile, are terminal and aged can leave the
   join state (`where_not_in` against a `terminal_ok` table). Q8 confirms that a bust arrives as
   a new state that reduces `CumQty`, possibly on an order that already looked closed, so pruned
   orders must stay resident for a grace period at least as long as the latest bust lag observed
   in M1 (start at the end of the trading day: prune only at the daily restart). Heap saved is
   the fraction of nodes that are terminal and quiet for longer than the grace period.
4. **Persisted lastBy in the ingesters** (doc 05 §4.3) moves the last-by hash tables out of
   the lineage worker and makes its start-up a subscription instead of a scan.

### 1.5 The execution path (P2b)

| Scope | What is resident | Heap | Verdict |
|---|---|---|---|
| `aged_gaps` (default): parents with an aged quantity gap only | per broken parent: its executions and its children's, as per-key counts (tens of rows) plus the `where_in` filter state | ∝ breaks; negligible next to §1.3 | the design |
| `all`: every parent on every edge | per-key counts for every execution at every level, ~100–150 B each | at 300 M executions/day: 30–45 GB, plus the join state | only in its own PQ (fed by the lineage PQ's `links_*` over Barrage) and only if the business needs every execution matched, not just the dropped ones |

The raw execution tables are on disk in either case. The cost to measure in M1 is the
`where_in` re-filter when the candidate set changes: with `OrderId` as a grouping column the
engine can use the index on merged data; on intraday data a candidate change may scan the
`OrderId` column once per change. If that scan approaches the day's size at peak break rates,
the execution path moves to its own PQ so the lineage update graph never waits on it (G2).

## 2. Time: the update graph and start-up

- A Core+ worker runs one update graph on one thread. Per cycle it recomputes the rows that
  changed: one `C_CHILD` fill touches its link row, one `C_PARENT` rollup, one edge row, the
  candidate filter and a few aggregate rows. The budget is set by the close burst (Q6):
  ~80 k msg/s per system, ~240 k msg/s fleet-wide. Messages for the same order inside one cycle
  collapse to one modified row in `last_by`, but at the close most messages are for different
  orders, so plan for ~240 k modified order rows per second flowing through ~8 operators each:
  about 2 M row-operations per second on one thread. The engine sustains that, but with little
  headroom at a 1 s cycle; the worker then batches (longer cycles, a few seconds, no loss), which
  N2 allows during the burst. The M1 replay at 240 k msg/s (doc 09 §5) turns this estimate into a
  measured cycle time; if it exceeds N2, the order of responses is: persisted lastBy in the
  ingesters (removes the three `last_by` passes from this update graph), then the edge families
  of §4.
- Start-up (and recovery) is one `last_by` pass per raw table over the window's messages, then
  the four link joins and four rollups: a few minutes at the confirmed volume (*est.*; measured in M1).
- Python runs **nothing per message**; the only Python in the hot path is the `break_events`
  listener, which sees transitions.
- Ingestion is in separate PQs; parsing never competes with the lineage update graph.
- An outage's backlog (Q7: a system or channel down for hours) replays at ingester speed and is
  the same load as the close burst, with one difference: the replayed states are hours old, so the
  channel is `CATCHING_UP` and its gaps are suppressed until its event time reaches the present
  (doc 04 §5). The feed-health tables cost one small aggregation per raw table and a vector of
  distinct channels per parent in the rollups (~50 B per parent, in-system edges only).

## 3. Phase gates — when to add a tier

| Gate | Threshold | Response |
|---|---|---|
| G1 heap | steady-state heap > 70 % of allocation after GC, or allocation would exceed the dispatcher's per-worker cap | levers §1.4; then §4 |
| G2 cycle | update-graph cycle p99 > 1 s (N2) during peak | check for materialized columns and Python in the loop; then §4 |
| G3 recovery | cold start > N6 (15 min) | persisted lastBy (doc 05 §4.3); then §4 |
| G4 ingest lag | an ingester's Kafka lag grows during the close burst | fewer topics per ingester PQ, down to one (§4.6; Q6 makes topics the natural ingestion unit) |
| G5 UI | the UI PQ's subscriptions exceed a few million rows | narrower slices; never `orders_*` |

The source document's "decide shard count from ingress rate and per-shard CPU" is replaced by
G1–G3: the worker is single-threaded per update graph and bounded by heap; cores are not the
constraint.

## 4. Edge sharding with downward root maps

### 4.1 Why hash(root) sharding needs a resolver, and edge sharding does not

Only `A_ROOT` and `B_PARENT` rows know their root from their own columns. A `B_CHILD` knows
its B parent, a `C_PARENT` its B child, a `C_CHILD` its C parent. Sharding whole trees by the
root therefore needs a lookup before three of the five levels can be routed — that is the
source document's resolver, and the lookup table is the system's biggest table.

Each **edge**, by contrast, is reconciled between a parent level and a child level, and the
child row carries the parent's id. So each edge shards on the **parent's id**, and every row
knows its shard from its own columns:

| Family | Edge | Shard key | Parent rows routed by | Child rows routed by | Root of the parent |
|---|---|---|---|---|---|
| F1 | `A-Bp` | `hash(A.OrderId)` | `OrderId` | `UpstreamRef` (the A id) | its own id |
| F2 | `Bp-Bc` | `hash(Bp.OrderId)` | `OrderId` | `ParentOrderId` | `UpstreamRef` (the A id): **local** |
| F3 | `Bc-Cp` | `hash(Bc.OrderId)` | `OrderId` | `UpstreamRef` (the B child id) | not local: comes from F2 |
| F4 | `Cp-Cc` | `hash(Cp.OrderId)` | `OrderId` | `ParentOrderId` | not local: comes from F3 |

Reconciliation is local in every family. What is **not** local is the root of F3's and F4's
parents, which is needed only for attribution to the A order (`drift_by_a_order`) and the UI.

### 4.2 Root maps flow down, partitioned, exactly once

F2 knows the root of every `B_CHILD` in its shard (it joined the child to its B parent, whose
`UpstreamRef` is the A id). It publishes that as a two-column map, **sliced by the hash of the
child id** — the key F3 shards on:

```
F2[j]  →  root_map_bc[j → k] = links_bc.where(hash(OrderId) % N == k).view(["OrderId", "RootId"])   for every k
F3[k]  ←  merge over j of root_map_bc[j → k]      (exactly the Bc ids F3[k] holds)
F3[k]  →  root_map_cp[k → m] = links_cp.where(hash(OrderId) % N == m).view(["OrderId", "RootId"])   for every m
F4[m]  ←  merge over k of root_map_cp[k → m]
```

Inside F3[k]: `b_child.natural_join(root_map_bc_k, on=["OrderId"], joins=["RootId"])` — the
root attaches whenever its slice delivers; until then `RootState = UNRESOLVED`, exactly as in
the single-worker DAG. Properties:

- **No lookup before routing.** Rows are routed by their own fields at ingest; the root is
  metadata that arrives independently.
- **Exactly-once replication.** Each `(child id, root)` pair goes to one subscriber. Total extra
  memory across the fleet is one copy of the `B_CHILD` and `C_PARENT` spines (two strings per
  node), spread over the shards — not N copies of a 100 M-pair table.
- **No pending buffer.** An unresolved root is a null column that fills; nothing is retried.
- **Lag.** Attribution lags reconciliation by one Barrage hop per level (milliseconds); the gap
  itself is detected in-cycle inside the family.

Transport: Barrage slices (N×N subscriptions per hop; with N = 4 that is 16 per hop) through
the Core+ client in-worker (doc 07 §4). The Enterprise-idiomatic alternative when N grows is a
**persisted lastBy table per producing shard** written with the Derived Table Writer
(`OMS_TRACE.RootMapBc_s<j>`, `lastBy OrderId`) and read by consumers with
`db.live_table(...).where(slice)` merged over `j`: fewer connections, and the map survives a
producer restart. One DIS table has one writer, hence one table per producing shard.

### 4.3 Routing: filters over the shared raw tables

```python
N, K = int(os.environ["OMS_TRACE_SHARD_COUNT"]), int(os.environ["OMS_TRACE_SHARD_INDEX"])
FAMILY = os.environ["OMS_TRACE_FAMILY"]        # F1 | F2 | F3 | F4
def shard(table, col):                         # String.hashCode() is stable across JVMs
    # the null guard matters: a row with no reference must not NPE the whole PQ
    return table.where(f"!isNull({col}) && Math.floorMod({col}.hashCode(), {N}) == {K}")
if FAMILY == "F2":
    parents  = shard(b_all.where(IS_PARENT), "OrderId")
    children = shard(b_all.where(f"!({IS_PARENT})"), PARENT_ID)
elif FAMILY == "F3":
    parents  = shard(b_all.where(f"!({IS_PARENT})"), "OrderId")
    children = shard(c_all.where(IS_PARENT), "UpstreamRef")
# F1 and F4 likewise
```

This is the pattern the Enterprise pipelining guide describes for data that is not already
partitioned: `where()` splits across JVMs, with the cost that each JVM reads the filter column
of the whole data set. Here that is one string column paged from the DIS — cheap relative to the
state each shard then holds (1/N of §1.3). The B table is read by F1 (as children), F2 (both
roles) and F3 (as parents); nothing is re-published.

### 4.4 The collector

One small PQ subscribes to every shard's `drift_edges`, `drift_by_symbol`, `unresolved_links`,
`root_unresolved` and `trace_stats`, merges them, and produces the global `drift_by_a_order`
(sum per `RootId` across the four families), `drift_by_symbol`, `unresolved_links` and
`trace_stats`, including `CParentsMinusBChildren`. It hosts the `break_events` listener and
feeds the drift writer, so `OMS_TRACE.DriftEvents` has one writer. Everything it holds is
proportional to breaks, not nodes. The UI PQ subscribes to the collector.

### 4.5 The fair version of the source design: a spine worker with root-sharded trees

If the UI must show **whole trees locally in one shard** (expand a root and see every level
without a collector), the alternative is a single **spine PQ** that holds only the link columns
(`Level, OrderId, LinkId`) for all nodes, runs the four link joins to produce `(node → RootId)`,
and publishes it sliced by `hash(RootId)` to N **tree shards** that hold full nodes for their
roots and run all four edges and the tree locally. This is the source document's architecture
with the resolver reduced to joins, no Kafka re-publish and no pending buffer. Its costs: the
spine worker holds join state for every node (~40 % of the full worker's heap — a single large
worker again, just smaller), and every node crosses Barrage once. Choose it only if the
whole-tree-per-shard property is worth that; the edge families of §4.2 are the default.

### 4.6 Sharding ingestion (G4)

Ingestion is already parallel by topic (Q6: one ingester per topic, grouped a few per PQ, doc
05 §4.3b), independently of lineage shards. G4 is answered by regrouping: fewer topics per PQ,
down to one PQ per topic at ~10 k msg/s each. Splitting a single topic across PQs by Kafka
partition range is the step after that and needs one table (or internal partition) per PQ; it
should not be necessary at 10 k msg/s per topic.

## 5. Comparison with the source design

| Aspect | Source: hash(root) shards + resolver + mapping table + pending buffer | Here: one worker, then edge families |
|---|---|---|
| Services on the hot path | DIS ×3, resolver, mapping publisher, pending-buffer PQ, reconciliation PQ, N shard workers | ingester ×3, lineage PQ (later: 4N family shards + collector) |
| Lookup before routing | yes, for three of five levels | none |
| Replicated state | the mapping table in every subscriber (~10 GB per copy at 100 M string pairs) | two spines sliced exactly once across shards |
| Out-of-sequence child | buffered, retried every 5 min, flushed on parent | a row state that heals in-cycle |
| Root attribution | via the mapping table, after the resolver | local for F1/F2; one Barrage hop per level for F3/F4 |
| Failure modes added | §11.7's five | none beyond PQ restart |
| Worker count at 100 M nodes | 50–100 workers of 32–64 GB (3–6 TB) | 1 worker of 96–128 GB (48–64 GB with long ids); sharded contingency: e.g. 4 families × 4 shards of ~24 GB + collector |

## 6. Rebalancing and shard count changes

Changing `N` is a config change plus a restart of the family shards (they rebuild from
`OMS_RAW` over the window) and the collector. Root maps are rebuilt by the same joins. No state
migrates because no shard owns state that is not derivable from the raw tables.

## 7. Memory settings for the PQs

From the Enterprise memory-tuning guidance: a worker steadily below 50 % heap is
over-allocated; above 80–90 % it is GC-bound. Set `oms-lineage` to the §1.3 estimate / 0.7 and
revisit after the first week. Replicas and spares multiply memory by `(R + S)`; the lineage PQ
runs with none (cheap to rebuild), the UI PQ may run spares (doc 07 §2).

## 8. Recovery time model

`T_recover ≈ T_pq_start + Σ_tables (rows_in_window / scan_rate) + T_join_init`, where
`scan_rate` is measured in M1 and `T_join_init` is the one-off hash build for the eight joins
(seconds per 10 M keys). Record the measured values in doc 08 §3 R2 and set N6 from them, with
persisted lastBy (doc 05 §4.3) as the lever if they miss.
