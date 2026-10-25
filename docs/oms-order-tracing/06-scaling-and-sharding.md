# 06 — Sizing, scaling and sharding

Replaces sections 4 and 7 of the source document (doc 00 F3, F4, F6, F11). The rule: **one
lineage worker until a measured gate trips; then shard per edge by the parent's id.** Numbers
marked *est.* are engineering estimates to be replaced by the 1 M-row measurement in doc 10
M1; the formulas are what to plug the measurements into.

---

## 1. Where the bytes are

### 1.1 What is on heap and what is not

| Data | Lives | Heap cost in `oms-lineage` |
|---|---|---|
| Raw messages (`OMS_RAW.*`, the day's history) | DIS storage; paged through the Table Data Service with an off-heap page cache | ~0 per row. Not copied onto the heap. |
| `last_by("OrderId")` result | heap | key hash table + one redirection per order |
| Adapter `view` columns | nothing (lazy formulas) | 0 |
| `natural_join` state | heap | right-side hash table per join (keys of the parent table), left-side redirection |
| `agg_by` rollups | heap | state per parent key + result columns |
| `update_view` columns (`FillGap`, `ParentState`, …) | nothing (lazy) | 0 |
| `update`/`select` columns | heap, materialized | **avoid** on the big tables (doc 02 D8) |
| `gaps`, `unresolved_links`, aggregates | heap | ∝ breaks, not orders |

So the heap scales with **distinct orders in the window**, not with messages, and the message
history costs only start-up CPU (one pass per `last_by`).

### 1.2 Per-order heap, by id type (*est.*)

| Component (per order, per system) | String ids (~20 chars) | long ids |
|---|---|---|
| `last_by` key + hash entry + redirection | ~110 B | ~40 B |
| parent-side `natural_join` hash entry (orders that are parents: A, B) | ~90 B | ~40 B |
| child-side `agg_by` state (B, C grouped by `LinkId`) | ~90 B | ~40 B |
| engine row-set / modified-set overhead (~20 %) | ~60 B | ~25 B |
| **total** | **~350 B** | **~145 B** |

String ids dominate because every String is a 24-byte header plus its bytes plus an 8-byte
reference wherever it is held; Latin-1 compact strings keep it at this level, UTF-16 doubles it.
A 20-char id as a `long` is 8 bytes everywhere.

### 1.3 Heap at the two readings of "100 M orders"

| Case | Orders in window (A + B + C) | String ids | long ids |
|---|---|---|---|
| expected (Q1 = 100 M per day across the chain, 2-day window ≈ 1.5 days resident after the daily restart) | ~150 M | ~52 GB | ~22 GB |
| upper (100 M per system per day, 2-day window) | ~450 M | ~155 GB | ~65 GB |

Plus a fixed ~4–8 GB for the JVM, Barrage buffers and Python. Reading: the expected case fits
one 96–128 GB worker with either id type; the upper case fits only with long ids, or sharded.

### 1.4 Levers before sharding

1. **Numeric ids** if the systems have them (A9). If the natural ids are strings but
   per-system numeric surrogate ids exist (an internal order number), carry both and join on
   the numeric one.
2. **Window**: `OMS_TRACE_DATE_WINDOW_DAYS = 2` is driven by Q7. If p99 B-after-C lag is
   minutes, a 1-day window halves resident state; unresolved rows past the window are
   reported, not lost (doc 04 §4).
3. **Closed-order pruning**: orders whose three systems agree and are terminal (`Status`
   filled/canceled, gap 0, aged) can leave the join state: filter them out of `orders_*` with a
   `where_not_in` against a `terminal_ok` table. This trades correctness for a reopened order
   (a late bust on a closed order would not be seen) for heap; use only with Q8 answered.
4. **Persisted lastBy in the ingesters** (doc 05 §4.3) moves the last-by hash tables out of
   the lineage worker and makes its start-up a subscription instead of a scan.

## 2. Time: the update graph and start-up

- A Core+ worker runs one update graph on one thread. Per cycle it recomputes the rows that
  changed: one C fill touches its `links_c` row, one B rollup row, one `edge_bc` row, the gap
  candidates and a few aggregate rows. At 50 k msg/s (Q6 default) that is a few hundred
  thousand row operations per second, which the engine sustains with headroom; the cost that
  matters is the per-cycle fixed overhead, so cycle time (`UpdateGraph` cycle metrics in the
  performance tables) is the metric to watch, not CPU.
- Start-up (and recovery) is one `last_by` pass per system over the window's messages. The
  engine's initial aggregation runs at tens of millions of rows per second from the page cache;
  400 M raw rows (upper case) is minutes, not hours (*est.*; measure in M1 and record in doc 08
  §3 R2).
- Python runs **nothing per message**. The only Python in the hot path is the `break_events`
  listener, which sees transitions (hundreds per minute at most).
- Ingestion is in separate PQs; JSON parsing never competes with the lineage update graph.

## 3. Phase gates — when to add a tier

Measure for a full trading day on UAT at the agreed volume (Q1, Q6). Move to the next phase
when any gate trips twice in a week.

| Gate | Threshold | Response |
|---|---|---|
| G1 heap | `oms-lineage` steady-state heap > 70 % of allocation after GC, or allocation would exceed the dispatcher's `maxPerWorkerHeapMB` | levers §1.4 first; then shard (§4) |
| G2 cycle | update-graph cycle p99 > 1 s (N2) during peak | check for materialized columns and Python in the loop first; then shard |
| G3 recovery | cold start > N6 (15 min) | persisted lastBy in the ingesters (doc 05 §4.3); then shard |
| G4 ingest lag | an ingester's Kafka consumer lag grows during peak | more ingester PQs over partition ranges for that system (writes stay with one DIS table per PQ; see §4.5) |
| G5 UI | the UI PQ's subscriptions exceed a few million rows | subscribe to narrower slices; never to `orders_*` |

The source document's "decide shard count from ingress rate and per-shard CPU" is replaced by
G1–G3: the worker is single-threaded per update graph and bounded by heap; cores are not the
constraint.

## 4. Edge sharding

### 4.1 Shard key per edge

Each edge is reconciled between a parent table and a child table, and the child carries the
parent's id. So each edge shards on the **parent's id**, and every row knows its shard from its
own columns:

| Shard family | Key | A rows | B rows | C rows | Needs a lookup? |
|---|---|---|---|---|---|
| `AB[k]`, k < N | `floorMod(hash(A.OrderId), N) == k` | by `OrderId` | by `LinkId` (= A id) | not read | no |
| `BC[k]`, k < N | `floorMod(hash(B.OrderId), N) == k` | not read | by `OrderId` | by `LinkId` (= B id) | no |

A `BC[k]` shard resolves a C row's root as its B parent's `LinkId`, a local join. An `AB[k]`
shard attributes its gaps to A ids directly. The root-level rollup (`drift_by_a_order`) needs
the `AB` gap for root `r` and the Σ `BC` gaps whose B parents have `LinkId = r`; those come
from different shard families and are combined in the collector (§4.3) — a join over the small
drift tables, not over orders.

This is the source document's §4.4 ("shard B and C by B's id") applied to both edges, and it
is why no mapping table, resolver or pending buffer is needed at any scale.

### 4.2 Routing: filters over the shared raw tables

Each shard PQ runs the doc 04 DAG for one edge with three extra `where` clauses:

```python
N, K = int(os.environ["OMS_TRACE_SHARD_COUNT"]), int(os.environ["OMS_TRACE_SHARD_INDEX"])
EDGE = os.environ["OMS_TRACE_EDGE"]           # AB | BC
def shard(table, col):                        # String.hashCode() is stable across JVMs
    # the null guard matters: a child with no reference must not NPE the whole PQ
    return table.where(f"!isNull({col}) && Math.floorMod({col}.hashCode(), {N}) == {K}")
if EDGE == "AB":
    parents  = shard(raw("OrdersA"), "OrderId")
    children = shard(raw("OrdersB"), "UpstreamRef")
else:
    parents  = shard(raw("OrdersB"), "OrderId")
    children = shard(raw("OrdersC"), "UpstreamRef")
```

This is the pattern the Enterprise pipelining guide describes for data that is not already
partitioned: `where()` splits across JVMs, with the cost that each JVM reads the filter
column of the whole data set. Here that is one string column paged from the DIS — cheap
relative to the state each shard then holds (1/N of §1.3). The B table is read by both
families with different filters; nothing is re-published.

### 4.3 The collector

One small PQ subscribes (Barrage, Core+ client in-worker, doc 07 §4) to each shard's
`drift_edges`, `drift_by_symbol`, `unresolved_links` and `trace_stats`, merges them, and
produces the global `drift_by_a_order`, `drift_by_symbol`, `unresolved_links` and
`trace_stats`. It also hosts the `break_events` listener and feeds the drift writer, so
`OMS_TRACE.DriftEvents` has one writer. Everything it holds is proportional to breaks and
unresolved links, not to orders.

### 4.4 What changes and what does not

| | Single worker (P1) | Edge-sharded (P3) |
|---|---|---|
| DAG code | doc 04 | the same functions; `EDGE`, `N`, `K` select inputs |
| Ingestion | 3 ingester PQs | unchanged |
| Lookups before routing | none | none |
| Cross-worker joins | none | collector joins drift aggregates only |
| Replay / rebalance | restart the PQ | change `N`, restart the shard PQs; state is rebuilt from `OMS_RAW` (no state copy, ever) |
| Failure of one worker | the whole view is down until restart | one shard's edge view is stale; the collector shows its `trace_stats` row as missing |
| Heap per worker | §1.3 | §1.3 / N plus the collector's small heap |

### 4.5 Sharding ingestion (only on G4)

Ingestion shards are independent of lineage shards. If one ingester cannot keep up with a
topic, run several ingester PQs over disjoint Kafka partition ranges. Because one DIS table has
one writer, each ingester writes its own table (`OrdersB_p0`, `OrdersB_p1`, …) and the lineage
adapter `merge`s them; alternatively confirm with Deephaven whether the installed version lets
several in-worker DISes write distinct internal partitions of one table. Ingestion rarely
trips first: JSON parsing of 50 k msg/s is well within one worker.

## 5. Comparison with the source design

| Aspect | Source: hash(root) shards + resolver + mapping table + pending buffer | Here: one worker, then edge shards |
|---|---|---|
| Services on the hot path | DIS ×3, resolver, mapping publisher, pending-buffer PQ, reconciliation PQ, N shard workers | ingester PQ ×3, lineage PQ (later: 2N shard PQs + collector) |
| Lookup before routing | yes, every C message | none |
| Replicated state | the mapping table in every subscriber (~10 GB per copy at 100 M string pairs) | none |
| Out-of-sequence C | buffered, retried every 5 min, flushed on B | a row state that heals in-cycle |
| Added latency | publish hop + retry period | none |
| Failure modes added | §11.7's five | none beyond PQ restart |
| Worker count at 100 M | 50–100 shard workers of 32–64 GB (3–6 TB) | 1 worker of 96–128 GB; sharded upper case: e.g. 2 × 4 shards of 32 GB + collector |

## 6. Rebalancing and shard count changes

Changing `N` is a config change plus a restart of the shard PQs; each rebuilds its edge from
`OMS_RAW` over the window. The collector tolerates shards appearing one by one (it merges
whatever is subscribed). There is no state migration because no shard owns state that is not
derivable from the raw tables.

## 7. Memory settings for the PQs

From the Enterprise memory-tuning guidance: a worker steadily below 50 % heap is
over-allocated; above 80–90 % it is GC-bound. Set `oms-lineage` to the §1.3 estimate / 0.7 and
revisit after the first week with the worker heap metrics. Replicas and spares multiply memory
by `(R + S)`; the lineage PQ runs with none (its state is cheap to rebuild), the UI PQ may run
spares (doc 07 §2).

## 8. Recovery time model

`T_recover ≈ T_pq_start + Σ_systems (rows_in_window / scan_rate) + T_join_init`, where
`scan_rate` is measured in M1 (expected tens of M rows/s per `last_by`) and `T_join_init` is
the one-off hash build (seconds per 10 M keys). Record the measured values in doc 08 §3 R2 and
set N6 from them, with persisted lastBy (doc 05 §4.3) as the lever if they miss.
