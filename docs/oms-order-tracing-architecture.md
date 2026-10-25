# OMS Order Tracing Architecture (A → B → C)

Cross-system order-state tracing across three OMS platforms using Deephaven Enterprise.
Goal: detect cum-share drift between upstream and downstream systems and identify exactly which executions were dropped.

---

## 1. Problem Statement

- Three OMS systems: **A** (upstream), **B** (mid), **C** (downstream).
- Every order in B carries a reference to its originating A order ID. Every order in C carries a reference to its originating B order ID.
- C does **not** know A's root order ID directly.
- Volume: ~100 million unique orders flowing through the chain. Not all need to reside in memory at once, but the working set for recently active orders is large.
- Requirement: compare cumulative executed shares at A vs B vs C per order lineage, surface non-zero diffs, and attribute dropped shares to the specific downstream system that missed them.

---

## 2. High-Level Topology

```
┌─────────────┐   binary log / Kafka    ┌──────────────────┐
│  OMS A feed │ ───────────────────────▶│  DIS-A (tailer)  │
└─────────────┘                         └────────┬─────────┘
                                                 │
┌─────────────┐   binary log / Kafka    ┌───────▼──────────┐
│  OMS B feed │ ───────────────────────▶│  DIS-B (tailer)  │
└─────────────┘                         └───────┬──────────┘
                                                 │
┌─────────────┐   binary log / Kafka    ┌───────▼──────────┐
│  OMS C feed │ ───────────────────────▶│  DIS-C (tailer)  │
└─────────────┘                         └───────┬──────────┘
                                                 │
                                    ┌────────────▼────────────┐
                                    │   Kafka / binary-log     │
                                    │   topic per OMS system   │
                                    └────────────┬─────────────┘
                                                 │
                      ┌──────────────────────────┼──────────────────────────┐
                      ▼                          ▼                          ▼
             ┌─────────────────┐     ┌─────────────────────┐    ┌─────────────────────┐
             │ Resolver Worker │     │  Shard Worker 0..N  │    │ Mapping Table       │
             │ (join chain +   │     │ (per-shard last-by, │    │ Publisher (single   │
             │  root resolution│     │  A-B-C join, tree,  │    │  Core+ PQ)          │
             │  + shard route) │     │  cum-share diff)    │    │                     │
             └────────┬────────┘     └──────────┬──────────┘    └──────────┬──────────┘
                      │                         │                          │
                      └────────────┬────────────┴──────────────────────────┘
                                   ▼
                      ┌────────────────────────────┐
                      │ Drift / dropped-shares     │
                      │ output tables + alerts     │
                      └────────────────────────────┘
```

### Component roles

| Component | Role | Heap profile |
|---|---|---|
| **DIS-A / DIS-B / DIS-C** | Dumb ingestion. Receive OMS feeds (binary logs or Kafka), validate schema, write to per-system binary logs or Kafka topics. No joins, no routing logic. | Small, stateless |
| **Resolver Worker** | Consumes raw A/B/C streams. Runs the A→B→C join chain. Resolves C's root A ID via the mapping table. Hashes the root ID to assign a shard. Publishes resolved rows to shard-specific topics. | Medium-large |
| **Shard Workers (0..N)** | Each owns one shard of the order universe. Runs last-by, intra-shard joins, tree-table construction, cum-share diff, and drift detection. | Per-shard working set only |
| **Mapping Table Publisher** | Single Core+ persistent query. Maintains the flat `downstream_order_id → a_root_id` table. Publishes over Barrage to all resolver/ingress subscribers. | ~2–4 GB at 100M pairs |

---

## 3. Data Model

### 3.1 Per-OMS raw tables (one per system, blink or append-only from DIS)

Common columns across A, B, C:

| Column | Type | Notes |
|---|---|---|
| `order_id` | long / string | System-local unique order ID |
| `symbol` | string | |
| `side` | byte / string | |
| `qty` | long | Original order quantity |
| `cum_shares` | long | Cumulative executed shares (the value we diff) |
| `upstream_ref_id` | long / string | B→A reference, C→B reference. Null on A rows. |
| `ts` | Instant | Event timestamp |

### 3.2 Joined lineage table (produced by resolver / shard workers)

One row per C order (deepest node), carrying the full ancestry:

| Column | Source |
|---|---|
| `c_order_id` | C.order_id |
| `b_order_id` | B.order_id (via C.upstream_ref_id = B.order_id) |
| `a_order_id` | A.order_id (via B.upstream_ref_id = A.order_id) |
| `symbol`, `side` | from A (authoritative) |
| `a_cum`, `b_cum`, `c_cum` | cum_shares from each system |
| `drift_ab` | a_cum − b_cum |
| `drift_bc` | b_cum − c_cum |
| `drift_ac` | a_cum − c_cum |

### 3.3 Mapping table (the root-ID index)

Two columns only:

| Column | Type |
|---|---|
| `downstream_order_id` | long / string |
| `a_root_id` | long / string |

- Keyed on `downstream_order_id` (last-by, so it always holds the current root).
- At 100M pairs: ~2–3 GB. One shared copy published over Barrage — **not** replicated per shard.
- Also serves as an audit trail: a C message whose B reference does not resolve is a break in the chain, flagged before it becomes silent false drift.

### 3.4 Drift output tables

- `drift_by_order`: per-lineage-row diffs (filter where any drift ≠ 0).
- `drift_by_symbol`: rolled up by symbol — "system B missed X shares from A today."
- `drift_by_a_order`: rolled up by root A order — the number system A needs to see.
- Unresolved-link table: C/B references that failed mapping lookup, with counts and last-seen timestamps.

---

## 4. Sharding Strategy

### 4.1 Shard key = hash(a_root_id)

- Hash A's order ID into N buckets (N = shard-worker count, e.g. 50–100).
- **Why the root ID and not B's or C's ID:** B and C orders always point backward to A. Hashing on the root guarantees every node of a lineage tree lands in the same shard. No cross-shard joins, no distributed tree assembly.
- Symbol skew is irrelevant because the hash is over the order ID space, which is essentially uniform.

### 4.2 Per-shard working set

- 100M orders / 100 shards = ~1M rows per shard.
- Columnar footprint: symbol, side, qty, cum shares, three order IDs ≈ 4–8 GB raw across all shards; per shard a few hundred MB. Comfortable on a 32–64 GB worker even with join state on top.

### 4.3 Routing flow

1. DIS-A/B/C land raw messages (stateless).
2. Resolver worker consumes all three streams.
3. A rows: root = own order ID → route to shard(hash(a_id)).
4. B rows: join B.upstream_ref_id → A to get root → route to shard(hash(root)).
5. C rows: left-join C.order_id → mapping table for root. **Hit:** route to shard(hash(root)). **Miss:** buffer briefly (mapping may lag) or flag as unresolved.
6. Shard worker receives only its shard's resolved rows and runs the per-shard pipeline.

### 4.4 Alternative when the mapping table feels heavy at ingress

Shard the B and C streams by B's order ID and the A stream by A's ID. B-workers emit resolved (b_id, a_root) pairs tagged with the root; downstream shard workers subscribe to those pairs. Trades a small replicated lookup table for zero cross-shard dependency at C ingress. Pick based on whether C-side latency or mapping-table size is the tighter constraint.

---

## 5. Root-ID Resolution for OMS C

**Do not traverse the tree table at runtime.** Tree tables are UI/display structures; per-message tree walking is slow and the wrong tool.

Instead:

1. The **join chain does the relational work once**: C → B on `C.upstream_ref_id = B.order_id`, B → A on `B.upstream_ref_id = A.order_id`.
2. Every resolved row **emits into the mapping table**: `(downstream_order_id, a_root_id)`, maintained with last-by on the downstream ID.
3. When a fresh C message arrives at a resolver, it does **one left-join** against the mapping table on C's order ID. A single hash lookup returns the root instantly.
4. The mapping table is published by one Core+ persistent query over **Barrage** so every resolver and ingress point sees the identical index.

This keeps ingress dumb and the relational intelligence in the workers where heap and CPU live.

---

## 6. Per-Shard Pipeline

Each shard worker runs the identical, independent pipeline:

1. **Ingest** resolved rows for this shard from the shard-specific topic.
2. **Last-by** on order ID per system (A, B, C) → current state, not full history. If only recently-ticked orders matter, apply a time-window filter first to shrink the working set further.
3. **Join chain**: A ↻ B ↻ C inside the shard (aj / exact join on the reference columns). All joins are intra-shard — no network hops.
4. **Tree table** (optional, for UI): `tree(joined, id_col=c_order_id, parent_col=a_order_id)` (or B as parent for a two-level view). Collapsed by default; expandable per lineage.
5. **Cum-share diff**: `drift_ab = a_cum - b_cum`, `drift_bc = b_cum - c_cum`, `drift_ac = a_cum - c_cum`.
6. **Filter** non-zero diffs → drift_by_order. Roll up by symbol and by a_order_id → the summaries system A consumes.
7. **Emit** drift tables to output namespaces / alert on thresholds.

---

## 7. Memory Budget (100M orders, Enterprise)

| Item | Estimate | Notes |
|---|---|---|
| Raw columns (all shards combined) | 4–8 GB | symbol, side, qty, cum, 3 IDs |
| Per-shard worker heap | 8–16 GB | 1M rows + join state |
| Mapping table | 2–4 GB | 2 cols × 100M pairs, single copy |
| Resolver worker | 16–32 GB | join chain + routing buffers |
| DIS tier | < 2 GB each | stateless tailers |
| Tree table (UI layer) | negligible server-side | viewporting pushes only visible rows to browser |

Enterprise levers used: large heap workers, direct memory (2× heap for the data buffer pool), Barrage chunked snapshots, formula columns via updateView for derived fields, and on-disk partitioned historical tables for anything that must survive beyond RAM.

---

## 8. Failure Modes & Guards

- **Unmapped C reference** (B ID not in mapping table): not silently dropped. Counted in the unresolved-link table with last-seen timestamp. A sustained rise indicates a B-side ingestion gap.
- **Re-keyed or batched downstream messages**: join keys stop lining up 1:1 → false drift. Validate ID mapping on a small sample before trusting diffs at scale; add a sanity check that B.order_id cardinality ≈ expected.
- **Shard worker death**: shard is rebuilt from the binary log / Kafka topic (durable), so no data loss — just replay lag.
- **Mapping table publisher death**: resolvers buffer C messages briefly; on republish, buffered messages resolve and flush. Alert if buffer depth exceeds a threshold.
- **Clock skew between OMS systems**: diffs on cum_shares are point-in-time; pair each cum value with its timestamp and diff only comparable snapshots, or diff on the latest-seen value per system with an explicit as-of note.

---

## 9. Open Questions / Next Steps

- [ ] Confirm whether C messages can arrive before their B parent has been ingested (ordering guarantees across the three feeds).
- [ ] Decide shard count from measured peak ingress rate and per-shard CPU.
- [ ] Prototype the resolver join chain on a 1M-row sample to validate join key cardinality before production cutover.
- [ ] Define alert thresholds on drift_by_symbol and unresolved-link counts.
- [ ] Decide retention: how long raw per-OMS history is kept on disk vs. in-memory last-by state.

---

## 10. Why This Splits Cleanly Across Community → Enterprise

- All table operations (last-by, aj, tree, updateView, where) are **Community Core** semantics — the code ports unchanged into Enterprise Core+ workers.
- What Enterprise adds that this design depends on: **Persistent Queries** (the mapping-table publisher and shard workers survive sessions), **ACLs/namespaces** (separating raw ingestion from drift output), **Data Import Server** (centralized binary-log ingestion from the three OMS feeds), **Barrage** (mapping-table distribution to resolvers), and **partitioned on-disk history** (durable replay for shard rebuilds).
- Nothing in the design requires Enterprise-only storage features, so a Community prototype of the join-and-diff logic can be built first and lifted as-is.
