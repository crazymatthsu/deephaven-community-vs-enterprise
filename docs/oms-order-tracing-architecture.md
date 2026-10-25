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
- **Out-of-sequence risk:** C messages can arrive before their B parent, with delays ranging from seconds to hours. The design must not drop or silently mis-attribute these.

---

## 2. High-Level Topology

```
┌─────────────┐   binary log / Kafka    ┌──────────────────┐
│  OMS A feed │ ───────────────────────►│  DIS-A (tailer)  │
└─────────────┘                         └────────┬─────────┘
                                                 │
┌─────────────┐   binary log / Kafka    ┌───────►──────────┐
│  OMS B feed │ ───────────────────────►│  DIS-B (tailer)  │
└─────────────┘                         └───────┬──────────┘
                                                 │
┌─────────────┐   binary log / Kafka    ┌───────►──────────┐
│  OMS C feed │ ───────────────────────►│  DIS-C (tailer)  │
└─────────────┘                         └───────┬──────────┘
                                                 │
                                    ┌────────────►────────────┐
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
                      └────────────┬────────────┼──────────────────────────┘
                                   ▼
                      ┌────────────────────────────┐
                      │ Drift / dropped-shares     │
                      │ output tables + alerts     │
                      └────────────────────────────┘
```

### Component roles

| Component | Role | Heap profile |
|---|---|---|
| **DIS-A / DIS-B / DIS-C** | Dumb ingestion. Receive OMS feeds (binary logs or Kafka), validate schema, write to per-system binary logs **and** intraday partitions on shared storage. No joins, no routing logic. | Small, stateless |
| **Resolver Worker** | Consumes raw A/B/C streams. Runs the A→B→C join chain. Resolves C's root A ID via the mapping table. Hashes the root ID to assign a shard. Publishes resolved rows to shard-specific topics. Holds unresolved C rows in the **pending buffer** (see §12). | Medium-large |
| **Shard Workers (0..N)** | Each owns one shard of the order universe. Runs last-by, intra-shard joins, tree-table construction, cum-share diff, and drift detection. | Per-shard working set only |
| **Mapping Table Publisher** | Single Core+ persistent query. Maintains the flat `downstream_order_id → a_root_id` table. Publishes over Barrage to all resolver/ingress subscribers. | ~2–4 GB at 100M pairs |
| **Pending Buffer PQ** | Manages the durable pending-C store in a user namespace with intraday partitioning. Retries resolution on a schedule. | Near-zero heap; storage-backed |
| **Reconciliation PQ** | Scheduled query that scans aged pending rows, retries joins, promotes resolved rows, alerts on stale unresolved ones. | Scheduled, short-lived |

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
| `ts` | Instant | OMS event timestamp |
| `received_ts` | Instant | Wall-clock time DIS received the message |

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

### 3.4 Pending buffer table (durable, namespace-backed)

| Column | Type | Notes |
|---|---|---|
| `c_order_id` | long / string | PK candidate |
| `b_ref_id` | long / string | The B ID C pointed at — the join key for flush |
| `c_cum`, `symbol`, `side`, `qty` | as received | Frozen snapshot of the C message |
| `received_ts` | Instant | When DIS saw it |
| `pending_since` | Instant | When it first entered the buffer |
| `retry_count` | int | How many reconciliation passes touched it |
| `status` | string | `pending` / `resolved` / `unresolved` |

- Partitioned intraday by `received_ts` (hourly or 15-minute partitions), merged to historical on a schedule.
- Keyed on `b_ref_id` for fast flush lookup when B arrives.
- This is a **PQ-managed table in a user namespace**, not a heap map. It can hold the full pending population without memory pressure.

### 3.5 Drift output tables

- `drift_by_order`: per-lineage-row diffs (filter where any drift ≠ 0).
- `drift_by_symbol`: rolled up by symbol — "system B missed X shares from A today."
- `drift_by_a_order`: rolled up by root A order — the number system A needs to see.
- Unresolved-link table: C/B references that failed mapping lookup or exhausted retry budget, with counts and last-seen timestamps.

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

1. DIS-A/B/C land raw messages (stateless) — binary log + intraday partition.
2. Resolver worker consumes all three streams.
3. A rows: root = own order ID → route to shard(hash(a_id)).
4. B rows: join B.upstream_ref_id → A to get root → route to shard(hash(root)). **Also:** flush any pending C rows keyed on this B ID.
5. C rows: left-join C.order_id → mapping table for root. **Hit:** route to shard(hash(root)). **Miss:** append to the pending buffer table (durable) and do not block.
6. Shard worker receives only its shard's resolved rows and runs the per-shard pipeline.
7. Reconciliation PQ periodically retries pending rows; resolved ones flow into step 5.

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
3. **Join chain**: A ⇌ B ⇌ C inside the shard (aj / exact join on the reference columns). All joins are intra-shard — no network hops.
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
| Pending buffer | **~0 GB heap** | Storage-backed PQ table; size = pending population × row width |
| Tree table (UI layer) | negligible server-side | viewporting pushes only visible rows to browser |

Enterprise levers used: large heap workers, direct memory (2× heap for the data buffer pool), Barrage chunked snapshots, formula columns via updateView for derived fields, and on-disk partitioned historical tables for anything that must survive beyond RAM.

---

## 8. Failure Modes & Guards

- **Unmapped C reference** (B ID not in mapping table): not silently dropped. Counted in the unresolved-link table with last-seen timestamp. A sustained rise indicates a B-side ingestion gap.
- **Re-keyed or batched downstream messages**: join keys stop lining up 1:1 → false drift. Validate ID mapping on a small sample before trusting diffs at scale; add a sanity check that B.order_id cardinality ≈ expected.
- **Shard worker death**: shard is rebuilt from the binary log / Kafka topic (durable), so no data loss — just replay lag.
- **Mapping table publisher death**: resolvers buffer C messages briefly; on republish, buffered messages resolve and flush. Alert if buffer depth exceeds a threshold.
- **Clock skew between OMS systems**: diffs on cum_shares are point-in-time; pair each cum value with its timestamp and diff only comparable snapshots, or diff on the latest-seen value per system with an explicit as-of note.
- **C arrives before B (out-of-sequence)**: handled by the pending buffer (§12). No data loss; resolution is deferred, not discarded.

---

## 9. Open Questions / Next Steps

- [x] Confirm whether C messages can arrive before their B parent has been ingested (ordering guarantees across the three feeds). **Answer: yes, delays can be large.** → Pending buffer design in §12.
- [ ] Decide shard count from measured peak ingress rate and per-shard CPU.
- [ ] Prototype the resolver join chain on a 1M-row sample to validate join key cardinality before production cutover.
- [ ] Define alert thresholds on drift_by_symbol, unresolved-link counts, and pending-buffer depth/age.
- [ ] Decide retention: how long raw per-OMS history is kept on disk vs. in-memory last-by state.
- [ ] Validate binary-log and intraday-partition retention windows against worst-case B lag.

---

## 10. Why This Splits Cleanly Across Community → Enterprise

- All table operations (last-by, aj, tree, updateView, where) are **Community Core** semantics — the code ports unchanged into Enterprise Core+ workers.
- What Enterprise adds that this design depends on: **Persistent Queries** (the mapping-table publisher, shard workers, pending-buffer manager, and reconciliation jobs survive sessions), **ACLs/namespaces** (separating raw ingestion from drift output and pending state), **Data Import Server** (centralized binary-log ingestion from the three OMS feeds), **Barrage** (mapping-table distribution to resolvers), and **partitioned on-disk history** (durable replay for shard rebuilds and the pending buffer).
- Nothing in the design requires Enterprise-only storage features, so a Community prototype of the join-and-diff logic can be built first and lifted as-is.

---

## 11. Pending Buffer Persistence Design

### 11.1 The principle: the log and the partitions are the store; memory is the cache

The in-memory pending map is explicitly **not** durable. Three layers back the buffer:

1. **Binary log / Kafka topic** (written by DIS at ingest) — the source of truth for every C message, regardless of resolution status.
2. **Intraday partitions on shared storage** — DIS also lands each message into a partitioned intraday table (splayed or partitioned layout, Parquet or DeephavenV1 merge format) keyed by `received_ts`. If B lag exceeds binary-log retention, the partition still holds the row.
3. **Merged historical partitions** — a scheduled merge job rolls intraday into historical on a chosen cadence, extending retention to days or weeks.

 consequence: a C row pending for six hours is still queryable. Nothing is lost unless storage itself is lost.

### 11.2 Is the persisted data part of the DIS?

No. The DIS is a **writer only** — a lightweight, stateless tailer with a small heap. It receives, validates, and appends. It does not index, does not join, does not serve queries. The persisted data lives in the **Enterprise storage layer** (shared filesystem or object storage mounted by the cluster), organized as system/user namespaces with intraday and historical partitions. The DIS's job ends at the write.

### 11.3 Do you need a Persistent Query to read it?

Yes. In Enterprise, **persistent queries (PQs)** are the mechanism that reads persisted data. A PQ is a long-running query definition that:

- Mounts the relevant namespaces (system for intraday/historical, user for application tables).
- Can reference both live ticking tables and on-disk partitions in the same query — from the PQ's perspective they are just tables.
- Survives the session that created it; Enterprise restarts and reschedules it.
- Can be triggered on a schedule (cron-style) for batch-style work like reconciliation.

So the pending buffer is not "a file you open" — it is a **PQ-managed table**. You create it once as a PQ in a user namespace, and thereafter any other PQ, the web UI, or a script can query it with ordinary Deephaven syntax (`where`, `join`, `last_by`, etc.). The storage engine handles paging from disk; you never manage file paths.

### 11.4 Concrete layout

**Pending buffer table** (user namespace, e.g. `oms.pending_c`):

- Partitioned intraday by `received_ts` — one partition per 15 minutes or hour.
- Keyed on `b_ref_id` (the column used to flush when B arrives).
- Merge job (another scheduled PQ) rolls intraday → historical daily.
- Retention: match or exceed worst-case observed B lag; start at 7 days and tune.

**Flush path** (hot, in the resolver):

- B message arrives → resolver joins B.order_id against the pending table on `b_ref_id`.
- Matches flush into the shard pipeline immediately.
- The pending row's status flips to `resolved` (or the row is removed, depending on whether you want an audit trail — recommend keeping it with status for forensics).

**Reconciliation path** (cold, scheduled PQ every 5 minutes):

- Scan pending partitions for rows with `status = pending` and `pending_since` older than a freshness threshold.
- Retry the B and mapping joins.
- Resolved → publish to shard topics, mark resolved.
- Still unresolved past an age threshold (e.g. 1 hour) → move to an `unresolved` partition, increment alert counter, page on threshold breach.
- This decouples the alert TTL from data retention: alerting is aggressive, storage is patient.

### 11.5 Why this avoids memory issues at 100M scale

- The pending table's heap footprint is proportional to the **PQ's working set** (rows touched by the current query), not the total pending population. Deephaven's columnar engine reads partitions on demand.
- Even if 10% of 100M C messages are pending simultaneously (10M rows), at ~200 bytes per row that is ~2 GB of storage — trivial on shared storage, invisible to worker heaps.
- The resolver worker only ever holds the in-flight C messages in its small receive buffer; everything older than that tick is already in the namespace.
- Shard workers similarly stay lean: they consume resolved rows only, and their last-by state is bounded by active orders per shard.

### 11.6 Querying the persisted data

From the Deephaven web UI or any authenticated client:

```groovy
// See everything currently pending, by age
p = db.t("oms", "pending_c").where("status = `pending`")
p.updateView("age_sec = (now() - pending_since) / SECOND").sortDescending("age_sec")

// Find pending rows for a specific B reference
p.where("b_ref_id = 12345")

// Reconciliation-style scan: aged pending rows
p.where("pending_since < now() - 'PT1H'")
```

No special API, no file-system access, no out-of-band tooling. The same table syntax works on in-memory and persisted tables because Enterprise unifies them under the namespace.

### 11.7 Failure semantics

| Failure | What happens to pending C rows |
|---|---|
| Resolver worker dies | In-flight buffer lost, but binary log + intraday partitions survive. Restarting the resolver PQ replays from the log and re-buffers. |
| Pending-buffer PQ dies | Table is durable on shared storage; PQ restart remounts it. No data loss. |
| Reconciliation PQ dies | Pending rows simply wait longer; next scheduled run catches up. Alert on missed schedule. |
| Shared storage loss | Data loss — mitigated by replication of the storage layer (standard Enterprise deployment practice). |
| B never arrives | Row ages past threshold → moved to unresolved partition + alert. Still queryable for forensics; just no longer auto-retried. |
