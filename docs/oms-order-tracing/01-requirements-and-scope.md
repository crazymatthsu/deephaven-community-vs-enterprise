# 01 — Requirements and scope

Restates what [`oms-order-tracing-architecture.md`](../oms-order-tracing-architecture.md)
asks for, separates requirement from proposed mechanism, pins down the assumptions the review
(doc 00) found implicit, and lists what must be answered before the design is frozen. Docs
02–10 cite requirements by the ids below.

**Topology as clarified on 2026-10-25:** OMS A holds the root order. OMS B holds one parent
order per A root and many child orders under it. OMS C holds one parent order per B child
(so C has as many parents as B has children) and many child orders under each. Five levels,
four edges.

---

## 1. Business goal and topology

Three order management systems form a chain: **A** is Raptor (the client-order system), **B** the
algo server, **C** the order router (Q7); each drop-copies its order states over several channels,
one Kafka topic per channel (Q6). The firm needs to know, continuously and after
the fact, whether the executed quantity each system believes in agrees along the chain, and
when it does not, **which system, and which book inside it, missed which shares**.

| Level | System | Parent level | Link the row carries | Expected cardinality | Role |
|---|---|---|---|---|---|
| `A_ROOT` | A | — | none | one per client order | the root; the only level that knows nothing downstream |
| `B_PARENT` | B | `A_ROOT` | the A order id (cross-system) | **1 : 1** with `A_ROOT` | B's view of the client order |
| `B_CHILD` | B | `B_PARENT` | the B parent's order id (in-system) | **1 : n** | B's slices (algo children) |
| `C_PARENT` | C | `B_CHILD` | the B child's order id (cross-system) | **1 : 1** with `B_CHILD` | C's view of each B slice |
| `C_CHILD` | C | `C_PARENT` | the C parent's order id (in-system) | **1 : n** | C's venue orders — the **street** level |

The four edges, named by their endpoints: `A-Bp` (cross-system), `Bp-Bc` (inside B),
`Bc-Cp` (cross-system), `Cp-Cc` (inside C). A gap on a cross-system edge means one system
missed what the other booked; a gap on an in-system edge means a system's own parent and child
books disagree.

C does not know A's id. A B child's root is its B parent's A reference; a C node's root is
reached through its B child.

## 2. Functional requirements

| Id | Requirement | Source | Notes |
|---|---|---|---|
| R1 | Ingest each system's order state feed into Deephaven durably: no acknowledged message is lost, and the full day can be replayed | §1, §11.1 | Kafka or Deephaven binary logs; doc 05 |
| R2 | Maintain the latest state per order per system, for parents and children alike | §6.2 | last-by on the system-local order id, then a role split |
| R3 | Link every node to its parent (four hops) and derive the A root of every node | §1, §5 | doc 04 §2 |
| R4 | Reconcile executed quantity **per edge**: a parent against the sum of its direct children, on all four edges | §1, §6.5, revised by doc 00 F1 | the per-row triple diff is wrong on the 1 : n edges |
| R5 | Attribute a gap: a parent showing more than its children = the children's book missed executions; children showing more than the parent = the parent's book has not booked them. On a cross-system edge that names a system; on an in-system edge it names the book (parent or child) inside the system | §1 | doc 03 §6 |
| R6 | Surface non-zero, **aged** gaps only; transient propagation lag is not drift | §8 (clock skew), doc 00 F12 | aging threshold is configuration |
| R7 | Roll up gaps by root A order (per edge, so the number names the system and book) and by symbol/side | §3.5 | doc 03 §4.5 |
| R8 | Handle out-of-sequence arrival at any hop: a child before its parent by seconds to hours is never dropped and never mis-attributed; it links when the parent arrives, and its root resolves when the chain above it is complete | §1, §9 | `UNRESOLVED` / `ROOT_UNRESOLVED` states (doc 04 §4) |
| R9 | Keep an **unresolved-links** table: child references that do not resolve, with first-seen time and age; alert when age or count exceeds thresholds | §3.5, §8, §11.4 | derived, not a separate store |
| R10 | Keep every raw message and every unresolved/aged break queryable after the fact | §3.5, §11.6 | raw intraday + historical partitions; persisted break events |
| R11 | Provide the outputs as live tables other PQs, the web UI and API clients can read, with ACLs separating raw from derived | §3.5, §10 | doc 03 §1, doc 07 §5 |
| R12 | Optional: a tree view of a lineage (A → Bp → Bc → Cp → Cc) for investigation, collapsed by default | §6.4 | one row per node (doc 04 §7) |
| R13 | Guard against mis-keyed feeds: validate id mapping on a sample and watch key cardinality (one B parent per A, one C parent per B child) before trusting diffs | §8 | doc 09 §6 |
| R14 | Survive restarts: every component is a persistent query that recovers from durable state without operator action | §10, §11.7 | doc 07, doc 08 |
| R15 | Name the **individual executions** that were dropped on a broken edge, not only the edge and the quantity. Execution ids do not survive a hop; **quantity, price and transaction time do** (Q17, answered 2026-10-26), so executions are matched on that key with multiset semantics | §1 ("identify exactly which executions were dropped") | a second data path over execution reports, computed **lazily for broken edges only** (doc 02 §10, doc 04 §10); phase P2b |
| R16 | Reconcile **open quantity** per edge as well: `RouteGap = LeavesQty − Σ direct children LeavesQty`, so a cancel that did not propagate, an over-routed slice or quantity held at a level is visible next to the executed-quantity gap | Q14 (answered 2026-10-26: `LeavesQty` is available in all three systems) | same rollup, one more sum (doc 03 §4.3, §6); doc 14's `RouteGap` semantics |
| R17 | A system or drop-copy channel that is **down or replaying a backlog** must not surface as thousands of drift breaks: gaps on either side of a stale or catching-up channel are suppressed as `FEED_STALE` (amber), one alert fires per channel, and the gaps resolve or become drift once the channel is current | Q7 (2026-10-26: an outage lags a system by a few hours) | per-topic feed health in the lineage DAG (doc 03 §4.4, doc 04 §5) |

## 3. Non-functional requirements

Values marked *proposed* are defaults to confirm with the business owner (§8).

| Id | Requirement | Target |
|---|---|---|
| N1 | Volume | **Confirmed (Q1, 2026-10-26): ~100 M orders per day in total across all five levels**, fewest in A, most in C (the order router). With a 2-day window and the daily restart that is ~150 M resident nodes; the design carries 2× as contingency (doc 06 §1.3) |
| N2 | Detection latency | a gap is visible within one update cycle after both sides' latest states are present; update-graph cycle ≤ 1 s p99 outside bursts, and **≤ 5 s during the close burst of N12** *(proposed; the lineage worker batches rather than drops — measured in the M1 burst replay, doc 09 §5)* |
| N3 | Aging threshold | a difference becomes drift when neither side has changed for 30 s *(proposed)*; unresolved link alert at 1 h (§11.4) |
| N4 | Retention | intraday: today plus the longest cross-system lag rounded up to whole days — **2 days** covers the "few hours" an outage can cost (Q7), overnight included; historical: 90 days *(proposed)* |
| N5 | Durability | no data loss after the ingester has checkpointed; recovery of any PQ rebuilds its state from DIS storage and Kafka offsets |
| N6 | Recovery time | lineage PQ cold start over the retained window ≤ 15 min at the N1 volume *(proposed; measured in doc 06 §8)* |
| N7 | Availability | ingestion PQs auto-restart; lineage PQ auto-restart with error-restart attempts; spares optional for the UI PQ |
| N8 | Security | raw tables readable by the tracing PQs and ops only; derived tables readable by the consuming desks; writes only from merge-dispatcher workers |
| N9 | Operability | every threshold and window is configuration; scripts are Git-sourced (sibling doc `git-persistent-query-scripts.md`) |
| N10 | Portability | the lineage/drift logic is Community-Core table operations and runs unchanged in a Core+ worker; only ingestion and persistence are Enterprise-specific |
| N12 | Peak ingest | ~10 k msg/s per topic, up to 8 topics per system (Q6): **~80 k msg/s per system, ~240 k msg/s fleet-wide** in the close burst, sustained for minutes. No message is dropped or delayed beyond Kafka; the lineage view may batch (longer update cycles) during the burst, see N2 |
| N11 | Execution volume | several execution reports per node per level (several hundred million rows per day at the N1 volume) are **stored on disk and matched only for parents with an aged gap**, so the execution path's heap is proportional to breaks, not to executions (doc 06 §1.5) |

## 4. Assumptions the design relies on

| Id | Assumption | If false |
|---|---|---|
| A1 | Within one system, `OrderId` is unique across parents and children for the retained window | add a disambiguating column to the key |
| A2 | Every message carries the order's **full current state**, in particular `CumQty` (a SOW/snapshot style feed), not a delta. *Confirmed 2026-10-26 (Q3).* `last_by` is therefore the complete state operator; executions are a separate feed (R15) | — (a per-system fold, the fix42 repo's `fix42cache`, would be needed before last-by) |
| A3 | `CumQty` is non-decreasing per order except for busts, which **reduce `CumQty` in all three systems** and are reported as a new full state. *Confirmed 2026-10-26 (Q8).* | — |
| A4 | Cardinality per §1: `A-Bp` 1 : 1, `Bp-Bc` 1 : n with **n up to a few hundred**, `Bc-Cp` 1 : 1, `Cp-Cc` 1 : n. *Confirmed 2026-10-26 (Q2); C's fan-out not yet quantified.* | the per-edge rule handles any cardinality; the 1 : 1 checks (`CARDINALITY`, `MISSING_CHILD`) are configuration |
| A5 | A node's parent reference (cross-system or in-system) is **stable** for the life of the order: the latest state message carries the parent's root id, which a cancel/replace does not change. *Confirmed 2026-10-26 (Q4).* The same stability is assumed of `OrderId` itself (A1) | index every id a parent was ever known by (doc 04 §8, as doc 14 §3 does) — configuration of the generic path, not a redesign |
| A6 | No level skips a hop (no C parent referencing an A order or a B parent directly). *Confirmed as the starting assumption 2026-10-26 (Q5).* | ordered candidate parents (doc 14 §2; the generic path of doc 04 §8) — a configuration change, not a redesign |
| A7 | Symbol and side use one coding across the three systems. *Confirmed 2026-10-26 (Q13).* | — (a mapping in the adapter view, if it ever changes) |
| A8 | The feeds are Kafka topics keyed by order id; message order within a key is preserved | last-by orders by `SeqNo`/`EventTs` instead of arrival |
| A9 | Order ids are **alphanumeric strings** in all three systems. *Confirmed 2026-10-26 (Q19).* The sizing's string column applies (doc 06 §1.3) | — |
| A10 | One trading day is the natural partition | multi-region sessions need a session-date column |
| A11 | B and C rows carry an **in-system parent id** column (`ParentOrderId`) that is **null on parent orders and non-null on child orders**; that column alone tells parents from children. *Confirmed 2026-10-26 (Q15).* | — |
| A12 | A B child and its C parent are 1 : 1 and the C parent carries the B child's id | if the C parent carries the B *parent's* id instead, the `Bc-Cp` edge becomes `Bp-Cp` (1 : n) and B children are reconciled only inside B |
| A13 | An execution reported on both sides of a hop has the **same quantity, price and transaction time** on both sides, at the configured precision (price to 1e-6, time to the millisecond); its ids differ. *From Q17, 2026-10-26.* | widen the time bucket, or add an as-of match for the residue (doc 04 §10) |
| A14 | Busts and corrections reference the execution they cancel or replace by **that system's** exec id (`ExecRefId`), and every system reports them | an unpropagated bust shows as an execution-level break, which is the intended reading; a feed without `ExecRefId` cannot net busts and the match keeps the busted fill on that side |
| A16 | An order's states are published on **one** topic of its system for its whole life (an order does not move between a system's topics). If it did, `last_by(OrderId)` over the merged topics still keeps the latest arrival, and `SeqNo`/`EventTs` ordering (A8) protects against a stale one | nothing changes in the DAG |
| A17 | Each system drop-copies over **several channels**; an outage of a system or of one channel stalls the states of the orders on it for up to a few hours, after which the backlog replays at burst rate (Q7). Normal cross-system propagation is seconds. | the 2-day window (N4) and Kafka retention must still cover the longest outage |
| A15 | Within one system, a parent order is published before its children on the **same topic** and **may be assumed to arrive first** (Q7, Q18). Because the topic is keyed by order id (Q11), Kafka preserves that order per partition only, so this is an assumption, not a guarantee; the design does not depend on it — a child that does arrive first is `UNRESOLVED` for the delay and heals — and the pre-cutover check reports the observed child-first share (expected ≈ 0, doc 09 §6) | nothing changes |

## 5. Explicitly out of scope

- Position or P&L: executed quantity only (`AvgPx` is carried as optional for a later notional view).
- Repairing the OMS feeds or re-sending messages.
- Reconciling order attributes beyond symbol/side (limit price, TIF, account).
- Real-time UI at 100 M rows: the UI shows filtered trees and aggregates, never the whole book.

## 6. Acceptance criteria

1. **AC1 (R4, R5):** A(1000) → Bp(1000) → Bc1(600), Bc2(400) → Cp1(600), Cp2(400) → Cc under
   each, all fills booked everywhere: every edge gap is 0, no drift row exists. Remove one
   100-share fill from Bc2's state (B's child book shows 300): `edge_bp_bc` shows Bp gap +100
   (`INTERNAL`, B child book short), `edge_bc_cp` shows Bc2 gap −100 (`UNBOOKED_UPSTREAM`, B
   short against C); `drift_by_a_order` shows `InternalB = +100`, `UnbookedAtB = 100`,
   `MissingAtC = 0`.
2. **AC2 (R8):** Ingest Cp1 at T, its Bc1 at T + 2 h. Between T and T + 2 h, Cp1 is
   `UNRESOLVED` and its Cc children are `LINKED` with `RootState = UNRESOLVED`; none appears in
   a drift table. At T + 2 h all of them resolve within one update cycle.
3. **AC3 (R6):** A fill reported by Cc at T, by Cp at T + 3 s, by Bc at T + 6 s, by Bp and A
   within 30 s produces no drift row; stopping the propagation at any hop produces exactly one
   drift row on that edge.
4. **AC4 (R9, R10):** A Bc that never arrives leaves its Cp `UNRESOLVED` past the alert
   threshold; the alert fires once per crossing; the rows remain queryable the next day.
5. **AC5 (R14, N5, N6):** SIGKILL the lineage PQ mid-day; after auto-restart its tables equal
   the pre-kill tables row for row, within N6.
6. **AC6 (N5):** SIGKILL an ingester mid-topic; after restart no message is duplicated or
   missing in `OMS_RAW`.
7. **AC7 (R13):** The pre-cutover validation reports, per hop, the fraction of references that
   resolve and the cardinality (B parents per A, C parents per B child both ≈ 1; children per
   parent within the expected band); cutover requires ≥ 99.9 % resolution on the sample.
8. **AC8 (N1, N2):** At the agreed volume the update-graph cycle stays ≤ 1 s p99 and heap
   ≤ 70 % of allocation for a full day.
9. **AC9 (R15):** In AC1's broken book (Bc2's child book short by one 100-share fill), once the
   gap is aged, `exec_breaks` names the dropped execution on the `Bp-Bc` edge: the B parent's
   exec id for the `(100, px, time)` key, `Kind = EXEC_MISSING_DOWNSTREAM`, `DroppedQty = 100`,
   and `exec_breaks_by_edge.Unexplained = 0`. With two identical 100-share fills at the same
   price and millisecond and one dropped, the row names both candidate exec ids with
   `Ambiguous = true`.
10. **AC10 (R16):** Cancel the A order and its B parent (both `LeavesQty = 0`) while one B child
    still shows 100 open: once aged, `edge_bp_bc` shows `RouteGap = −100`,
    `BreakKind = CHILD_STILL_OPEN` on the B parent; `drift_by_a_order.OverRoutedQty = 100`. A B
    parent holding 200 unsliced shares shows `HELD` (amber) and no drift row.

## 7. Glossary

| Term | Meaning here |
|---|---|
| Node | one order in one system at one level |
| Edge | the parent → children relation at one hop; **cross-system** (`A-Bp`, `Bc-Cp`) or **in-system** (`Bp-Bc`, `Cp-Cc`) |
| Gap (`FillGap`) | parent `CumQty` − Σ direct children `CumQty`; positive = the children's book is short, negative = the parent's book is short |
| Aged gap / drift | a gap whose both sides have been unchanged for ≥ the aging threshold |
| Unresolved | a child whose parent reference matches no parent yet |
| Root-unresolved | a node linked to its parent whose chain to A is broken higher up |
| Root | the A order a node descends from |
| Street | `C_CHILD`: the level whose quantity is the market-facing truth |
| Feed stale / catching up | a drop-copy channel (topic) that has been silent longer than `FEED_STALE_SEC`, or whose newest event time is more than `FEED_CATCHUP_SEC` behind now while a backlog replays; gaps touching it are suppressed |
| DIS, Core+, PQ | as in doc 02 §8 |

## 8. Open questions

| # | Question | Why it matters | Default assumed | Owner |
|---|---|---|---|---|
| Q1 | Is 100 M per day, per level, or total ever? **Answered 2026-10-26: ~100 M per day in total across all systems; A has the fewest orders, C (the order router) the most** | N1 confirmed at the expected case; one lineage worker is the design, edge families the contingency (doc 06 §1.3) | as answered | — |
| Q2 | Children per parent at `Bp-Bc` and `Cp-Cc`. **Answered 2026-10-26: a B parent has up to a few hundred children; 1 : 1 at the cross-system hops, 1 : n inside B and C.** C's children per parent still to be read off the data | rollups of a few hundred rows per parent are cheap; the UI slice caps the roots it expands (doc 04 §7); `large_fanout` tests 300 children (doc 09 §2) | as answered; C fan-out measured in M1 | — |
| Q3 | Full state per message including `CumQty`, or deltas? **Answered 2026-10-26: full state per message, including `CumQty`** | A2 confirmed; no fold stage; the execution path (R15) stays a separate feed | as answered | — |
| Q4 | Can a parent reference change on cancel/replace? **Answered 2026-10-26: no** — the latest order state carries a root parent id that a replace or cancel does not change | A5 confirmed; the native-id joins of doc 04 §2 need no id-history index; the pre-cutover check still reports references that fail to resolve (AC7) | as answered | — |
| Q5 | Can any level skip a hop? **Answered 2026-10-26: assume no skipping to start with** | A6; the native-id DAG links each level to exactly one parent level. If a skip appears later, `C_PARENT` gets `A` as a second candidate parent system in the generic path (doc 04 §8) and the production link gains one more `natural_join` | as answered | — |
| Q6 | Peak and average message rates per system. **Answered 2026-10-26:** a topic peaks at ~10 k msg/s, and a system publishes its latest order states on **several topics** (OMS B: 7–8), so a system bursts at **~80 k msg/s near the close**; ~240 k msg/s fleet-wide if all three burst together | N12; ingestion is one ingester per topic (doc 05 §4.6); the lineage update graph is sized and measured at the burst (doc 06 §2, doc 09 §5) | as answered; average rates still to be measured | — |
| Q7 | Lag distribution child-before-parent at each hop. **Answered 2026-10-26.** In-system: within B and within C the parent arrives before its children (A15). Cross-system: normally seconds; when a system or one of its drop-copy channels goes down, **a few hours**, then a backlog replay | N4 stays at 2 days; the backlog replay is the Q6 burst again (doc 06 §2); gaps during an outage are `FEED_STALE`, not drift (R17) | as answered | — |
| Q8 | Do busts reduce `CumQty` in all systems, reported as a new state? **Answered 2026-10-26: yes, in all systems** | A3 confirmed; a bust propagates as a transient negative move down the chain and reconciles once every system reports it; a bust reported in one system only is a real gap (`MISSING_DOWNSTREAM` or `UNBOOKED_UPSTREAM`) and, at execution level, an unmatched fill on the other side. Because a bust can land on an order that already looked closed, the closed-order pruning lever keeps terminal orders for a grace period (doc 06 §1.4) | as answered | — |
| Q9 | Who consumes alerts and how? **Answered 2026-10-26: a table plus one HTTP sink** | every threshold crossing is a row in the `alerts` table (doc 03 §4.5, persisted through `DriftEvents`) and one HTTP POST to `OMS_TRACE_ALERT_URL` (doc 08 §2) | as answered | — |
| Q10 | Enterprise and Core+ versions in prod. **Answered 2026-10-26: Deephaven Enterprise 2026.01.054** — a build of the **1.202601** release line, whose Core+ workers and clients are versioned `2026.01.0xx` and bundle **Community Core 42.5** (the release ships a `frozen-requirements-2026.01.0xx-42.5.txt` for worker Python environments) | every API name in docs 04–07 is confirmed against that release's documentation and Core+ Python package in M2's first task; `scripts/check_env.sh` asserts the version | as answered | — |
| Q11 | Are the topics keyed by order id? **Answered 2026-10-26: by order id, not by parent id** | parent-before-child (Q18) holds within a partition only, so in-system child-first arrivals are normal transients (A15); lineage sharding is unaffected (it filters by hash in the worker, doc 06 §4.3) | as answered | — |
| Q12 | Clock source per OMS; is `EventTs` comparable? **Answered 2026-10-26: event time (FIX tag 60, `TransactTime`) is GMT in all three systems and comparable.** Why it was asked: aging declares a gap drift only when *neither side has changed for N seconds*, which needs one clock across systems; with skewed local clocks a 5 s propagation could read as an hour, or the reverse. Receive time was the safe default; comparable event time makes it a choice (`OMS_TRACE_AGE_CLOCK`, doc 03 §7) and is what the execution match key needs (Q17) | both clocks usable; default stays `RecvTs` because a republished old event would age instantly under event time | as answered | — |
| Q13 | Symbol/side coding per system. **Answered 2026-10-26: identical across all three** | A7 confirmed; `MISMATCH` compares values as sent | as answered | — |
| Q14 | Is `LeavesQty` available in all three? **Answered 2026-10-26: yes** | R16: open quantity reconciled per edge (`RouteGap`, `HELD`, `OVER_ROUTED`, `CHILD_STILL_OPEN`, `LEAVES_MISMATCH`) | as answered | — |
| Q15 | In B and C, which field carries the in-system parent id, and how are parents told from children? **Answered 2026-10-26:** `ParentOrderId`, null on parents, non-null on children | A11; the adapter role split | as answered | — |
| Q16 | Do B and C children also carry the cross-system reference? **Answered 2026-10-26: no** — children carry `ParentOrderId` only, `UpstreamRef` is null on them | children link and route by `ParentOrderId` alone (already the design); no `ROOT_MISMATCH` cross-check is available; `UpstreamRef` non-null on a child is a contract violation the pre-cutover check reports | as answered | — |
| Q17 | Is execution-level identification required, and how do executions match across hops? **Answered 2026-10-26:** execution ids are **not** shared across hops; **quantity, price and transaction time are the same** on both sides | R15 in scope (phase P2b); multiset match on `(Qty, Px, TransactTime)` per broken edge (doc 04 §10); precision and jitter measured pre-cutover (doc 09 §6) | as answered | — |
| Q19 | Are order ids numeric or strings? **Answered 2026-10-26: alphanumeric.** | A9 confirmed; the lineage worker is sized on the string column (~54 GB resident at the confirmed volume → one 96–128 GB worker); a numeric surrogate scheme is not a lever because the references would need the same mapping at ingest (doc 06 §1.4), so the contingency for doubled volume is the edge-family sharding | as answered | — |
| Q18 | Are B's (and C's) parent and child orders on one topic or two? **Answered 2026-10-26: one topic per system, so the parent is received before its children** | one ingester and one table per system; A15 on what the ordering does and does not guarantee | as answered | — |
