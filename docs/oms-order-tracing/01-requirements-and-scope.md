# 01 — Requirements and scope

Restates what [`oms-order-tracing-architecture.md`](../oms-order-tracing-architecture.md)
asks for, separates requirement from proposed mechanism, pins down the assumptions the review
(doc 00) found implicit, and lists what must be answered before the design is frozen. Docs
02–10 cite requirements by the ids below.

---

## 1. Business goal

Three order management systems form a chain: **A** (upstream, the client-facing order) →
**B** (mid) → **C** (downstream, the street-facing order). Every B order carries the id of the
A order it was created from; every C order carries the id of its B order; C does not know A's
id. The firm needs to know, continuously and after the fact, whether the executed quantity
each system believes in agrees along the chain, and when it does not, **which system missed
which shares**.

## 2. Functional requirements

| Id | Requirement | Source | Notes |
|---|---|---|---|
| R1 | Ingest each system's order state feed into Deephaven durably: no acknowledged message is lost, and the full day can be replayed | §1, §11.1 | Kafka or Deephaven binary logs; doc 05 |
| R2 | Maintain the latest state per order per system | §6.2 | last-by on the system-local order id |
| R3 | Link every B order to its A parent and every C order to its B parent, and derive the A root of every C order | §1, §5 | doc 04 §2 |
| R4 | Reconcile executed quantity **per edge**: a parent against the sum of its direct children (A vs its B children; B vs its C children) | §1, §6.5, revised by doc 00 F1 | the per-row triple diff is a special case (fan-out 1) |
| R5 | Attribute a gap to a system: a parent showing more than its children = the downstream system missed executions; children showing more than the parent = the upstream system has not booked them | §1 | doc 03 §6 taxonomy |
| R6 | Surface non-zero, **aged** gaps only; transient propagation lag is not drift | §8 (clock skew), doc 00 F12 | aging threshold is configuration |
| R7 | Roll up gaps by root A order and by symbol (and side), i.e. "system B missed X shares of A's orders in AAPL today" | §3.5 | per edge, so the number names the system |
| R8 | Handle out-of-sequence arrival: a C before its B (or a B before its A) by seconds to hours is never dropped and never mis-attributed; it links when the parent arrives | §1, §9 | `UNRESOLVED` state, heals incrementally (doc 04 §4) |
| R9 | Keep an **unresolved-links** table: child references that do not resolve, with first-seen time and age; alert when age or count exceeds thresholds | §3.5, §8, §11.4 | derived, not a separate store |
| R10 | Keep every raw message and every unresolved/aged break queryable after the fact (same day and historical) | §3.5, §11.6 | raw intraday + historical partitions; persisted break events (doc 05 §8) |
| R11 | Provide the drift outputs as live tables that other PQs, the web UI and API clients can read, with ACLs separating raw from derived | §3.5, §10 | namespaces (doc 03 §1, doc 07 §5) |
| R12 | Optional: a tree view of a lineage (A → B → C) for investigation, collapsed by default | §6.4 | one row per node (doc 04 §7) |
| R13 | Guard against mis-keyed feeds: validate id mapping on a sample and watch key cardinality before trusting diffs | §8 | doc 09 §6 |
| R14 | Survive restarts: every component is a persistent query that restarts and recovers from durable state without operator action | §10, §11.7 | doc 07, doc 08 |

## 3. Non-functional requirements

Values marked *proposed* are defaults to confirm with the business owner (doc 01 §8).

| Id | Requirement | Target |
|---|---|---|
| N1 | Volume | "~100 M unique orders flowing through the chain" (§1). **Unit unknown** — see Q1. Design for 100 M resident order states per system per retained window as the upper case, 35 M each as the expected case |
| N2 | Detection latency | A gap is visible within one update cycle after both sides' latest states are present; target update-graph cycle ≤ 1 s p99 under peak ingest *(proposed)* |
| N3 | Aging threshold | A difference becomes drift when neither side has changed for 30 s *(proposed)*; unresolved link alert at 1 h (§11.4) |
| N4 | Retention | Intraday: today plus the longest observed B-after-C lag rounded up to whole days, minimum 2 days *(proposed)*; historical: 90 days *(proposed)*; break events: same as historical |
| N5 | Durability | No data loss after the ingester has checkpointed; recovery of any PQ rebuilds its state from DIS storage and Kafka offsets (doc 05 §5) |
| N6 | Recovery time | Lineage PQ cold start over the retained window ≤ 15 min at N1 upper case *(proposed; to be measured, doc 06 §8)* |
| N7 | Availability | Ingestion PQs auto-restart; lineage PQ auto-restart with error-restart attempts; spares optional for the UI PQ (doc 07 §2) |
| N8 | Security | Raw tables readable by the tracing PQs and ops only; derived tables readable by the consuming desks; writes only from merge-dispatcher workers (doc 07 §5) |
| N9 | Operability | Every threshold and window is configuration, not code; scripts are Git-sourced (sibling doc `git-persistent-query-scripts.md`) |
| N10 | Portability | The lineage/drift logic is Community-Core table operations and runs unchanged in a Core+ worker; only ingestion and persistence are Enterprise-specific (§10) |

## 4. Assumptions the design relies on

Each one is checked by a test or a pre-cutover validation (doc 09). If an assumption is false
the design changes in the way noted.

| Id | Assumption | If false |
|---|---|---|
| A1 | Within one system, `OrderId` is unique for the retained window (a system never reuses an id within a day) | add a disambiguating column (account, session) to the key |
| A2 | Every message carries the order's **full current state**, in particular `CumQty` (a SOW/snapshot style feed), not a delta | a per-system fold (state machine) is needed before last-by; the fix42 repo's `fix42cache` is that fold |
| A3 | `CumQty` is non-decreasing per order except for trade busts/corrections, which are reported as a new full state | nothing changes in the DAG; busts show as a transient negative move and reconcile when all systems report it |
| A4 | Fan-out is one parent → many children at each hop (1:1 is a special case) | nothing changes (R4 covers both) |
| A5 | A child's upstream reference is **stable** for the life of the order (a cancel/replace at B does not change the id C stores) | index every id a parent was ever known by (doc 04 §2 "extra ids", as doc 14 §3 does for FIX 41/11 rotation) |
| A6 | C never references A directly (no DMA-style skip) | add A as a second candidate parent system for C (doc 14 §2's ordered candidates) |
| A7 | Symbol and side use one coding across the three systems, or a mapping table exists | normalize in the adapter view (doc 03 §3) |
| A8 | The feeds are Kafka topics the firm can key (or re-key) by order id; message order within a key is preserved | if ordering within a key is not preserved, last-by must use a sequence/timestamp column instead of arrival order (`sequence` in the adapter) |
| A9 | Order ids are strings; if all three systems use numeric ids, longs are used | sizing changes by ~5× (doc 06 §1) |
| A10 | One trading day is the natural partition; the system's clock source for `Date` is the receiving worker's | multi-region sessions need a session-date column from the feed |

## 5. Explicitly out of scope

- Position or P&L: executed quantity only, no prices (the raw contract carries `AvgPx` as optional for a later notional view).
- Repairing the OMS feeds or re-sending messages. The system reports and attributes; humans or upstream systems fix.
- Cross-system *order-attribute* reconciliation beyond symbol/side mismatch (limit price, TIF, account) — a later extension of the same per-node table.
- Real-time UI at 100 M rows: the UI shows filtered trees and aggregates (R12), never the whole book.

## 6. Acceptance criteria

Testable statements; the scenario catalog in doc 09 §2 maps to them.

1. **AC1 (R4, R5):** On a chain A(1000) → B1(600), B2(400) → C1(600), C2(400) with all fills booked, every edge gap is 0 and no drift row exists. Remove one 100-share fill from B2's state: `edge_ab` shows A gap +100 attributed to B; `drift_by_a_order` shows `MissingAtB = 100`, `MissingAtC = 0`.
2. **AC2 (R8):** Ingest C1 at T, its B1 at T + 2 h. Between T and T + 2 h, C1 is `UNRESOLVED` in `unresolved_links` with a growing age and appears in no drift table. At T + 2 h it becomes `LINKED` within one update cycle and its edge reconciles; no row was re-sent or re-read.
3. **AC3 (R6):** A fill reported by C at T and by B at T + 5 s produces no drift row when the aging threshold is 30 s; it produces one if B never reports it.
4. **AC4 (R9, R10):** A B that never arrives leaves its C rows `UNRESOLVED` past the alert threshold; the alert fires once per threshold crossing; the rows remain queryable from the raw and the derived tables the next day.
5. **AC5 (R14, N5, N6):** Kill the lineage PQ with SIGKILL mid-day; after auto-restart its tables equal the pre-kill tables row for row (compared against the Python reference), within N6.
6. **AC6 (N5):** Kill an ingester PQ mid-topic; after restart no message is duplicated or missing in `OMS_RAW` (checked by offset ranges and row counts).
7. **AC7 (R13):** The pre-cutover validation reports, per system, the fraction of child references that resolve and the key cardinality ratio; cutover requires ≥ 99.9 % resolution on the sample and a ratio within the expected fan-out band.
8. **AC8 (N1, N2):** At the agreed volume (Q1) the update-graph cycle stays ≤ 1 s p99 and heap stays ≤ 70 % of the PQ allocation for a full day.

## 7. Glossary

| Term | Meaning here |
|---|---|
| Node | one order in one system (A, B or C) |
| Edge | the parent→children relation at one hop (A–B or B–C) |
| Gap (`FillGap`) | parent `CumQty` − Σ direct children `CumQty`; positive = downstream missed shares, negative = upstream has not booked shares |
| Aged gap / drift | a gap whose both sides have been unchanged for ≥ the aging threshold |
| Unresolved | a child whose upstream reference matches no parent yet |
| Root | the A order a node descends from |
| DIS | Data Import Server, the Enterprise service that writes and serves intraday table partitions |
| Core+ | Deephaven Enterprise workers running the Community Core engine with Enterprise integrations |
| PQ | Persistent Query, an Enterprise-managed long-running worker with a script, schedule, heap and ACLs |

## 8. Open questions

Ordered by how much they change the design. "Default" is what docs 02–10 assume until answered.

| # | Question | Why it matters | Default assumed | Owner |
|---|---|---|---|---|
| Q1 | Is 100 M orders per day, per system, or total ever? | sets retention, resident state, whether a single worker suffices | 100 M per day across the chain (~35 M per system), 2-day window | OMS data owners |
| Q2 | Fan-out at A→B and B→C (max and typical children per parent) | confirms R4; sets the UI tree depth and the rollup cost | 1:1–1:20 | OMS B / OMS C teams |
| Q3 | Does every message carry full state including `CumQty`, or deltas/executions? | A2; a delta feed needs a fold before last-by | full state | feed owners |
| Q4 | Can C's reference to B (or B's to A) change on cancel/replace? | A5; id history index | stable | OMS B / C |
| Q5 | Can C reference A directly (DMA skip)? | A6; second candidate parent | no | OMS C |
| Q6 | Peak and average message rates per system | sizing of ingesters and the update graph | 50 k msg/s peak total | feed owners |
| Q7 | Lag distribution of B-after-C and A-after-B (p50/p99/max) | N3, N4, the date window | p99 1 h, max 1 day | measured in M1 (doc 10) |
| Q8 | Do busts reduce `CumQty` in all three systems, and are they reported as a new state? | A3 | yes | OMS teams |
| Q9 | Who consumes alerts and how (pager, chat, email, a desk blotter)? | alert sink and dedupe rules | a table plus one HTTP sink | ops |
| Q10 | Enterprise version and Core+ Python version in prod | API names in docs 04–07 | current Core+ with Python workers | Deephaven admins |
| Q11 | Are the Kafka topics keyed by order id? Can the producer key by parent id if sharding is needed? | doc 06 §4 routing option | keyed by order id | feed owners |
| Q12 | Clock source and timezone per OMS; is `EventTs` comparable across systems? | aging uses `RecvTs` by default for this reason | use receive time | OMS teams |
| Q13 | Symbol/side coding per system | A7 | identical | OMS teams |
| Q14 | Is `LeavesQty` (open quantity) available in all three? | enables an open-exposure view (doc 14's `RouteGap`) at no extra cost | optional | feed owners |
