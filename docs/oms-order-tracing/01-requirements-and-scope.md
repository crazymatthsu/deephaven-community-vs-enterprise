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

Three order management systems form a chain. The firm needs to know, continuously and after
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
| R15 | *Stretch.* Name the **individual executions** that were dropped, not only the edge and quantity | §1 ("identify exactly which executions were dropped") | not derivable from cumulative-quantity states; needs execution reports on both sides of an edge (Q17); see doc 02 §10 |

## 3. Non-functional requirements

Values marked *proposed* are defaults to confirm with the business owner (§8).

| Id | Requirement | Target |
|---|---|---|
| N1 | Volume | "~100 M unique orders flowing through the chain" (§1), now read as **nodes across the five levels**. **Unit unknown** — Q1. Design for 100 M resident nodes per retained window as the upper case, ~35 M as the expected case |
| N2 | Detection latency | a gap is visible within one update cycle after both sides' latest states are present; update-graph cycle ≤ 1 s p99 under peak ingest *(proposed)* |
| N3 | Aging threshold | a difference becomes drift when neither side has changed for 30 s *(proposed)*; unresolved link alert at 1 h (§11.4) |
| N4 | Retention | intraday: today plus the longest observed child-before-parent lag rounded up to whole days, minimum 2 days *(proposed)*; historical: 90 days *(proposed)* |
| N5 | Durability | no data loss after the ingester has checkpointed; recovery of any PQ rebuilds its state from DIS storage and Kafka offsets |
| N6 | Recovery time | lineage PQ cold start over the retained window ≤ 15 min at the N1 upper case *(proposed; measured in doc 06 §8)* |
| N7 | Availability | ingestion PQs auto-restart; lineage PQ auto-restart with error-restart attempts; spares optional for the UI PQ |
| N8 | Security | raw tables readable by the tracing PQs and ops only; derived tables readable by the consuming desks; writes only from merge-dispatcher workers |
| N9 | Operability | every threshold and window is configuration; scripts are Git-sourced (sibling doc `git-persistent-query-scripts.md`) |
| N10 | Portability | the lineage/drift logic is Community-Core table operations and runs unchanged in a Core+ worker; only ingestion and persistence are Enterprise-specific |

## 4. Assumptions the design relies on

| Id | Assumption | If false |
|---|---|---|
| A1 | Within one system, `OrderId` is unique across parents and children for the retained window | add a disambiguating column to the key |
| A2 | Every message carries the order's **full current state**, in particular `CumQty` (a SOW/snapshot style feed), not a delta | a per-system fold is needed before last-by (the fix42 repo's `fix42cache`) |
| A3 | `CumQty` is non-decreasing per order except for busts, which are reported as a new full state | nothing changes; busts show as a transient negative move |
| A4 | Cardinality per §1: `A-Bp` 1 : 1, `Bp-Bc` 1 : n, `Bc-Cp` 1 : 1, `Cp-Cc` 1 : n | the per-edge rule handles any cardinality; the 1 : 1 checks (`CARDINALITY`, `MISSING_CHILD`) become configuration |
| A5 | A node's parent reference (cross-system or in-system) is **stable** for the life of the order | index every id a parent was ever known by (doc 04 §8, as doc 14 §3 does) |
| A6 | No level skips a hop (no C parent referencing an A order or a B parent directly) | ordered candidate parents (doc 14 §2) |
| A7 | Symbol and side use one coding across the three systems, or a mapping exists | normalize in the adapter view |
| A8 | The feeds are Kafka topics keyed by order id; message order within a key is preserved | last-by orders by `SeqNo`/`EventTs` instead of arrival |
| A9 | Order ids are strings; if all systems use numeric ids, longs are used | sizing changes ~5× (doc 06 §1) |
| A10 | One trading day is the natural partition | multi-region sessions need a session-date column |
| A11 | B and C rows carry an **in-system parent id** column (`ParentOrderId`) that is **null on parent orders and non-null on child orders**; that column alone tells parents from children. *Confirmed 2026-10-26 (Q15).* | — |
| A12 | A B child and its C parent are 1 : 1 and the C parent carries the B child's id | if the C parent carries the B *parent's* id instead, the `Bc-Cp` edge becomes `Bp-Cp` (1 : n) and B children are reconciled only inside B |

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
| DIS, Core+, PQ | as in doc 02 §8 |

## 8. Open questions

| # | Question | Why it matters | Default assumed | Owner |
|---|---|---|---|---|
| Q1 | Is 100 M per day, per level, or total ever? | retention, resident state, single worker or not | 100 M nodes per day across the five levels, 2-day window | OMS data owners |
| Q2 | Children per parent at `Bp-Bc` and `Cp-Cc` (typical, max) | rollup cost, UI tree width, the 1 : 1 checks | 1–50 | OMS B / C teams. *Partly answered 2026-10-25: 1 : 1 at the cross-system hops, 1 : n inside B and C* |
| Q3 | Full state per message including `CumQty`, or deltas/executions? | A2 | full state | feed owners |
| Q4 | Can a parent reference change on cancel/replace (cross-system or in-system)? | A5 | stable | OMS B / C |
| Q5 | Can any level skip a hop? | A6 | no | OMS C |
| Q6 | Peak and average message rates per system | sizing | 50 k msg/s peak total | feed owners |
| Q7 | Lag distribution child-before-parent at each hop (p50/p99/max) | N3, N4, the date window | p99 1 h, max 1 day | measured in M1 |
| Q8 | Do busts reduce `CumQty` in all systems, reported as a new state? | A3 | yes | OMS teams |
| Q9 | Who consumes alerts and how? | alert sink | a table plus one HTTP sink | ops |
| Q10 | Enterprise and Core+ Python versions in prod | API names in docs 04–07 | current Core+ | Deephaven admins |
| Q11 | Are the topics keyed by order id? Can the producer key by parent id? | doc 06 §4 routing | keyed by order id | feed owners |
| Q12 | Clock source per OMS; is `EventTs` comparable? | aging uses `RecvTs` by default | receive time | OMS teams |
| Q13 | Symbol/side coding per system | A7 | identical | OMS teams |
| Q14 | Is `LeavesQty` available in all three? | enables an open-exposure view (doc 14's `RouteGap`) | optional | feed owners |
| Q15 | In B and C, which field carries the in-system parent id, and how are parents told from children? **Answered 2026-10-26:** `ParentOrderId`, null on parents, non-null on children | A11; the adapter role split | as answered | — |
| Q16 | Do B and C children also carry the cross-system reference? **Answered 2026-10-26: no** — children carry `ParentOrderId` only, `UpstreamRef` is null on them | children link and route by `ParentOrderId` alone (already the design); no `ROOT_MISMATCH` cross-check is available; `UpstreamRef` non-null on a child is a contract violation the pre-cutover check reports | as answered | — |
| Q17 | Is **execution-level** identification required (R15)? If so, do execution reports cross the hops with a shared id (`ExecID`/`ExecRefID`), or only quantity/price/time? | a second data path (executions) and a set-difference per edge | not in v1 | business owner |
| Q18 | Are B's (and C's) parent and child orders on one topic or two? | one or two ingesters per system (doc 05 §4) | one topic, role by `ParentOrderId` | feed owners |
