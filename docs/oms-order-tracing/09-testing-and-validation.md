# 09 — Testing and validation

How the implementation proves docs 03–04 correct, how the sizing claims of doc 06 are
measured, and what must be true before cutover. The approach is the one the fix42 repo's
doc 14 module uses: a pure-Python **reference oracle** computes every output from dicts and
loops, unit tests pin the oracle to the scenario catalog, and an embedded-engine test asserts
the Deephaven DAG equals the oracle row for row.

---

## 1. Layers

| Layer | Runs where | Needs | Proves |
|---|---|---|---|
| L1 oracle unit tests | any Python 3.10+, no Deephaven | `oms_trace.reference`, `oms_trace.mockdata` | the semantics of doc 03 §6 on every scenario; the two identities |
| L2 embedded engine | `deephaven-server` wheel in a venv (the fix42 repo's `*_DH_TEST=1` pattern) | the DAG module | the DAG ≡ oracle, row for row, on static books; live healing with keyed input tables |
| L3 Community end to end | Community server in Podman, mock or replayed Kafka | `OMS_TRACE_SOURCE=kafka|files` | the app entry, the UI, sizing at 1 M rows |
| L4 Enterprise integration | dev-enterprise / UAT | the ingesters, DIS, PQs | AC5–AC8, replay, midnight, merge |
| L5 pre-cutover validation | UAT with real feeds | `oms_trace.trace_check` CLI | AC7: the feeds mean what the adapters assume |

## 2. Scenario catalog

Each scenario is a small five-level book (`A_ROOT`, `B_PARENT`, `B_CHILD`, `C_PARENT`,
`C_CHILD` states) plus the expected `drift_edges`, `drift_by_a_order` and `unresolved_links`.
L1 asserts the oracle; L2 asserts the engine equals the oracle. "A(1000/1000)" reads
`OrderQty/CumQty`.

| Scenario | Book | Expected |
|---|---|---|
| `clean_chain` | A(1000/1000) → Bp(1000/1000) → Bc1(600/600), Bc2(400/400) → Cp1, Cp2 (1 : 1) → Cc1a(300/300), Cc1b(300/300), Cc2a(400/400) | no rows anywhere (AC1; the source design's per-row diff would show 1 000 of false drift on `A-Bp` vs `Bc`) |
| `working` | same shape, partially filled everywhere consistently | no rows |
| `missing_at_b` | B's child book: Bc2 shows 300; A, Bp, Cp2, Cc2a show 400 of it | `Bp-Bc` on Bp: `+100 INTERNAL ShortSide CHILD MissedBy B`; `Bc-Cp` on Bc2: `−100 UNBOOKED_UPSTREAM MissedBy B`; root: `InternalB +100`, `UnbookedAtB 100` |
| `missing_at_c` | Cp2 shows 300 of Bc2's 400; Cc2a shows 300 | `Bc-Cp` on Bc2: `+100 MISSING_DOWNSTREAM MissedBy C`; root: `MissingAtC 100` |
| `missing_at_c_children` | Cp2 shows 400, its Cc children sum to 300 | `Cp-Cc` on Cp2: `+100 INTERNAL MissedBy C`; root: `InternalC +100` |
| `unbooked_at_a` | everything below agrees on 1000, A shows 900 | `A-Bp` on A: `−100 UNBOOKED_UPSTREAM MissedBy A`; root: `UnbookedAtA 100` |
| `b_parent_lags` | Bc children sum 1000, Bp shows 900 | `Bp-Bc` on Bp: `−100 INTERNAL ShortSide PARENT MissedBy B` |
| `transient` | as `missing_at_c` but the Cp fill is 10 s old and Bc's newer | `TRANSIENT`, no drift rows; after the aging threshold → `MISSING_DOWNSTREAM` (AC3) |
| `late_bc_seconds` | Cp1 arrives, Bc1 5 s later | `UNRESOLVED` then `LINKED`; Cc1* `RootState` UNRESOLVED then RESOLVED; no drift |
| `late_bc_hours` | Cp1 at T, Bc1 at T + 2 h | `unresolved_links` row with `AgeSec`, `Alerted` after 3600 s; `root_unresolved` holds Cc1a, Cc1b; heals at T + 2 h in one cycle (AC2) |
| `bc_never` | Cp1's Bc1 never arrives | `UNRESOLVED`, `Alerted`; `Cp-Cc` still reconciles; present next window if the dates are in it (AC4) |
| `late_bp` | Bc1 arrives before Bp1 | `links_bc UNRESOLVED`; Bc1's Cp and Cc have `RootState UNRESOLVED`; heals when Bp1 lands |
| `late_a` | Bp1 arrives before A1 | `links_bp UNRESOLVED` but `RootId = A1` already (the A id is on the row); `edge_a_bp` has no A row; heals when A1 lands |
| `no_link` | a `C_PARENT` with empty `UpstreamRef` | `NO_LINK`; counted, never joined |
| `parent_misfiled` | a B parent row with `ParentOrderId = ""` (empty, not null) | filed as a parent (the role rule tests both); the `_null` twin likewise |
| `two_b_parents` | two B parents reference the same A | `CARDINALITY` on the `A-Bp` row once aged; both B subtrees still reconcile |
| `b_child_never_reached_c` | Bc3 (400/0) has no C parent | `MISSING_CHILD` (amber) on `Bc-Cp` for Bc3; becomes `MISSING_DOWNSTREAM` if Bc3 executes; `CParentsMinusBChildren = −1` |
| `mismatch` | a Cc SELL under a BUY Cp | `MISMATCH` on the Cc row; quantities still roll up |
| `overfill` | a Cp shows `CumQty > OrderQty` | `OVERFILL` on Cp plus whatever gap it causes |
| `bust` | Cc reports 300 then 200 (bust); Cp, Bc, Bp, A follow within 30 s | transient negative gaps climbing the chain, then none; if Bc never follows: `Bc-Cp` on Bc2 `+100 MISSING_DOWNSTREAM` (B says 400, C says 300) |
| `duplicate_messages` | the same Cc state delivered twice | identical to once |
| `out_of_order_same_order` | Cc states with `SeqNo` 2 then 1 | last-by on arrival keeps the stale one unless the adapter orders by `SeqNo` (documents the configured mode; A8) |
| `rekey_c_ref` | a Cp's `UpstreamRef` rotates after a Bc cancel/replace | `UNRESOLVED` in the native-id DAG; `LINKED` in the generic id-history DAG (A5 decides which ships) |
| `midnight` | Bc on `Date` D, Cp on D + 1, window 2 | `LINKED` |
| `window_exceeded` | Bc on D, Cp on D + 2, window 2 | `UNRESOLVED` on D + 2 (documented limitation; sets Q7) |
| `large_fanout` | A → Bp → 50 Bc → 50 Cp → 10 Cc each (2 500 Cc rows) | all `NONE`; rollup counts 50, 1 and 10 |

The mock generator (`oms_trace.mockdata`) builds every scenario plus N random healthy families
with realistic fan-out, so the "no drift" assertion runs over a book, not a toy. Doc 14's
`mockdata.py` already produces this five-level shape (Raptor / algo / router) and is the
starting point.

## 3. Invariants asserted on every run

1. **Telescoping**: for each fully linked root, `ACumQty − StreetCumQty == GapABp + Σ GapBpBc + Σ GapBcCp + Σ GapCpCc`.
2. **Attribution sums**: `MissingAtB − UnbookedAtA == GapABp`, `MissingAtC − UnbookedAtB == Σ GapBcCp`, `InternalB == Σ GapBpBc`, `InternalC == Σ GapCpCc`.
3. **Conservation**: `Σ drift_by_symbol.MissingDownstream` over edges equals
   `Σ drift_edges.max(FillGap, 0)` for drift rows.
4. **No row in two states**: a child is in `unresolved_links` xor contributes to a parent's
   rollup.
5. **Monotone healing**: a parent's arrival never increases the unresolved count.
6. **Direct children only**: changing a `C_CHILD` never changes `edge_a_bp`'s or `edge_bp_bc`'s `ChildCumQty`.
7. **Root propagation**: every node with `RootState = RESOLVED` has the `RootId` of the A row reachable by following `LinkId` four times at most; `root_unresolved` is exactly the set of linked nodes with a broken chain above.

## 4. Live tests in the embedded engine (L2)

The three raw tables are **keyed input tables** (`OrderId` key) so a test can add or replace a
state and assert the DAG after each step, as the fix42 repo's live-fill tests do:

1. `heals_in_one_cycle`: add Cp1 and its Cc rows → Cp1 `UNRESOLVED`, Cc rows `RootState UNRESOLVED`; add
   Bc1 → all `LINKED`/`RESOLVED` and the `edge_bc_cp` row present, in the same `await_update`.
2. `aging_flips_transient_to_drift`: use a test clock (a keyed input table standing in for
   `time_table`) and advance `Now` past `AGE_SEC`.
3. `no_throw_on_duplicate_ids`: insert two raw rows for one `OrderId`; `natural_join` does not
   throw (the adapter `last_by` is in place).
4. `restart_equivalence`: build the DAG twice over the same raw tables (fresh session) and
   compare every output (AC5's logic without the PQ).
5. `break_events_blink`: each transition produces exactly one blink row.

## 5. Sizing run (L3, doc 06 §1)

Replay 1 M orders per system (captured samples or mock at the measured fan-out) into a
Community server with the doc 04 DAG and record, from the server's own metrics:

| Measurement | How |
|---|---|
| heap per order per system | `Runtime` used heap after a forced GC, divided by rows in `orders_*`, with string and with long ids |
| `last_by` scan rate | wall time of the initial build over N raw rows |
| cycle time at a replay rate of 50 k msg/s | performance tables |
| `gaps` candidate count on a healthy book | `gaps.size()` (should be ~0 when quiet) |
| UI subscription sizes | rows in each subscribed table |

Plug into doc 06 §1.3 and §8; set the PQ heap and N6 from the result.

## 6. Pre-cutover validation (L5) — `oms_trace.trace_check`

A pure-Python CLI (pattern: the fix42 repo's `fixstate_check`) that reads a sample of each
topic (or a day of `OMS_RAW` exported to files) and prints, per system:

- rows, distinct `OrderId`, messages per order (expect ≥ 1, median by Q3);
- `UpstreamRef` null rate (must be ~0 on B and C, 100 % on A);
- **role split**: parents versus children per system from `ParentOrderId` (null and empty
  counted separately, Q15), and whether children carry `UpstreamRef` too (Q16);
- **resolution rate per hop**: B parents → A ids, B children → B parent ids, C parents → B
  child ids, C children → C parent ids (AC7 ≥ 99.9 % on a complete sample window; lower means
  A5/A6 or an ingestion gap);
- **cardinality per hop**: B parents per A and C parents per B child (both ≈ 1, A4); children
  per parent inside B and C (Q2); flags > 1 000 as a probable key error;
- `CumQty` monotonicity per order (busts counted; Q8);
- symbol/side coding per system (A7);
- `SeqNo`/`EventTs` ordering versus arrival (A8).

Cutover requires the printed report attached to the change record, with every assumption in
doc 01 §4 either confirmed or reflected in configuration.

## 7. Enterprise integration tests (L4)

Scripted against dev-enterprise/UAT; each is a numbered step in `scripts/uat_checks.sh` with
an expected observation.

1. Ingest a replayed day into the three topics; `OMS_RAW` row counts equal the replay counts.
2. SIGKILL `oms-ingest-b` mid-replay, restart: counts and offset ranges continuous (AC6).
3. SIGKILL `oms-lineage` mid-day, restart: outputs equal the oracle over the same raw tables
   (AC5); record recovery time (R2).
4. Publish a C whose B is held back 2 h: `unresolved_links`, alert at 1 h, heal (AC2).
5. Midnight rollover with the window: no restart needed; next morning's restart drops D − 2.
6. Data Merge + Validation for D − 1; `db.historical_table` returns the rows; intraday D − 2
   deleted; `DriftEvents` merged.
7. Malformed record on a UAT topic: rejected and counted; the feed continues.
8. One trading day at agreed volume: heap ≤ 70 %, cycle p99 ≤ 1 s (AC8).

## 8. Acceptance mapping

| AC (doc 01 §6) | Tests |
|---|---|
| AC1 | `clean_fanout`, `missing_at_b` (L1, L2) |
| AC2 | `late_b_hours` (L1, L2 `heals_in_one_cycle`), L4 step 4 |
| AC3 | `transient` (L1, L2 `aging_flips_transient_to_drift`) |
| AC4 | `b_never` (L1, L2), L4 step 4 |
| AC5 | L2 `restart_equivalence`, L4 step 3 |
| AC6 | L4 step 2 |
| AC7 | L5 report |
| AC8 | L3 sizing, L4 step 8 |
