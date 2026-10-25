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

Each scenario is a small book (A, B, C states) plus an expected `drift_edges`, `drift_by_a_order`
and `unresolved_links`. L1 asserts the oracle; L2 asserts the engine equals the oracle.

| Scenario | Book | Expected |
|---|---|---|
| `clean_1_1_1` | A(1000/1000) → B(1000/1000) → C(1000/1000) | no rows anywhere |
| `clean_fanout` | A(1000/1000) → B1(600/600), B2(400/400) → C1, C2 | no rows (AC1; the source design would show 1 000 of false drift) |
| `working` | A(1000/300) → B1(600/300), B2(400/0) → C1(600/300), C2(400/0) | no rows |
| `missing_at_b` | B2 shows 300 while A shows 400 of it executed (A: 1000/1000, B: 600+300, C: 600+400) | `edge AB` A: `FillGap +100 MISSING_DOWNSTREAM MissedBy B`; `edge BC` B2: `−100 UNBOOKED_UPSTREAM MissedBy B`; `drift_by_a_order`: `MissingAtB 100`, `UnbookedAtB 100` |
| `missing_at_c` | C2 shows 300 of B2's 400 | `edge BC` B2: `+100 MISSING_DOWNSTREAM MissedBy C`; root: `MissingAtC 100` |
| `unbooked_at_a` | C and B agree on 1000, A shows 900 | `edge AB` A: `−100 UNBOOKED_UPSTREAM MissedBy A`; root: `UnbookedAtA 100` |
| `transient` | as `missing_at_c` but the C fill is 10 s old and B's is newer | `BreakKind TRANSIENT`, no drift rows; after the aging threshold → `MISSING_DOWNSTREAM` (AC3) |
| `late_b_seconds` | C1 arrives, B1 5 s later | `UNRESOLVED` then `LINKED`; no drift |
| `late_b_hours` | C1 at T, B1 at T + 2 h | `unresolved_links` row with `AgeSec`, `Alerted` after 3600 s; heals at T + 2 h in one cycle (AC2) |
| `b_never` | C1's B never arrives | `UNRESOLVED`, `Alerted`; present next window if the dates are in it (AC4) |
| `late_a` | B1 arrives before A1 | `links_b UNRESOLVED`; `edge_ab` has no A row; heals when A lands |
| `no_link` | a C row with empty `UpstreamRef` | `NO_LINK` in `unresolved_links`; counted, never joined |
| `mismatch` | a C row SELL under a BUY B | `MISMATCH` on the C row; quantities still roll up |
| `overfill` | B shows `CumQty > OrderQty` | `OVERFILL` on B, plus whatever gap it causes |
| `bust` | C reports 1000 then 900 (bust); B follows 5 s later | transient negative then zero gap; with B never following → `UNBOOKED_UPSTREAM` on B? no: B (900) vs C (900) reconciles once B follows; if B stays 1000: `MISSING_DOWNSTREAM +100 MissedBy C` (B says 1000, C says 900) |
| `duplicate_messages` | the same C state delivered twice | identical to once |
| `out_of_order_same_order` | C states with `SeqNo` 2 then 1 | last-by on arrival keeps the stale one unless the adapter orders by `SeqNo` (the test documents which mode is configured; A8) |
| `rekey_c_ref` | C's `UpstreamRef` rotates after a B cancel/replace | `UNRESOLVED` in the native-id DAG; `LINKED` in the generic id-history DAG (A5 decides which ships) |
| `midnight` | B on `Date` D, C on D + 1, window 2 | `LINKED` |
| `window_exceeded` | B on D, C on D + 2, window 2 | `UNRESOLVED` on D + 2 (documented limitation; sets Q7) |
| `large_fanout` | A → 50 B → 50 C each (2 500 C rows) | all `NONE`; rollup counts 50 and 2 500 |

The mock generator (`oms_trace.mockdata`) builds every scenario plus N random healthy families
with realistic fan-out, so the "no drift" assertion runs over a book, not a toy.

## 3. Invariants asserted on every run

1. **Telescoping**: for each fully linked root, `ACumQty − Σ C CumQty == GapAB + Σ GapBC`.
2. **Attribution sums**: `MissingAtB − UnbookedAtA == GapAB`, `MissingAtC − UnbookedAtB == Σ GapBC`.
3. **Conservation**: `Σ drift_by_symbol.MissingDownstream` over edges equals
   `Σ drift_edges.max(FillGap, 0)` for drift rows.
4. **No row in two states**: a child is in `unresolved_links` xor contributes to a parent's
   rollup.
5. **Monotone healing**: a parent's arrival never increases the unresolved count.
6. **Direct children only**: changing a C never changes `edge_ab`'s `ChildCumQty`.

## 4. Live tests in the embedded engine (L2)

The three raw tables are **keyed input tables** (`OrderId` key) so a test can add or replace a
state and assert the DAG after each step, as the fix42 repo's live-fill tests do:

1. `heals_in_one_cycle`: add C1 → `UNRESOLVED`; add B1 → `LINKED`, `edge_bc` row present, in
   the same `await_update`.
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
- **resolution rate**: fraction of B refs found among A ids, C refs among B ids (AC7 ≥ 99.9 %
  on a complete sample window; lower means A5/A6 or an ingestion gap);
- **fan-out**: children per parent distribution (Q2); flags 1:1 if the design could be
  simplified, flags > 1 000 as a probable key error;
- **cardinality sanity** (source §8): distinct B ids ÷ distinct A ids and C ÷ B within the
  expected band;
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
