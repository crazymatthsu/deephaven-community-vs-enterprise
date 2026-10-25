# 08 — Operations, failure modes and runbooks

Replaces sections 8 and 11.7 of the source document for the design in docs 02–07.

---

## 1. Failure modes

| Failure | Effect on the view | Data at risk | Recovery | Alert |
|---|---|---|---|---|
| An ingester PQ dies | that system's last states stop updating; the other two keep ticking; gaps against it open as `TRANSIENT`, then as drift if it stays down past the aging threshold | none after the last checkpoint; Kafka retains the rest | controller restarts it; it resumes from the checkpoint offset (AC6) | PQ status; Kafka lag; `trace_stats.LastUpdate<X>` stale |
| `oms-lineage` dies | all derived tables disappear until restart | none (derived) | auto-restart; full recompute over the window (N6) | PQ status; UI shows disconnected |
| `oms-drift-writer` dies | `OMS_TRACE.DriftEvents` stops; live views unaffected | transitions during the outage are **not** recorded (blink source) | auto-restart; accept the gap, or widen: have the writer also snapshot `drift_edges` on start | PQ status |
| `oms-trace-ui` dies | dashboard down | none | auto-restart; spares optional | PQ status |
| DIS storage lost / unmounted | ingesters fail to write; lineage reads fail or go stale | the day's intraday partitions on that volume | restore the volume; replay from Kafka for the window (R1 runbook) | storage alerts; PQ errors |
| Kafka retention shorter than an outage | messages lost before ingestion | those messages | request a re-publish from the OMS; the unresolved/drift tables show what is missing | ingest lag vs retention metric |
| B (or A) never arrives for a child | child stays `UNRESOLVED`; its quantity is unattributed | none | nothing to recover: it is the finding; alert at `UNRESOLVED_ALERT_SEC`, forensic record in `DriftEvents` | `unresolved_links.Alerted` count |
| C before B by hours | `UNRESOLVED` then heals (doc 04 §4) | none | none | only if past the alert age |
| Lag longer than the `Date` window | permanently `UNRESOLVED` in the earlier day | none | widen `OMS_TRACE_DATE_WINDOW_DAYS` (restart) | unresolved count by `Date` |
| Duplicate messages (Kafka redelivery, OMS re-send) | none: last-by keeps the latest state | none | none | — |
| Out-of-order messages within one order (A8 fails) | last-by may keep a stale state | wrong `CumQty` until the next message | switch last-by to `sort_descending("SeqNo").first_by` or `agg.sorted_last` on `SeqNo`/`EventTs` in the adapter | compare `SeqNo` monotonicity in M1 validation |
| Re-keyed ids (A1/A5 fail) | orphans or mislinks | none | the id-history index (doc 04 §2, generic path) | resolution rate in `trace_stats` drops |
| Schema drift in a feed (new/renamed field) | rejects rise; or a column reads null | rows rejected until fixed | fix the value spec; replay the partition (R1) | reject rate |
| Clock skew between OMS and ingesters | none: aging uses `RecvTs` | — | — | — |
| Update-graph stall (a long cycle) | views lag | none | find the operator (performance tables); usually a materialized column or a Python callback | cycle p99 > N2 |
| Barrage snapshot on UI start on a large table | lineage worker pauses while building the snapshot | none | subscribe to small tables only (doc 07 §4) | cycle spike at UI restarts |
| Midnight rollover | new `Date` partition; the window includes tomorrow so no gap | none | daily restarts drop the oldest day | partition count per table |
| Nightly merge fails | historical missing for a day; intraday retained (validation did not run) | none | re-run the merge with `force`; validation follows | PQ status of the merge |
| Controller failover | Core+ PQs keep running and re-register | none | none | controller logs |

Compared with the source document's tables: the five pending-buffer and mapping-table modes
are gone; "shard worker death" is `oms-lineage` (or one shard PQ in P3) and recovers from
`OMS_RAW`, not from Kafka.

## 2. Alerting

All alerts derive from `trace_stats` and `unresolved_links`/`drift_edges` in `oms-lineage`,
evaluated by a `TableListener` that posts to `OMS_TRACE_ALERT_URL` (and writes the alert as a
row in `break_events` so it lands in `DriftEvents`). Defaults; tune after the first week.

| Metric | Source | Warn | Page | Dedupe |
|---|---|---|---|---|
| Unresolved links older than the alert age | `unresolved_links.where("Alerted")` count per system | > 0 | > 100 or any older than 4 h | one alert per (System, OrderId) crossing; summary every 15 min while non-zero |
| Drift quantity by edge | `trace_stats.MissingDownstreamQty<Edge>`, `UnbookedUpstreamQty<Edge>` | > 0 for > 5 min | > threshold agreed with the desks (Q9) | per edge, re-alert on 2× growth |
| Drift count by symbol | `drift_by_symbol.Breaks` | any new symbol | > 10 symbols | per symbol |
| Stale system | `now() − max(RecvTs)` per system during market hours | > 60 s | > 5 min | per system |
| Ingest lag | Kafka consumer lag per ingester | > 30 s | > 5 min | per PQ |
| Reject rate | ingester reject counter | > 0 | > 1 % of messages | per PQ |
| Update-graph cycle | performance tables | p99 > 1 s | p99 > 5 s | per PQ |
| Heap | worker heap after GC | > 70 % | > 85 % | per PQ |
| PQ not running (any `oms-*`) | controller | — | immediately, market hours | per PQ |
| Nightly merge/validation failed | controller | next morning | — | per PQ |

A transient gap (`TRANSIENT`) never alerts; that is the point of the aging threshold (R6).

## 3. Runbooks

**R1 — Replay a system's day from Kafka.** Use when a partition is corrupt, a value spec was
wrong, or storage was lost.
1. Stop `oms-ingest-<x>`.
2. Move the affected intraday partition directory out of `/db/dataImportServers/oms_ingest_<x>/...`
   (keep it until the replay is verified).
3. Reset the consumer group's offset to the start of the day (`kafka-consumer-groups
   --reset-offsets --to-datetime`), or start the PQ once with `ignore_offset_from_checkpoints`
   and an explicit start offset per the installed version's option.
4. Start the ingester; watch row counts converge to the previous partition's count (or the
   corrected count).
5. `oms-lineage` needs no action: the live table re-fills and the joins recompute. If the
   partition was replaced wholesale, restart `oms-lineage` to drop stale last-by state.

**R2 — Lineage restart and recovery time.** Expected: PQ start + Σ `last_by` scans + join
init (doc 06 §8). Record the measured value here after M3: `__ min at __ M raw rows`. If a
restart exceeds the initialization timeout, raise the timeout, then apply doc 05 §4.3.

**R3 — Investigate a drift row.** From `drift_edges` take `Edge`, `System`, `OrderId`,
`RootId`. Then (Core+ Python console, §5): the parent's and children's current states, the
raw message history of each from `OMS_RAW` for the day, and the `DriftEvents` transitions for
the node. The `MissedBy` column already names the system; the raw history shows the message the
system never sent (or sent with a lower `CumQty`).

**R4 — Unresolved spike.** `unresolved_links` grouped by `System`, `Date`, `Alerted`: a spike
of C rows with references to B ids that never appear points at a B ingestion gap (check
`oms-ingest-b` lag and rejects) or a B-side change of what it stamps in its id (compare a sample
of `LinkId` values with current B ids; A5). A spike of B rows unresolved against A is the same
check one level up.

**R5 — Feed schema change.** Add the column to the schema (`dhconfig schema`), to the value
spec and, if the lineage needs it, to the adapter view; restart the ingester, then the lineage
PQ at the next daily restart. Renames and type changes: new table name (doc 07 §10).

**R6 — Change the shard count (P3).** Set `OMS_TRACE_SHARD_COUNT` on every shard PQ and the
collector, restart the shard PQs (they rebuild from `OMS_RAW`), then the collector. No data
moves.

**R7 — Acknowledge a known break.** Until the persisted input table of doc 05 §8.3 exists:
record the acknowledgement in the desk's tracker; the row stays in `drift_edges` by design.

## 4. Monitoring

- **Enterprise status dashboard / Query Monitor**: PQ state, restarts, heap per worker.
- **Performance tables** (the worker's update-graph and operation performance logs):
  cycle time, the heaviest operators; watch after every script change.
- **`trace_stats`** as the application's own health row: per system `Orders`, `Unresolved`,
  `UnresolvedAlerted`, `MaxUnresolvedAgeSec`, `LastUpdate`; per edge `Gaps`, `Drift`,
  quantities; `WindowDates`; cycle time copied from the performance table.
- **Kafka**: consumer lag per ingester group; topic retention versus the window.
- **Storage**: free space on the DIS volumes; partition counts per table (should equal the
  window after validation deletes).

## 5. Forensic queries (Core+ Python)

Replacing the source document's §11.6 Groovy snippets:

```python
# everything unresolved, oldest first (any console or PQ with read access to oms-lineage via the client,
# or inside oms-lineage itself)
unresolved_links.sort_descending(["AgeSec"])

# a C order's raw history for the day, across the partitions in the window
db.live_table("OMS_RAW", "OrdersC").where(["Date in `2026-10-25`, `2026-10-24`", "OrderId == `C-00123`"]).sort(["RecvTs"])

# every B order that pointed at a given A order, with its current state
orders_b.where("LinkId == `A-00042`")

# break transitions for one node, from the persisted history (yesterday and today)
db.live_table("OMS_TRACE", "DriftEvents").where(["Date in `2026-10-25`, `2026-10-24`", "System == `B`", "OrderId == `B-00777`"]).sort(["Ts"])

# what B missed per symbol yesterday, from the EOD snapshot
db.historical_table("OMS_TRACE", "DailyDriftSummary").where(["Date == `2026-10-24`", "Kind == `symbol`", "Edge == `AB`"])

# aged gaps only, as of now
drift_edges.where("GapAgeSec > 300").sort_descending(["FillGap"])
```

`db.live_table` returns intraday partitions, `db.historical_table` merged ones; a `Date` filter
is always the first clause.
