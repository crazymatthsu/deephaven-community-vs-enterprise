# 05 — Ingestion and persistence on Deephaven Enterprise

How the three OMS feeds become durable `OMS_RAW` tables, how the lineage PQ reads them, how
history is kept, and how derived results are persisted. This replaces sections 2, 4.3 (step 1)
and 11 of the source document (doc 00 F2, F5, F9).

API identifiers below are the ones the Core+ documentation uses; the site could not be fetched
from the review environment, so **confirm each signature against the installed version's
pages** (links in §11) before the first deployment. Where a name is uncertain it is marked.

---

## 1. Decision: one Core+ Kafka ingester PQ per system

| Option | When | Persistence | Replay / restart | Chosen |
|---|---|---|---|---|
| **Core+ Kafka ingester** in a worker that embeds a Data Import Server | the OMS publishes to Kafka (A8) | the in-worker DIS writes intraday partitions to its private storage; offsets live in the DIS checkpoint record, updated atomically with the table size | resumes from the checkpoint offset: exactly-once; forced replay by ignoring checkpoints or resetting the partition | **yes**, for all three systems |
| Deephaven binary logs → Data Tailer → system DIS | the OMS (or an adapter next to it) can write Deephaven-format binary logs with the Enterprise logger | the system DIS writes intraday partitions | tailer `startupLookbackTime`; DIS checkpoint truncation | fallback when a feed is not on Kafka |
| Custom source → `TablePublisher` → Derived Table Writer | AMPS, TCP, files | a DTW appends an add-only/blink table to a DIS | DTW checkpoints | used for **derived** tables (§8), not for raw feeds |

Why not the source doc's "DIS-A tailer" picture: a DIS does not consume Kafka and does not
write binary logs; the tailer ships logs an application wrote. For Kafka the platform's own
mechanism is the in-worker DIS ingester, which also offers a persisted `lastBy` view (§4.3).

## 2. Routing configuration

Each ingester owns a DIS with private storage that **claims** its table, so no other DIS (and
no tailer) can write it, and query workers find it through the Table Data Service.

```bash
# One DIS per ingester, private storage, exclusive claim. Run on a node with the storage mounted.
dhconfig dis add --name oms_ingest_a --storage /db/dataImportServers/oms_ingest_a --claim OMS_RAW.OrdersA
dhconfig dis add --name oms_ingest_b --storage /db/dataImportServers/oms_ingest_b --claim OMS_RAW.OrdersB
dhconfig dis add --name oms_ingest_c --storage /db/dataImportServers/oms_ingest_c --claim OMS_RAW.OrdersC
dhconfig dis add --name oms_trace    --storage /db/dataImportServers/oms_trace    --claim OMS_TRACE   # §8
```

Equivalent `routing.yml` shape (an entry under `dataImportServers`; a DIS entry uses `claims`
*or* `filters`, never both):

```yaml
dataImportServers:
  oms_ingest_b:
    endpoint: { serviceRegistry: true }        # or host/port on older formats
    claims:
      - { namespace: OMS_RAW, tableName: OrdersB }
    storage: oms_ingest_b_storage              # a `storage` entry pointing at /db/dataImportServers/oms_ingest_b
```

Query workers read through a `tableDataServices` entry whose sources include the dynamic
`dataImportServers` source, which covers every DIS added with `dhconfig dis`. Check with:

```bash
dhconfig routing validate       # or the equivalent on the installed version
dhconfig dis list
```

The storage directory must be dedicated to that DIS (the DTW and Kafka pages both say so) and
on the shared filesystem the merge and query nodes mount, so a restarted ingester on another
node finds its checkpoints and the merge PQ (§7) finds the partitions.

## 3. Schemas

Deploy the three `OMS_RAW` schemas from doc 03 §2 and the `OMS_TRACE` schemas from doc 03 §5
before any ingester starts:

```bash
dhconfig schema import --file schemas/OMS_RAW.OrdersA.schema   # one per table
dhconfig schema list --namespace OMS_RAW
```

The ingester's `TableDefinition` (the JSON/Avro value spec) must match the deployed schema
column for column; a mismatch is a start-up error, which is the behaviour we want (Core+
input/ingest paths enforce column types).

## 4. The ingester PQ script

One script, parameterized by system. It runs as a **"Live Query - Merge Server"** PQ (the Core+
Kafka page's production recommendation: writes to DIS storage happen on the merge dispatcher).

```python
# oms-ingest-<x>: Core+ Python, "Live Query - Merge Server" PQ.
# Identifiers as named on the Core+ "Ingest Kafka Data" page; import them from the
# deephaven_enterprise package of the installed version (the module path varies by release).
import os
from deephaven.stream.kafka.consumer import KeyValueSpec, json_spec   # Community spec builders
from deephaven.table import TableDefinition
from deephaven import dtypes as dt
# from deephaven_enterprise.<kafka module> import kafka_table_writer_options, get_dis_by_name, OnReject, LastByView

SYSTEM = os.environ["OMS_SYSTEM"]                  # A | B | C
TABLE  = f"Orders{SYSTEM}"
DIS    = f"oms_ingest_{SYSTEM.lower()}"
TOPICS = os.environ["OMS_TOPICS"].split(",")      # this PQ's share of the system's topics (§4.3b)
TZ     = os.environ.get("OMS_TRACE_TZ", "America/New_York")

value_def = TableDefinition({
    "OrderId": dt.string, "UpstreamRef": dt.string, "ParentOrderId": dt.string,   # ParentOrderId: in-system parent, null on parents
    "Symbol": dt.string, "Side": dt.string,
    "OrderQty": dt.long, "CumQty": dt.long, "LeavesQty": dt.long, "AvgPx": dt.double,
    "OrdStatus": dt.string, "EventTs": dt.Instant, "SeqNo": dt.long,
})

ONE_TABLE = os.environ.get("OMS_TRACE_ONE_TABLE_PER_SYSTEM", "true") == "true"
kafka_props = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    # security.*, sasl.* from the environment's secrets, never in the script
}

dis = get_dis_by_name(DIS)                    # the DIS from §2; must have private storage
for topic in TOPICS:                          # one ingester per topic (§4.3b)
    opts = kafka_table_writer_options(
        dis_name=DIS,
        namespace="OMS_RAW",
        table_name=TABLE if ONE_TABLE else f"{TABLE}_{topic.rsplit('.', 1)[-1]}",   # §4.3b: preferred vs fallback layout
        topic=topic,
        kafka_properties={**kafka_props, "group.id": f"dh-oms-ingest-{SYSTEM.lower()}-{topic}"},
        key_spec=KeyValueSpec.IGNORE,             # the key is the order id; it is also in the value
        value_spec=json_spec(value_def),          # or avro_spec(...) with the schema registry
        partition_func=daily_partition("RecvTs", TZ),   # §4.2: a callable mapping the row's RecvTs to a yyyy-MM-dd string in TZ
        on_reject=OnReject.LOG,                   # §4.1: never TERMINATE in production
        # internal_partition=topic,              # §4.3b preferred layout, if the release exposes it
        # last_by=LastByView(["OrderId"]),       # §4.3, optional
    )
    dis.consume(opts)                             # each runs for the life of the PQ
```

B's and C's parent and child orders arrive on **one topic per system** (Q18) and land in one
table; the lineage adapter splits them on `ParentOrderId`. The OMS teams publish a parent before
its children and, within a system, the parent may be assumed to arrive first (Q7). The topic is
keyed by **order id** (Q11), so the ingester can preserve that order only within a Kafka
partition; the lineage DAG does not depend on it (A15), and any child-first arrival a lagging
partition produces is a transient `UNRESOLVED` state that heals.

`RecvTs`, `KafkaPartition` and `KafkaOffset` are supplied by the ingester's built-in
timestamp/offset columns where the version provides them; otherwise add them through the value
spec's derived columns. The lineage adapter only needs `RecvTs`.

### 4.1 Validation at the edge

The ingester is the only place that sees malformed records. `OnReject.LOG` writes the reject
to the worker log and keeps going; count rejects (a small `TablePublisher` fed from the reject
callback where the version exposes one, otherwise log-scrape) and alert on a rate (doc 08
§2). Terminating on a bad record (`TERMINATE`, the default) would stop a whole system's feed
for one bad message.

### 4.2 Partitioning by `Date`

`partition_func` rotates the column partition daily in `OMS_TRACE_TZ` from the receive
timestamp (A10). A fixed `partition_value` works only if the PQ restarts after midnight, which
the daily-restart schedule (doc 07 §7) guarantees; the function form avoids depending on it.
Both forms exist on the Core+ page.

### 4.3 The persisted lastBy view

`LastByView(["OrderId"])` (Java: `opts.lastBy(...)`) makes the ingester maintain the latest
row per order and **persist that state in the checkpoint record**, so after a restart the
last-state table is available without rescanning the day. Use it when:

- the lineage PQ's start-up `last_by` over the window takes longer than N6 allows (doc 06 §8);
- more than one consumer needs last state (several shard PQs, the UI) and should not each
  rescan.

Cost: heap in the ingester proportional to distinct orders in the partition (doc 06 §1), and
one more table the lineage PQ subscribes to or reads. Phase 1 does without it; the gate is in
doc 06 §3.

### 4.3b Several topics per system (Q6)

A system publishes its latest order states on several topics (OMS B: 7–8), each peaking at
~10 k msg/s, so a system bursts at ~80 k msg/s near the close. One `kafka_table_writer_options`
+ `dis.consume` serves one topic; the ingester script therefore loops over its `OMS_TOPICS` and
starts one ingester per topic. Grouping: a few topics per "Live Query - Merge Server" PQ, sized
from the parse throughput measured in M2 (JSON parsing is the cost; start at ≤ 4 topics, ≤ 40 k
msg/s per PQ, 4 GB heap) — or one PQ per topic when operational isolation matters more than PQ
count. Each ingester has its own consumer group and checkpoints independently, so a slow topic
never holds back the others.

Where the rows land is the one point to confirm on 2026.01.054 before M2 starts:

- **Preferred — one table per system.** Enterprise intraday tables are written as *internal
  partitions* (one per writer) under the column partition; if the Core+ Kafka ingester lets each
  topic's ingester name its internal partition, all of a system's topics write `OMS_RAW.Orders<X>`
  and the lineage reads one table. Check the ingester options and the DIS claims for that
  release.
- **Fallback — one table per topic.** `OMS_RAW.Orders<X>_<topic>` with the same schema; the
  lineage adapter merges the system's tables before `last_by` (`OMS_TRACE_ONE_TABLE_PER_SYSTEM=false`,
  doc 04 §1). Nightly merge and validation then run per table. The `Topic` column makes the two
  layouts interchangeable for forensics.

### 4.4 Execution feeds (P2b)

Each system's execution reports are ingested the same way from its execution topic(s) into
`OMS_RAW.Execs<X>` (doc 03 §2.1): further `kafka_table_writer_options` + `dis.consume` loops in
the system's ingester PQs (one DIS can serve several tables it claims), or separate PQs when the
execution topics alone saturate a worker (they usually carry more messages than the state
topics). The adapter normalizes the feed's execution-type codes
to `FILL` / `BUST` / `CORRECT` in the value spec's derived columns, or in the lineage adapter if
the raw codes are kept. Retention follows the order tables; the lineage PQ reads executions
only for parents with an aged gap (doc 04 §10), so a day's executions are paged, not resident.

### 4.5 Exactly-once and restart

- Kafka offsets are stored in the DIS checkpoint record, updated atomically with the table
  size; on start the ingester reads the most recent column partition's checkpoint and resumes
  from that offset. No duplicates, no gaps (AC6).
- A forced replay (bad deserialization fixed, schema corrected) is: stop the PQ, remove or
  rename the affected intraday partition under the DIS storage, restart with
  `ignore_offset_from_checkpoints=True` (or the version's equivalent) and the consumer's
  offset reset to the day's start. Document it as runbook R1 (doc 08 §3).
- The Kafka topic's retention must exceed the longest outage you intend to recover from
  without a replay from the OMS (N4: at least the intraday window).

## 5. What the lineage PQ sees

`db.live_table("OMS_RAW", "OrdersB")` in `oms-lineage` is the live union of that table's
intraday partitions, served from the ingester's DIS through the Table Data Service. Filter on
`Date` immediately (doc 04 §1): partition pruning happens on that column. New rows appear as
the ingester checkpoints them (latency: the DIS flush interval, typically sub-second).

- The raw columns are paged from storage; the lineage worker's heap holds derived state
  (last-by keys, join tables, aggregates), not a copy of the day's messages (doc 06 §1).
- A restarted ingester does not disturb the lineage PQ: the live table stops ticking and
  resumes; gaps that opened because one side went quiet are `TRANSIENT`/`UNRESOLVED` and heal.
- The `Date` list is computed at start and includes tomorrow, so midnight needs no restart;
  the scheduled daily restart drops the oldest day (doc 07 §7).

## 6. Historical storage and retention

| Store | Written by | Retention (N4 defaults) | Read with |
|---|---|---|---|
| intraday partitions, `OMS_RAW.Orders*` and `Execs*` | the ingesters | today + `OMS_TRACE_DATE_WINDOW_DAYS` (deleted by the validation PQ after merge) | `db.live_table` |
| historical partitions, `OMS_RAW.*` | **Data Merge** PQ nightly, partition value formula = previous day (`MergeIntradayData`; `force` to re-merge) | 90 days, then the platform's archival | `db.historical_table` |
| `OMS_TRACE.DriftEvents` | the drift writer (§8), merged nightly like the raw tables | 90 days | both |
| `OMS_TRACE.DailyDriftSummary` | EOD snapshot (§8) | indefinitely (small) | `db.historical_table` |

The nightly chain (doc 07 §2): `oms-merge-raw` (Data Merge, 01:00) → `oms-validate-raw`
(Data Validation, dependent on the merge succeeding, row-count validator against intraday,
"Delete intraday data" when it passes). Intraday partitions for the window the lineage PQ still
reads are **not** deleted: validation deletes `today − WINDOW` and older only.

## 7. What replaces each part of the source document's §11

| Source §11 item | Here |
|---|---|
| 11.1 the log and the partitions are the store | the Kafka topic (retention ≥ window) and `OMS_RAW` intraday/historical partitions |
| 11.2 the DIS is a writer only | yes: three in-worker DISes, one per system, plus one for `OMS_TRACE` |
| 11.3 "a PQ is needed to read persisted data" | any worker, console or client reads `db.live_table` / `db.historical_table`; PQs are for work that must survive sessions |
| 11.4 `oms.pending_c` user table keyed on `b_ref_id` | `links_cp.where("ParentState != `LINKED`")` — the C parents whose B child has not arrived, a view over `OrdersC`; no second store |
| 11.4 flush path when B arrives | the `natural_join` modifies the C row in the same cycle (doc 04 §4) |
| 11.4 reconciliation PQ every 5 minutes | removed; aging is a clock join on the candidate rows (doc 04 §5) |
| 11.5 memory argument | holds for the raw tables themselves (paged from the DIS) |
| 11.6 `db.t("oms","pending_c")` Groovy queries | Core+ Python forensic queries in doc 08 §5 |
| 11.7 failure table | doc 08 §1, without the three removed components |

## 8. Persisting derived results

### 8.1 Break history — Derived Table Writer

`break_events` (doc 04 §7) is a blink table in `oms-lineage`. `oms-drift-writer`, a Live
Query - Merge Server PQ, subscribes to it and appends it to `OMS_TRACE.DriftEvents` through the
Derived Table Writer bound to the `oms_trace` DIS from §2:

```groovy
// Core+ Groovy names from the Derived Table Writer page; the Python wrapper, if present in the
// installed version, mirrors them. Run on the merge dispatcher.
import io.deephaven.enterprise.dataimportserver.DataImportServerTools
import io.deephaven.enterprise.derivedtablewriter.DerivedTableWriter

dis = DataImportServerTools.getDisByNameWithStorage("oms_trace", "/db/dataImportServers/oms_trace")
events = /* Barrage subscription to oms-lineage's break_events (doc 07 §4) */
DerivedTableWriter.ingestTable(dis, "OMS_TRACE", "DriftEvents", todayPartition, events,
        new DerivedTableWriter.Options().onReject(DerivedTableWriter.OnReject.LOG))
// Options also include sequenceColumn, transactionsEnabled, logInitial, chunkSize, lastBy.
```

Why a separate PQ: writes to a DIS belong on the merge dispatcher, and a blink subscription is
tiny, so the lineage worker's update graph never waits on storage.

### 8.2 End-of-day summary — partitioned user table (Python)

```python
# in oms-lineage, just before the daily restart (or a tiny dependent Batch PQ that subscribes)
from deephaven import merge
summary = drift_by_symbol.update_view(["Kind = `symbol`"])  # plus drift_by_a_order, trace_stats with a Kind column
# first time only (the prototype must not include the partitioning column):
# db.add_partitioned_table_schema("OMS_TRACE", "DailyDriftSummary", "Date", summary)
db.add_table_partition("OMS_TRACE", "DailyDriftSummary", today_str, summary.snapshot())
```

User-table writes require merge-dispatcher permissions (the user-tables page: write and delete
rights sit with merge-dispatcher workers); run this step from a Batch PQ on the merge
dispatcher if the lineage PQ runs on a query dispatcher.

### 8.3 Why not a persisted input table

Core+ persisted input tables are keyed and editable from the UI. They fit a hand-maintained
exception list (orders a desk has reviewed and acknowledged), which is a reasonable later
addition: `drift_edges.natural_join(acknowledged, on=["System","OrderId"])` hides acknowledged
breaks. They are the wrong fit for the pending buffer the source doc described, which is
gone anyway.

## 9. Community prototype inputs (doc 10 M1)

The same adapters and DAG run on a Community server with three inputs swapped in:

- `OMS_TRACE_SOURCE=mock`: `mockdata.py` generates A/B/C books and scenarios (doc 09 §2).
- `OMS_TRACE_SOURCE=kafka`: Community `consume(...)` on the same topics into append tables
  (no durability — the prototype's point is the DAG, not persistence).
- `OMS_TRACE_SOURCE=files`: replayed JSON lines captured from the real topics, for sizing.

`db.live_table(...)` is wrapped by `sources.raw_table(system)` so the switch is one function.

## 10. Checklist before the first Enterprise run

1. Schemas imported; `dhconfig schema list` shows `OMS_RAW.Orders{A,B,C}` and `OMS_TRACE.*`.
2. DISes exist with private storage on shared disk; claims validated; TDS routing includes them.
3. Kafka credentials in the environment's secrets; topics reachable from the merge nodes.
4. Each ingester PQ starts, writes today's partition, and `db.live_table` in a console ticks.
5. Kill an ingester (SIGKILL) and restart: row counts and offset ranges continue without a gap
   (AC6).
6. `OnReject.LOG` confirmed by sending one malformed record to a UAT topic.
7. Midnight: a new `Date` partition appears without a restart.
8. Data Merge for yesterday runs and `db.historical_table` returns the merged rows; validation
   passes; the intraday partition older than the window is deleted.

## 11. References (official pages the design relies on)

- Release notes for the installed line, Deephaven Enterprise 1.202601 (build 2026.01.054): `https://deephaven.io/enterprise/docs/` → Release Notes, Version 1.202601, and its detailed version log
- Ingest Kafka Data (Core+): `https://deephaven.io/enterprise/docs/data-guide/streaming/coreplus-kafka/`
- lastBy ingesters in a Core+ worker: `https://deephaven.io/enterprise/docs/data-guide/streaming/coreplus-lastby/`
- Data Import Server: `https://deephaven.io/enterprise/docs/data-guide/dis/`
- Data routing YAML: `https://deephaven.io/enterprise/docs/sys-admin/configuration/yaml/`
- Data Tailer: `https://deephaven.io/enterprise/docs/sys-admin/configuration/data-tailer/`
- Derived Table Writer: `https://deephaven.io/enterprise/docs/data-guide/streaming/coreplus-dtw/`
- User tables: `https://deephaven.io/enterprise/docs/deephaven-database/user-tables/`
- Input tables: `https://deephaven.io/enterprise/docs/deephaven-database/input-tables/`
- Data lifecycle (merge, validate, delete): `https://deephaven.io/enterprise/docs/sys-admin/architecture/data-lifecycle/`
- Pipeline and multi-thread your workload: `https://deephaven.io/enterprise/docs/performance/pipeline-your-workload/`
