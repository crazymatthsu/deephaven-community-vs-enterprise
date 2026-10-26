# 07 — Enterprise deployment

The persistent queries, their settings and the order to deploy them. Scripts are Git-sourced
as described in the sibling document
[`git-persistent-query-scripts.md`](../git-persistent-query-scripts.md); this document only
adds what is specific to order tracing.

---

## 1. Environments

| Environment | Platform | Purpose | Notes |
|---|---|---|---|
| dev-community | Deephaven Community in Podman (the fix42 repo's compose files) | the DAG, the oracle, the UI (doc 10 M1) | no Enterprise APIs; `OMS_TRACE_SOURCE=mock|files|kafka` |
| dev-enterprise | single-node Enterprise Podman on a Linux host | ingester + lineage end to end on captured samples | Windows/WSL2 Podman is unsupported and fragile (fix42 doc 17); use a Linux VM or a dev EKS namespace |
| uat | the production topology (EKS or multi-node Podman), UAT Kafka | AC5–AC8, the sizing day (doc 06 §3) | same PQ definitions as prod, lower heap |
| prod | Deephaven Enterprise **2026.01.054** (Q10) | | every environment runs the same release; `scripts/check_env.sh` refuses a mismatch |

## 2. Persistent query inventory

| PQ | Type | Dispatcher | Schedule | Heap (start) | Restart policy | Depends on | Script (Git path) |
|---|---|---|---|---|---|---|---|
| `oms-ingest-<x>-<group>` (one PQ per group of ≤ 4 topics; OMS B needs 2–8 PQs for its 7–8 topics, Q6) | Live Query - Merge Server | merge | Continuous; daily restart 00:05 `OMS_TRACE_TZ` (optional with `partition_func`) | 4 GB (8–16 GB with `LastByView`) | error restart attempts 5, delay 30 s | schemas, DIS `oms_ingest_<x>` | `queries/oms_trace/ingest.py` (`OMS_SYSTEM`, `OMS_TOPICS` env) |
| `oms-ingest-<x>-execs` (P2b) | Live Query - Merge Server | merge | as the order ingesters | 4 GB | as the order ingesters | DIS `oms_ingest_<x>` claiming `OMS_RAW.Execs<X>` | `queries/oms_trace/ingest.py` (`OMS_SYSTEM`, `OMS_FEED=execs`) |
| `oms-lineage` | Live Query (Script) | query | Continuous; daily restart 00:15 (rolls the `Date` window) | doc 06 §1.3 ÷ 0.7 (e.g. 96 GB expected case, string ids) | error restart attempts 3, delay 60 s; init timeout ≥ 2 × measured N6 | the three ingesters running (Dependent schedule or just "start after") | `queries/oms_trace/lineage.py` |
| `oms-drift-writer` | Live Query - Merge Server | merge | Continuous; daily restart 00:20 | 4 GB | restart attempts 5 | `oms-lineage` (Dependent), DIS `oms_trace` | `queries/oms_trace/drift_writer.groovy` (or `.py` if the DTW wrapper exists) |
| `oms-trace-ui` | Live Query (Script) | query | Continuous; daily restart 00:25 | 8–16 GB | restart attempts 5; 1 spare if the desk needs it | `oms-lineage` | `queries/oms_trace/ui.py` |
| `oms-eod-summary` | Batch Query (RunAndDone) | merge | Daily 23:55 | 4 GB | 1 retry | `oms-lineage` running | `queries/oms_trace/eod_summary.py` |
| `oms-merge-raw-<x>` | Data Merge | merge | Daily 01:00; partition value formula = previous day | per platform default | re-run with `force` on failure | ingester restarted (so yesterday's partition is closed) | n/a (type-defined) |
| `oms-validate-raw-<x>` | Data Validation | merge | Dependent on `oms-merge-raw-<x>` success | default | | | row-count validator; "Delete intraday data" enabled for partitions older than the window |
| shard PQs (P3 only) | Live Query (Script) | query | as `oms-lineage` | doc 06 §4.4 | | ingesters | `queries/oms_trace/lineage.py` with `OMS_TRACE_EDGE`, `_SHARD_COUNT`, `_SHARD_INDEX` |
| `oms-collector` (P3 only) | Live Query (Script) | query | Continuous | 8–16 GB | | shard PQs | `queries/oms_trace/collector.py` |

Type names (`Live Query (Script)`, `Live Query - Merge Server`, `Batch Query (RunAndDone)`,
`Data Merge`, `Data Validation`) are the Controller's built-in types; confirm the exact labels
in the installed version's Query Monitor.

## 3. Git repository layout for the scripts

```
oms-order-tracing/                     # this design's implementation repo (doc 10 §2)
├── queries/oms_trace/                 # iris.scripts.repo.<name>.paths = queries/oms_trace
│   ├── ingest.py                      # one script, OMS_SYSTEM selects the system
│   ├── lineage.py                     # app entry: builds the doc 04 DAG, exports globals
│   ├── drift_writer.groovy            # DTW binding (doc 05 §8.1)
│   ├── ui.py                          # deephaven.ui dashboard, subscribes to oms-lineage
│   ├── eod_summary.py                 # doc 05 §8.2
│   └── collector.py                   # P3
├── src/oms_trace/                     # the importable package the scripts use
├── schemas/                           # OMS_RAW.*.schema, OMS_TRACE.*.schema (doc 03)
├── routing/                           # dhconfig dis commands / routing.yml fragments (doc 05 §2)
├── pq/                                # dhconfig pq export XML of every PQ above, versioned
└── scripts/                           # deploy_schemas.sh, deploy_dis.sh, deploy_pqs.sh, check_env.sh
```

The scripts import `oms_trace` through the controller's module path mapping (the sibling doc's
"Modularizing Scripts"). A PQ **does not** pick up a new commit until it restarts; the daily
restarts in §2 are the normal rollout, and `dhconfig pq restart` the immediate one.

## 4. Cross-PQ table access

`oms-trace-ui`, `oms-drift-writer` and the P3 collector subscribe to tables exported by
`oms-lineage` with the Core+ Python client **inside the worker**:

```python
# in oms-trace-ui (Core+ Python)
from deephaven_enterprise.client.session_manager import SessionManager   # module path per installed version

sm = SessionManager(os.environ["DH_CONNECTION_JSON_URL"])                 # the cluster's connection.json
sm.private_key(os.environ["DH_UI_KEYFILE"])                                # a service account's key; never a password in a script
pq = sm.connect_to_persistent_query("oms-lineage")                         # the PQ must be running
drift_by_symbol = pq.barrage_subscribe(pq.open_table("drift_by_symbol"))   # a live local copy (in-worker use only)
unresolved_links = pq.barrage_subscribe(pq.open_table("unresolved_links"))
```

Rules that follow from how Barrage works (doc 00 F4): subscribe only to tables that are small
by construction (aggregates, breaks, a filtered slice); the subscriber holds a full copy and the
producer builds a snapshot per subscriber start. Never subscribe to `orders_*`.

A `pq://` URI resolver or a `remote_table` helper may exist in the installed version; if so it
is sugar over the same mechanism and the same rules apply.

## 5. Namespaces, ACLs and service accounts

| Group | `OMS_RAW` | `OMS_TRACE` | PQs |
|---|---|---|---|
| `oms-trace-svc` (service account running the PQs) | read; the ingesters' DISes write | write via merge dispatcher | owner of all `oms-*` PQs |
| `oms-trace-ops` | read | read | admin on `oms-*` |
| `oms-desk-a`, `oms-desk-b`, `oms-desk-c` | none | read | viewer of `oms-trace-ui` |
| everyone else | none | none | none |

- Writes happen only from merge-dispatcher workers (ingesters, drift writer, EOD summary); the
  lineage and UI PQs run on query dispatchers as `dbquery`-class workers with read access.
- ACLs are managed with the ACL editor or `dhconfig acl`; keep the exported ACL definitions in
  `pq/acl/` next to the PQ XML.
- Kafka credentials and the UI service account's private key come from the environment's
  secret store (Kubernetes secrets on EKS; the host-mounted secrets directory on Podman), never
  from the script or the PQ configuration.

## 6. Worker settings

- **Heap**: from doc 06 §1.3; the Controller's `PersistentQueryController.defaultMaxHeapSizeGB`
  and the dispatcher's `RemoteQueryDispatcher.maxPerWorkerHeapMB` / `maxTotalQueryProcessorHeapMB`
  must allow the lineage PQ's allocation on its node. `Webapi.persistentquery.heap.default` is
  only the UI default (4 GB).
- **Time zone**: the lineage and ingester workers must run with `OMS_TRACE_TZ` for `Date`
  assignment and the daily window (`-Duser.timezone=` in the PQ's JVM arguments, or compute the
  dates with an explicit zone in `date_window`).
- **JVM args**: `-XX:+UseG1GC` (default) is fine; set `-XX:MaxGCPauseMillis=200` if cycle
  time shows GC pauses. No direct-memory setting is needed for Core+ (doc 00 F6).
- **Python**: the worker image or venv must carry the `oms_trace` package's dependencies (none
  beyond `deephaven` for the lineage; `deephaven.ui` for the UI PQ). Build worker environments
  from the release's pinned file (`frozen-requirements-2026.01.0xx-42.5.txt` for the 1.202601
  line: Core+ 2026.01.0xx on Community Core 42.5), never from floating versions; the
  `deephaven_enterprise` package version must match the cluster build (2026.01.054).

## 7. Scheduling details

- **Daily restart times** are staggered (ingesters first, lineage, writer, UI) so the lineage
  PQ restarts after the ingesters have reopened and the writer after the lineage. Each restart
  re-evaluates date-sensitive code, which the sibling doc flags as the main reason to restart
  at all.
- **Dependent** schedules: the validation PQ depends on its merge PQ; the drift writer and UI
  depend on the lineage PQ so that a controller restart brings them up in order.
- **Merge timing**: the Data Merge of day D runs after the ingesters' daily restart has rolled
  the partition to D + 1, so D is closed; validation then deletes intraday D − WINDOW.
- **Initialization timeout** on `oms-lineage` must exceed the measured recovery time (doc 06
  §8) with margin, or the controller will kill a healthy start-up.

## 8. Configuration reference

All keys are environment variables on the PQ (set in the PQ's "Extra environment" / JVM
properties) or `oms_trace.config` defaults.

| Key | Default | PQs | Meaning |
|---|---|---|---|
| `OMS_SYSTEM` | — | ingesters | `A`, `B`, `C` |
| `OMS_TOPICS` | — | ingesters | this PQ's comma-separated topics (its share of `OMS_TOPICS_<X>`) |
| `OMS_TOPICS_A` / `_B` / `_C` | — | lineage | every topic per system (doc 03 §8), for the fallback table layout |
| `OMS_TRACE_ONE_TABLE_PER_SYSTEM` | `true` | ingesters, lineage | `true`: every topic's ingester writes `Orders<X>` (internal partitions); `false`: one table per topic (doc 05 §4.3b) |
| `KAFKA_BOOTSTRAP` and `KAFKA_*` security props | — | ingesters | from secrets |
| `OMS_TRACE_TZ` | `America/New_York` | all | `Date` assignment, daily window |
| `OMS_TRACE_RAW_NAMESPACE` / `OMS_TRACE_OUT_NAMESPACE` | `OMS_RAW` / `OMS_TRACE` | lineage, writer, EOD | |
| `OMS_TRACE_DATE_WINDOW_DAYS` | `2` | lineage | partitions read (plus tomorrow) |
| `OMS_TRACE_GAP_AGE_SEC` | `30` | lineage | `TRANSIENT` boundary |
| `OMS_TRACE_UNRESOLVED_ALERT_SEC` | `3600` | lineage | unresolved alert |
| `OMS_TRACE_CLOCK_SEC` | `30` | lineage | aging clock period |
| `OMS_TRACE_QTY_TOL` | `0` | lineage | tolerance |
| `OMS_TRACE_ALERT_URL` | — | lineage | HTTP sink for threshold crossings (Q9) |
| `OMS_TRACE_EXEC_PX_SCALE` / `_EXEC_TIME_BUCKET_MS` / `_EXEC_SCOPE` | `1000000` / `1` / `aged_gaps` | lineage | execution match precision and scope (doc 03 §8) |
| `OMS_TRACE_SHARD_COUNT` / `_SHARD_INDEX` / `_EDGE` | `1` / `0` / — | shard PQs | P3 |
| `DH_CONNECTION_JSON_URL`, `DH_UI_KEYFILE` | — | UI, writer, collector | Core+ client access to `oms-lineage` |

A malformed value is a start-up error (as the fix42 modules do), never a silent default.

## 9. Deployment order (first install and every environment)

1. `scripts/check_env.sh`: Enterprise version (expected 2026.01.054, Q10) and the matching Core+ Python package version, dispatcher heap caps,
   Kafka reachability from the merge nodes, storage mounted on merge and query nodes.
2. `scripts/deploy_schemas.sh`: `dhconfig schema import` for `OMS_RAW.*` and `OMS_TRACE.*`.
3. `scripts/deploy_dis.sh`: `dhconfig dis add` ×4 (doc 05 §2); validate routing; confirm the
   TDS sees them from a console (`db.live_table("OMS_RAW","OrdersA")` returns an empty table,
   not an error).
4. ACLs for the groups in §5.
5. Git repo configured on the controller (`iris.scripts.repo.oms_trace.*`), `dhconfig pq
   check-repo`.
6. `dhconfig pq import pq/*.xml` (or create the PQs in the Query Monitor the first time and
   export them); start the ingesters; verify partitions appear.
7. Start `oms-lineage`; verify `trace_stats`, then `oms-drift-writer`, `oms-trace-ui`.
8. Create the merge/validation PQs; run one merge manually for a closed day and confirm with
   `db.historical_table`.
9. Hand over with doc 08's dashboards and runbooks.

## 10. Upgrades and rollback

- A script change is a Git commit; it takes effect at the next (daily or manual) PQ restart.
  Rolling back is reverting the commit and restarting.
- A schema change (new column) is additive only: `dhconfig schema` update plus an ingester
  restart; the adapter view tolerates missing columns by not referencing them. Removing or
  retyping a column means a new table name.
- A heap or schedule change is a PQ edit; keep `pq/*.xml` in sync (`dhconfig pq export`).
- Platform upgrades: re-run `scripts/check_env.sh`; the DAG is Community-Core operations and
  is the least version-sensitive part; the ingester and client import paths are the most, which
  is why they are isolated in `ingest.py` and `oms_trace.enterprise`. The design is written
  against 2026.01.054; a release change re-runs M2's API confirmation before anything else.
