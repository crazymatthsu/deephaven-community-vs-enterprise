# 10 — Implementation plan

Phased delivery of docs 02–09, with the module layout, what is reused from the
`deephaven-fix42-dashboard` repository, risks and the definition of done.

---

## 1. Milestones

| Milestone | Deliverables | Exit gate | Depends on |
|---|---|---|---|
| **M0 Decisions** | answers to doc 01 §8 Q1–Q8 and Q10–Q13; id type decided (A9); window and thresholds set | doc 01 updated; docs 03/06 numbers replaced where answers differ | OMS teams, Deephaven admins |
| **M1 Community prototype** | `oms_trace` package: `config`, `adapters`, `dag`, `reference`, `mockdata`, `app`; L1 + L2 tests; Community compose with mock and file sources; the 1 M-row sizing run | AC1–AC4 against the oracle; doc 06 §1.3 filled with measured bytes; `trace_check` run on a captured sample (AC7 dry run) | M0 (can start on defaults) |
| **M2 Enterprise ingest and lineage** | `OMS_RAW` schemas, DIS routing, `ingest.py`, `lineage.py` as PQs on dev-enterprise; Git repo wired to the controller; nightly merge/validation | L4 steps 1–3, 5, 6 on dev-enterprise | M1, platform access |
| **M3 Outputs** | `break_events` listener, `drift_writer`, `OMS_TRACE` schemas, EOD summary, `ui.py` dashboard, alert sink, ACLs | L4 step 4, 7; desks sign off on `MissedBy` attribution on real UAT data (R5) | M2 |
| **M4 Scale and hardening** | full-day UAT run at agreed volume; doc 06 gates evaluated; shard PQs and collector only if a gate trips; runbooks R1–R7 rehearsed; N6 measured | AC5, AC6, AC8; doc 08 R2 filled | M3 |
| **M5 Production cutover** | prod PQs, ACLs, dashboards, on-call handover; `trace_check` report attached | one week of prod with no unexplained alerts | M4 |

M1 can start immediately: nothing in it needs Enterprise or the final answers.

## 2. Repository and module layout

A new submodule `oms-order-tracing/` (Gradle-wrapped Python like the fix42 repo's
`order-tree-recon`, so `./gradlew :oms-order-tracing:check` runs the suite), or a standalone
repo with the same shape:

```
oms-order-tracing/
├── src/oms_trace/
│   ├── config.py          # env parsing (doc 07 §8), thresholds, topology constants; malformed = error
│   ├── adapters.py        # OMS_RAW -> canonical view per system (doc 03 §3); the only feed-specific code
│   ├── sources.py         # raw_table(system): enterprise (db.live_table) | mock | files | community kafka
│   ├── dag.py             # doc 04 §1–§6: last state, links, edges, aging, aggregates; returns {name: table}
│   ├── events.py          # break_events listener + TablePublisher (doc 04 §7)
│   ├── ui_slice.py        # lineage_nodes(root_ids) -> nodes, tree (doc 04 §7)
│   ├── dashboard.py       # deephaven.ui panels (doc 02 §4 step 8)
│   ├── alerts.py          # threshold listener -> HTTP sink (doc 08 §2)
│   ├── reference.py       # the pure-Python oracle (doc 09 §1)
│   ├── mockdata.py        # scenario catalog + random families (doc 09 §2)
│   ├── trace_check.py     # pre-cutover CLI (doc 09 §6)
│   ├── enterprise.py      # every Enterprise import in one place: ingester options, DIS lookup, Core+ client, DTW
│   └── app.py             # main(globals()): builds everything and exports the doc 03 §4 globals
├── queries/oms_trace/     # PQ entry scripts (doc 07 §3) — thin: import oms_trace, call app.main
├── schemas/               # Enterprise schema XML (doc 03 §2, §5)
├── routing/               # dhconfig dis commands, routing.yml fragment (doc 05 §2)
├── pq/                    # dhconfig pq export XML + ACL exports (doc 07)
├── scripts/               # deploy_*.sh, check_env.sh, uat_checks.sh, run_demo.sh (Community)
├── docker/                # Community compose for M1 (images via repos.env, per the fix42 repo's doc 15 rule)
├── tests/                 # L1 (pure), L2 (OMS_TRACE_DH_TEST=1)
└── README.md
```

`dag.py` imports only `deephaven`; `enterprise.py` is the only module that imports
`deephaven_enterprise`, so the Community prototype and the L2 tests never touch Enterprise APIs
and the version-sensitive surface is one file (doc 07 §10).

## 3. What is reused from `deephaven-fix42-dashboard`

| Piece | From | Use |
|---|---|---|
| Normalize → index → link → walk → rollup → recon DAG, taxonomy, `TreePath`, tree | `order-tree-recon/src/order_tree_recon/{dag,config,reference}.py` (doc 14) | the generic path of doc 04 §8 for the prototype and the UI slice; the production edge DAG is a specialization written in `oms_trace.dag` |
| Reference-oracle + embedded-engine test harness, `faulthandler` handling | `order-tree-recon/tests`, `run_tests.sh` | L1/L2 scaffolding |
| Pre-flight feed check pattern | `fixstate_check.py` | `trace_check.py` |
| `deephaven.ui` dashboard patterns (uncontrolled inputs, row-press pinning, breaks-only) | `order-tree-recon/dashboard.py` (doc 14 §7) | `dashboard.py` |
| Multi-server leaves/collector reasoning and `rx_leaf_stats` | `deephaven-remote-uri` (doc 10) | the P3 collector |
| Gradle wrapping of a pytest module, `repos.env` image/pip indirection, LF `.gitattributes` | `basket-oms-demo`, doc 15 | the module skeleton |
| Stateful fold for delta feeds, if Q3 says deltas | `deephaven-scripts/src/fix42cache` | an adapter stage before `last_by` |

## 4. Work breakdown (relative sizes; S < M < L)

| # | Task | Size | Milestone |
|---|---|---|---|
| 1 | `config`, `adapters`, `sources(mock)`, `mockdata` scenario catalog | M | M1 |
| 2 | `reference.py` oracle + L1 tests for every scenario and the identities | M | M1 |
| 3 | `dag.py` production edge DAG + L2 equality tests + live healing tests | L | M1 |
| 4 | Community compose, `app.py`, file/kafka sources, `run_demo.sh` | M | M1 |
| 5 | Sizing run and doc 06 update | S | M1 |
| 6 | `trace_check.py` | S | M1 |
| 7 | Schemas, DIS routing scripts, `ingest.py`, `enterprise.py` (ingester part) | M | M2 |
| 8 | `lineage.py` PQ, Git repo config, daily restart schedule, merge/validation PQs | M | M2 |
| 9 | `events.py`, `drift_writer`, `OMS_TRACE` schemas, EOD summary | M | M3 |
| 10 | `ui_slice.py`, `dashboard.py`, `ui.py` PQ with Core+ client subscription | L | M3 |
| 11 | `alerts.py`, thresholds, ACLs | S | M3 |
| 12 | UAT full-day run, gates, runbook rehearsal, N6 | M | M4 |
| 13 | Shard PQs + collector (only if a gate trips) | L | M4 |
| 14 | Prod rollout, handover | S | M5 |

## 5. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Q3 answered "deltas": the feeds are executions, not states | the DAG needs a fold per system before `last_by` | `fix42cache`-style fold as an adapter stage; the DAG is unchanged |
| String ids and the upper volume case together (doc 06 §1.3) | one worker does not fit | numeric surrogate ids at ingest; else edge sharding (designed, not built, until M4 says so) |
| Core+ API names differ from this design's (ingester options, Core+ client, DTW wrapper) | M2 slips | `enterprise.py` isolates them; M2's first task is to compile that file against the installed version |
| Windows/WSL2 Podman for Enterprise dev (fix42 doc 17) | dev-enterprise unstable | Linux VM or a dev EKS namespace |
| Lag beyond the window (Q7 wrong) | permanently unresolved rows | measured in M1 from captured samples; window is configuration |
| Desks disagree with attribution semantics (`MissedBy`) | rework of doc 03 §6 | M3 sign-off on real data before M4 |
| Barrage subscription of a large table from the UI | lineage worker pauses | doc 07 §4 rule; UI subscribes to slices only; code review checks subscribed tables' sizes |

## 6. Definition of done

- Docs 01–09 reflect every M0 answer; this plan's gates are met and recorded in doc 08 R2 and
  doc 06 §1.3.
- `oms-order-tracing` passes L1 and L2 in CI; L3 demo runs with one command; L4 checklist
  (doc 05 §10, doc 09 §7) executed on UAT with the results attached.
- `trace_check` report for the production feeds attached to the change record (AC7).
- All PQs defined in `pq/*.xml`, scripts in Git on the controller, ACLs exported, runbooks
  R1–R7 rehearsed once, alert routing confirmed with the on-call.
- The source document is marked superseded by this folder, with doc 00 as the record of why.
