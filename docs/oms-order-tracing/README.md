# OMS order tracing (A → B → C) on Deephaven Enterprise — design set

An architecture review of [`../oms-order-tracing-architecture.md`](../oms-order-tracing-architecture.md)
and the documents needed to design, build and run the system it describes: detect
cumulative-execution drift across three chained OMS platforms and name the system that missed
the shares.

Reading order:

| Doc | What it answers | Read it if you… |
|---|---|---|
| [00 Architecture review](00-architecture-review.md) | what in the original design to keep, change and decide, ranked, with the reasoning | want the verdict and the "why" in one place |
| [01 Requirements and scope](01-requirements-and-scope.md) | requirements R1–R14, non-functionals, assumptions, acceptance criteria, open questions Q1–Q14 | own the requirements or must answer the questions |
| [02 Target architecture](02-target-architecture.md) | the revised topology, components, data flow, phases, decision log | need the picture |
| [03 Data model and contracts](03-data-model-and-contracts.md) | namespaces, schemas, the adapter contract, every output table, the break taxonomy (binding) | write adapters or consume the outputs |
| [04 Lineage and drift DAG](04-lineage-and-drift-dag.md) | the Deephaven table graph with code: linking, rollups, aging, how a late parent heals | implement or review the engine logic |
| [05 Ingestion and persistence](05-ingestion-and-persistence.md) | Kafka → in-worker DIS ingesters, routing, schemas, merge/retention, persisting derived tables | run the data path |
| [06 Scaling and sharding](06-scaling-and-sharding.md) | sizing model, phase gates, edge sharding when needed | size the workers or decide when to shard |
| [07 Enterprise deployment](07-enterprise-deployment.md) | PQ inventory, Git scripts, ACLs, settings, deployment order | deploy it |
| [08 Operations and failure modes](08-operations-and-failure-modes.md) | failure table, alerts, runbooks, forensic queries | operate it |
| [09 Testing and validation](09-testing-and-validation.md) | oracle-based tests, scenario catalog, sizing run, pre-cutover validation | build the tests or sign off |
| [10 Implementation plan](10-implementation-plan.md) | milestones, module layout, reuse from `deephaven-fix42-dashboard`, risks, done | plan the work |

The topology (clarified 2026-10-25): A holds the root order; B holds one parent per root and
many children; C holds one parent per B child and many children under each — five levels, four
edges, the same shape as the fix42 repo's doc 14 module.

The one-paragraph summary: reconcile **per edge** on all four edges (parent versus the sum of
its direct children, in executed **and** open quantity), treat a child whose parent has not arrived as a **state** that heals when
the parent lands rather than a buffer to retry, ingest each feed through a **Core+ Kafka ingester with an
in-worker Data Import Server**, run the whole lineage in **one worker** until a measured gate
trips, and shard **per edge by the parent's id** when it does, with root ids flowing down
between shard families as partitioned maps. Executions that were dropped are named by matching
execution reports on quantity, price and transaction time (ids do not cross hops), computed only
for edges that already show an aged gap. The resolver, the Barrage-published mapping table and
the pending-buffer PQ of the original design are not needed.

Related: [`../git-persistent-query-scripts.md`](../git-persistent-query-scripts.md) (how the
PQ scripts get from Git to the controller) and the `deephaven-fix42-dashboard` repository's
docs 09, 10 and 14, whose order-tree reconciliation module is the prototype substrate.
