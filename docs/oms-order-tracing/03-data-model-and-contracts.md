# 03 — Data model and contracts

Table names, column names, types and formulas for the design in doc 02, for the five-level
topology of doc 01 §1. This document is **binding** for the implementation (doc 10): a
deviation updates it in the same change. Naming follows Enterprise conventions
(`Namespace.TableName`, PascalCase columns) for persisted tables, and snake_case Python globals
for in-worker tables.

---

## 1. Namespaces

| Namespace | Kind | Written by | Read by | Holds |
|---|---|---|---|---|
| `OMS_RAW` | system namespace | the three ingester PQs (each DIS claims its table) | `oms-lineage`, ops, forensics | `OrdersA`, `OrdersB`, `OrdersC` (intraday → historical) |
| `OMS_TRACE` | system namespace | `oms-drift-writer` (DTW) and the EOD snapshot | desks, ops, reports | `DriftEvents`, `DailyDriftSummary` |
| `oms_trace_dev` | user namespace | developers | developers | scratch snapshots during P0/P1 |

## 2. Raw per-system tables — `OMS_RAW.OrdersA`, `OrdersB`, `OrdersC`

One row per message as received (append-only within a partition). The same schema for all
three; `OrdersB` and `OrdersC` hold **both** parent and child orders, told apart by
`ParentOrderId` (A11, Q15). A system publishes on several topics (Q6, up to 8). The target is
one table per system with each topic's ingester writing its own **internal partition**; if the
installed release's Kafka ingester cannot do that, the fallback is one table per topic
(`OrdersB_<topic>`) with the same schema, merged in the adapter (doc 05 §4.3b). Either way the
`Topic` column records provenance.

| Column | Type | Null | Meaning |
|---|---|---|---|
| `Date` | String (partitioning) | no | trading date the ingester assigned from `RecvTs` (A10) |
| `OrderId` | String | no | the system's own order id, unique across parents and children (A1) |
| `UpstreamRef` | String | A: null; B parent: the A id; C parent: the B child id; **children: null** (Q16, confirmed 2026-10-26) | the **cross-system** reference, carried by parents only |
| `ParentOrderId` | String | parents: **null**; children: the parent's `OrderId` in the same system (Q15, confirmed 2026-10-26) | the **in-system** reference; non-null makes the row a child |
| `Symbol` | String | no | as sent; one coding across the three systems (Q13) |
| `Side` | String | no | as sent; one coding across the three systems (Q13) |
| `OrderQty` | long | no | order quantity |
| `CumQty` | long | no | cumulative executed quantity — the value reconciled |
| `LeavesQty` | long | no | open quantity — available in all three systems (Q14); reconciled as `RouteGap` |
| `AvgPx` | double | yes | optional |
| `OrdStatus` | String | yes | the system's status |
| `EventTs` | Instant | yes | the OMS's event time (FIX tag 60, GMT in all three systems and comparable, Q12) |
| `RecvTs` | Instant | no | the ingester's receive time; used for aging (Q12) |
| `SeqNo` | long | yes | a per-order sequence from the feed, if any (A8) |
| `Topic` | String | no | the source topic (Q6: several per system) |
| `KafkaPartition` | int | no | provenance |
| `KafkaOffset` | long | no | provenance |

Enterprise schema sketch (one per table; confirm attribute names against the installed
version's schema reference):

```xml
<Table namespace="OMS_RAW" name="OrdersB" storageType="NestedPartitionedOnDisk" defaultMergeFormat="DeephavenV1">
  <Partitions keyFormula="${autobalance_single}"/>
  <Column name="Date"           dataType="String"  columnType="Partitioning"/>
  <Column name="OrderId"        dataType="String"  columnType="Grouping"/>
  <Column name="UpstreamRef"    dataType="String"/>
  <Column name="ParentOrderId"  dataType="String"/>
  <Column name="Symbol"         dataType="String"/>
  <Column name="Side"           dataType="String"/>
  <Column name="OrderQty"       dataType="long"/>
  <Column name="CumQty"         dataType="long"/>
  <Column name="LeavesQty"      dataType="long"/>
  <Column name="AvgPx"          dataType="double"/>
  <Column name="OrdStatus"      dataType="String"/>
  <Column name="EventTs"        dataType="java.time.Instant"/>
  <Column name="RecvTs"         dataType="java.time.Instant"/>
  <Column name="SeqNo"          dataType="long"/>
  <Column name="Topic"          dataType="String"/>
  <Column name="KafkaPartition" dataType="int"/>
  <Column name="KafkaOffset"    dataType="long"/>
</Table>
```

Parents and children of B and of C arrive together on each of the system's topics (Q18), parents first, and
within a system the parent may be assumed to arrive before its children (Q7). The topic is keyed
by order id (Q11), so Kafka keeps that order per partition only; the design does not depend on it
either way: a child that arrives first is `UNRESOLVED` until its parent lands (A15).

### 2.1 Raw execution tables — `OMS_RAW.ExecsA`, `ExecsB`, `ExecsC` (P2b)

One row per execution report as received. Same schema for all three systems.

| Column | Type | Null | Meaning |
|---|---|---|---|
| `Date` | String (partitioning) | no | assigned from `RecvTs` |
| `OrderId` | String (grouping) | no | the order in this system the execution belongs to (parent or child) |
| `ExecId` | String | no | this system's execution id; **not** comparable across systems (Q17) |
| `ExecType` | String | no | `FILL`, `BUST`, `CORRECT` (normalized from the feed's codes in the adapter) |
| `ExecRefId` | String | `FILL`: null; `BUST`/`CORRECT`: the `ExecId` it cancels or replaces | within this system (A14) |
| `LastQty` | long | no | executed quantity of this report |
| `LastPx` | double | no | execution price |
| `TransactTime` | Instant | no | the execution's time as the executing venue reported it — the same on every hop (Q17) |
| `EventTs`, `RecvTs` | Instant | `EventTs` yes, `RecvTs` no | as for orders |
| `Symbol`, `Side` | String | no | for display and sanity checks |
| `KafkaPartition`, `KafkaOffset` | int, long | no | provenance |

`OrderId` is a grouping column so the lazy `where_in` of doc 04 §10 can use the index on merged
data; the intraday cost is measured in M1 (doc 06 §1.5). The match key derived in the worker is
`(Qty = LastQty, PxKey = round(LastPx × PX_SCALE), TimeKey = lowerBin(TransactTime, TIME_BUCKET))`.

## 3. The adapter contract (canonical node columns)

The lineage DAG never reads `OMS_RAW` columns directly. One `where` + `view` per **level**
maps them onto this contract (doc 04 §1). It is the only code that changes when a feed changes.

| Column | Type | Rule |
|---|---|---|
| `Level` | String | `A_ROOT`, `B_PARENT`, `B_CHILD`, `C_PARENT`, `C_CHILD` (constant per adapter) |
| `System` | String | `A`, `B`, `C` |
| `OrderId` | String | as raw; never null |
| `LinkId` | String | the id of the **parent node**: `UpstreamRef` on `B_PARENT` and `C_PARENT`, `ParentOrderId` on `B_CHILD` and `C_CHILD`, `""` on `A_ROOT`; null → `""` |
| `Symbol`, `Side` | String | as sent (one coding across systems, Q13); null → `""` |
| `OrderQty`, `CumQty`, `LeavesQty` | long | null → `0` (`LeavesQty` is present in all three systems, Q14) |
| `Status` | String | `OrdStatus`, null → `""` |
| `RecvTs` | Instant | as raw |
| `Topic` | String | the drop-copy channel the state came from (Q6, Q7); null → `""` |
| `Present` | boolean | constant `true`; joined onto children to tell "parent exists" from "parent exists with null columns" |

Role split rule (A11, confirmed): `IS_PARENT = isNull(ParentOrderId)`. Null means parent; any
non-null value means child. An empty string is therefore a child with no link, which the DAG
reports as `NO_LINK` — a visible contract violation, never a silently misfiled parent. The
pre-cutover check (doc 09 §6) asserts that no row carries an empty `ParentOrderId`. Should a
feed turn out to send `""` for parents after all, the fix is the one-line relaxation doc 14 §3
uses (`|| ParentOrderId == ```), applied knowingly and recorded here.

## 4. In-worker lineage tables (`oms-lineage` globals)

### 4.1 Last state per level

`a_root`, `b_parent`, `b_child`, `c_parent`, `c_child`: `last_by(["OrderId"])` per raw table
over the `Date` window, then the role split and the adapter view. One row per node. Columns = §3.

### 4.2 Linked nodes — `links_bp`, `links_bc`, `links_cp`, `links_cc`

Each child level with its parent's state joined on and the root carried down. One row per node
of that level.

| Column | Source |
|---|---|
| §3 columns | the node |
| `ParentPresent` | parent `Present` (null when unresolved) |
| `ParentCumQty`, `ParentSymbol`, `ParentSide`, `ParentStatus`, `ParentRecvTs` | the parent |
| `ParentSystem` | the parent level's system (`A`, `B`, `B`, `C`), for the feed check on unresolved rows |
| `ParentState` | `LinkId == `` ? NO_LINK : isNull(ParentPresent) ? UNRESOLVED : LINKED` |
| `RootId` | `B_PARENT`: `LinkId` (the A id, known even before A arrives); other levels: the parent's `RootId` (null while any hop above is missing) |
| `RootState` | `isNull(RootId) ? UNRESOLVED : RESOLVED` |
| `UnresolvedSince` | `ParentState == UNRESOLVED ? RecvTs : null` |

`a_root` carries `RootId = OrderId`, `RootState = RESOLVED`, `ParentState = ROOT`.

### 4.3 Edges — `edge_a_bp`, `edge_bp_bc`, `edge_bc_cp`, `edge_cp_cc`

Parents with the rollup of their direct children. One row per parent node.

| Column | Formula |
|---|---|
| §3 columns, `RootId`, `RootState` | the parent (from its `links_*` table; `a_root` for the first edge) |
| `ChildCount` | `count` of children with `LinkId = parent OrderId` (null → 0) |
| `ChildCumQty` | `sum(CumQty)` over direct children (null → 0) |
| `ChildLastTs` | `max(RecvTs)` over direct children |
| `ChildTopics` | the distinct channels the direct children came from (a small vector per parent) |
| `ChildSystem` | the child level's system (`B`, `B`, `C`, `C`) |
| `FillGap` | `CumQty − ChildCumQty` (long) |
| `ChildLeavesQty` | `sum(LeavesQty)` over direct children (null → 0) |
| `RouteGap` | `LeavesQty − ChildLeavesQty` (long): positive = open here and not routed below, negative = more open below than here |
| `LastChangeTs` | `max(RecvTs, ChildLastTs)` |
| `Edge` | `A-Bp`, `Bp-Bc`, `Bc-Cp`, `Cp-Cc` |
| `Scope` | `CROSS` (`A-Bp`, `Bc-Cp`) or `INTERNAL` (`Bp-Bc`, `Cp-Cc`) |
| `ExpectOne` | `true` on `A-Bp` and `Bc-Cp` (A4) — configuration, not code |

A parent with no children yet has `ChildCount = 0` and `FillGap = CumQty`: it is a gap (the
downstream has nothing) and ages like any other.

### 4.4 Aged gaps — `gaps`

`merge([edge_*]).where("FillGap != 0 || RouteGap != 0 || (ExpectOne && ChildCount != 1)")` joined
with the clock (doc 04 §5):

| Column | Formula |
|---|---|
| `Now` | the clock's latest tick |
| `GapAgeSec` | `diffNanos(LastChangeTs, Now) / SECOND` |
| `BreakKind` | §6 |
| `IsDrift` | `BreakKind` is a red kind |
| `ShortSide` | `FillGap > 0 ? CHILD : PARENT` — which book is short |
| `MissedBy` | the system whose book is short: `CROSS` edges: `CHILD` → the child level's system, `PARENT` → the parent level's system; `INTERNAL` edges: the system itself (meaningful for the fill kinds) |
| `OpenAt` | `RouteGap > 0 ? PARENT : RouteGap < 0 ? CHILD : ""` — where the unmatched open quantity sits (meaningful for the route kinds) |
| `ParentFeed` | the parent's channel state from `feed_health` (`CURRENT` / `STALE` / `CATCHING_UP`) |
| `ChildSideStale` | any child channel non-current; with no children yet, the child system has any non-current channel |
| `FeedStale` | `ParentFeed != CURRENT || ChildSideStale` — the gap is suppressed (R17) |

### 4.5 Outputs

| Table | Grain | Columns |
|---|---|---|
| `drift_edges` | one row per parent with an aged break | `Edge`, `Scope`, `Level`, `System`, `OrderId`, `RootId`, `RootState`, `Symbol`, `Side`, `Status`, `OrderQty`, `CumQty`, `ChildCount`, `ChildCumQty`, `FillGap`, `LeavesQty`, `ChildLeavesQty`, `RouteGap`, `GapAgeSec`, `BreakKind`, `ShortSide`, `MissedBy`, `OpenAt` |
| `drift_by_a_order` | one row per root with any aged break below it | `RootId`, `Symbol`, `Side`, `ACumQty`, `StreetCumQty` (Σ `C_CHILD` cum under the root), `GapABp`, `SumGapBpBc`, `SumGapBcCp`, `SumGapCpCc`, `MissingAtB = max(GapABp, 0)`, `UnbookedAtA = max(−GapABp, 0)`, `InternalB = SumGapBpBc` (signed), `MissingAtC = max(SumGapBcCp, 0)`, `UnbookedAtB = max(−SumGapBcCp, 0)`, `InternalC = SumGapCpCc` (signed), `ALeavesQty`, `StreetLeavesQty` (Σ `C_CHILD` leaves under the root), `HeldQty = Σ max(RouteGap, 0)`, `OverRoutedQty = Σ max(−RouteGap, 0)` over the root's edges, `Breaks` |
| `drift_by_symbol` | one row per `Symbol`, `Side`, `Edge` | `Breaks`, `ParentCumQty`, `ChildCumQty`, `MissingDownstream = Σ max(FillGap, 0)`, `UnbookedUpstream = Σ max(−FillGap, 0)`, `OverRouted = Σ max(−RouteGap, 0)` |
| `feed_health` | one row per system and topic | `System`, `Topic`, `LastRecvTs`, `LastEventTs`, `SilentSec`, `BehindSec`, `TopicState` (`CURRENT` / `STALE` / `CATCHING_UP`) — the channel view the alert sink pages on (one alert per topic, R17) |
| `feed_suppressed` | gaps with `BreakKind = FEED_STALE` | as `drift_edges`, plus `ParentFeed`, `ChildSideStale` — shown and counted, never paged |
| `unresolved_links` | one row per node with `ParentState != LINKED` | `Level`, `System`, `OrderId`, `LinkId`, `ParentState`, `Symbol`, `Side`, `CumQty`, `Status`, `UnresolvedSince`, `AgeSec`, `Alerted = AgeSec >= threshold` |
| `root_unresolved` | nodes `LINKED` but `RootState = UNRESOLVED` | `Level`, `System`, `OrderId`, `LinkId`, `CumQty` — informational; the alert is on the unresolved node above |
| `alerts` | append-only in the worker (ring-capped), one row per threshold crossing | `Ts`, `Metric` (doc 08 §2 row name), `Key` (system, edge, symbol or node as applicable), `Value`, `Threshold`, `Severity` (`WARN`/`PAGE`), `Message`, `Posted` (HTTP sink result); also written as a `break_events` row so it lands in `DriftEvents` (Q9) |
| `break_events` | **blink**: one row per node whose `BreakKind` or `ParentState` changed this cycle | `Ts`, `Level`, `System`, `OrderId`, `RootId`, `Edge`, `From`, `To`, `FillGap`, `GapAgeSec` |
| `trace_stats` | one row | per level: `Nodes`, `Unresolved`, `UnresolvedAlerted`, `MaxUnresolvedAgeSec`, `LastUpdate`; per edge: `Gaps`, `Drift`, `MissingDownstreamQty`, `UnbookedUpstreamQty`; `BChildren`, `CParents`, `CParentsMinusBChildren` (the 1 : 1 check as a count); `WindowDates`; cycle time |
| `exec_breaks` (P2b) | one row per `Edge`, broken parent and `(Qty, Px, TimeKey)` with a non-zero count difference | `Edge`, `ParentId`, `RootId`, `Qty`, `Px`, `TimeKey`, `ParentCount`, `ChildCount`, `Diff = ChildCount − ParentCount`, `Kind` (`EXEC_MISSING_DOWNSTREAM` when the parent has more, `EXEC_UNBOOKED_UPSTREAM` when the children have more), `DroppedQty = |Diff| × Qty`, `DroppedExecIds` (the exec ids on the side with more), `ChildOrderIds`, `Ambiguous` (k of n identical fills) |
| `exec_breaks_by_edge` (P2b) | one row per `Edge`, broken parent | `RootId`, `FillGap`, `ExplainedGap = Σ (−Diff × Qty)`, `Unexplained = FillGap − ExplainedGap`, `Keys`, `AmbiguousKeys` |
| `trace_nodes` | UI only, over a **selection**: one row per node of the selected roots, all five levels | §3 columns + `NodeKey`, `ParentKey`, `RootId`, `Depth`, `FillGap`, `BreakKind` |
| `trace_tree` | `trace_nodes.tree("NodeKey", "ParentKey", promote_orphans=True)` | |

## 5. Persisted derived tables — `OMS_TRACE`

### 5.1 `DriftEvents` (append-only, via the Derived Table Writer from `break_events`)

| Column | Type |
|---|---|
| `Date` | String (partitioning) |
| `Ts` | Instant |
| `Level`, `System`, `OrderId`, `RootId`, `Edge` | String |
| `From`, `To` | String (the `BreakKind` / `ParentState` before and after) |
| `FillGap` | long |
| `GapAgeSec` | long |

### 5.2 `DailyDriftSummary` (one partition per day, written at EOD by `add_table_partition`)

`drift_by_symbol`, `drift_by_a_order` and `trace_stats` as of the lineage PQ's last cycle
before its daily restart, with a `Kind` column.

## 6. Break taxonomy and attribution

Evaluated per parent row (edges) and per child row (links), first match wins. Red kinds are
drift; amber are structural or informational.

| `BreakKind` | Applies to | Condition | Meaning / attribution | Colour |
|---|---|---|---|---|
| `FEED_STALE` | parent | a channel on either side of the edge is `STALE` or `CATCHING_UP` | the comparison is not meaningful yet: a system or channel is down or replaying a backlog (Q7). Shown in `feed_suppressed`, counted in `trace_stats`; the alert is the channel's, not the gap's (R17). Evaluated first | amber |
| `NO_LINK` | child | `LinkId == ``` on a non-root level | the parent id was never stamped; the node's quantity is unattributable | red |
| `UNRESOLVED` | child | `LinkId` set, no parent found | parent not arrived (R8) or never will (R9); alert on age | amber → red at alert age |
| `ROOT_UNRESOLVED` | child | `LINKED` but `RootId` null | a hop above is missing; shown, not alerted here | amber |
| `MISMATCH` | child | `Symbol != ParentSymbol || Side != ParentSide` | a child working the wrong instrument or direction | red |
| `OVERFILL` | any | `CumQty > OrderQty` | executed beyond order size in that book | red |
| `CARDINALITY` | parent on an `ExpectOne` edge | `ChildCount > 1`, aged | two B parents for one A, or two C parents for one B child: a re-keyed or duplicated downstream order (R13) | red |
| `MISSING_CHILD` | parent on an `ExpectOne` edge | `ChildCount == 0`, aged, `CumQty == 0` | the downstream never created its order; no quantity gap yet | amber |
| `TRANSIENT` | parent | `FillGap != 0` and `GapAgeSec < AGE_SEC` | propagation lag, not drift (R6) | none |
| `MISSING_DOWNSTREAM` | parent | `FillGap > 0`, aged | the children's book is short by `FillGap`: on `A-Bp` → **B dropped executions**; `Bp-Bc` → **B's child book is short against its own parent**; `Bc-Cp` → **C dropped executions**; `Cp-Cc` → **C's child book is short** | red |
| `UNBOOKED_UPSTREAM` | parent | `FillGap < 0`, aged | the parent's book is short: `A-Bp` → **A has not booked** what B shows; `Bp-Bc` → **B's parent book lags its children**; `Bc-Cp` → **B has not booked** what C shows; `Cp-Cc` → **C's parent book lags** | red |
| `LEAVES_MISMATCH` | parent on an `ExpectOne` edge | `RouteGap != 0`, aged, no fill break | the two books of a 1 : 1 hop disagree on open quantity (a cancel or replace seen on one side only) | red |
| `CHILD_STILL_OPEN` | parent on a 1 : n edge | `RouteGap < 0 && LeavesQty == 0`, aged | the parent is done or canceled but a child is still working: **the cancel did not propagate** | red |
| `OVER_ROUTED` | parent on a 1 : n edge | `RouteGap < 0`, aged | more open below than above: overfill risk | red |
| `HELD` | parent on a 1 : n edge | `RouteGap > 0`, aged | open here, not routed further (unsliced quantity at B, unrouted at C) | amber |
| `NONE` | any | | reconciles | — |

The identities that make the rollups add up (doc 14 §5.3 proves the same telescoping; unit
tested in doc 09): for a root whose every node is linked,

```
ACumQty − StreetCumQty = GapABp + Σ GapBpBc + Σ GapBcCp + Σ GapCpCc
MissingAtB − UnbookedAtA = GapABp        MissingAtC − UnbookedAtB = Σ GapBcCp
InternalB = Σ GapBpBc                    InternalC = Σ GapCpCc
ALeavesQty − StreetLeavesQty = Σ RouteGap over the four edges = HeldQty − OverRoutedQty
```

So `drift_by_a_order` is the source document's "the number system A needs to see", with the
system **and the book** named.

**Execution-level kinds (P2b, `exec_breaks`).** Evaluated per broken parent and match key:

| `Kind` | Condition | Meaning |
|---|---|---|
| `EXEC_MISSING_DOWNSTREAM` | `ParentCount > ChildCount` | the parent's book has an execution the children's book does not: **the children's system dropped it**; `DroppedExecIds` are the parent's exec ids |
| `EXEC_UNBOOKED_UPSTREAM` | `ChildCount > ParentCount` | the children executed something the parent never booked; `DroppedExecIds` are the children's exec ids |
| `EXEC_UNEXPLAINED` (on `exec_breaks_by_edge`) | `Unexplained != 0` | the quantity gap is not accounted for by whole missing executions: quantities, prices or times disagree across the hop, or a bust was netted on one side only |

`Ambiguous` is set when fewer than all of the identical fills for a key are missing: the row
names every candidate, which is the most the key can say.

## 7. Keys, types and null rules

- **Ids stay in their native column type.** Joins use `LinkId = OrderId` per edge; no
  concatenated `NodeKey` outside `trace_nodes` (D8). `OrderId` and the parent references are the
  orders' **root ids**, invariant under cancel/replace (Q4), so one id per node is enough and no
  id-history index is kept.
- **Quantities are `long`.** Gaps are exact; `QTY_TOL` exists only for fractional feeds.
- **Nulls** are removed at the adapter (§3). A null parent column after a join means "no
  parent", never "parent with null value", because `ParentPresent` is checked first.
- **Time**: `EventTs` (FIX tag 60) is GMT in all three systems and comparable (Q12). Aging uses
  `RecvTs` by default (`OMS_TRACE_AGE_CLOCK=recv`): a republished old report would age instantly
  under event time. `OMS_TRACE_AGE_CLOCK=event` switches the gap and unresolved ages to
  `EventTs`, which measures how long the books have disagreed in market time rather than in
  our observation time. The execution match key always uses event time (`TransactTime`).
- **`Date`** is assigned by the ingester from `RecvTs` in the configured zone.

## 8. Configuration keys referenced by these tables

| Key | Default | Used by |
|---|---|---|
| `OMS_TRACE_RAW_NAMESPACE` / `OMS_TRACE_OUT_NAMESPACE` | `OMS_RAW` / `OMS_TRACE` | adapters, writer |
| `OMS_TRACE_DATE_WINDOW_DAYS` | `2` | §4.1 window (plus tomorrow) |
| `OMS_TRACE_GAP_AGE_SEC` | `30` | `TRANSIENT` boundary |
| `OMS_TRACE_UNRESOLVED_ALERT_SEC` | `3600` | `unresolved_links.Alerted` |
| `OMS_TRACE_CLOCK_SEC` | `30` | the aging clock period |
| `OMS_TRACE_QTY_TOL` | `0` | tolerance |
| `OMS_TRACE_EXPECT_ONE_EDGES` | `A-Bp,Bc-Cp` | the 1 : 1 edges (A4) |
| `OMS_TRACE_PARENT_ID_COLUMN` | `ParentOrderId` | the in-system parent id column (Q15, confirmed) |
| `OMS_TOPICS_A` / `_B` / `_C` | — | comma-separated topic list per system (Q6); each topic maps to an ingester and, in the fallback layout, to a raw table `Orders<X>_<topic>` |
| `OMS_TRACE_ONE_TABLE_PER_SYSTEM` | `true` | `true`: one raw table per system (internal partitions); `false`: one per topic, merged in the adapter |
| `OMS_TRACE_UI_MAX_ROOTS` | `20` | the most roots one dashboard selection expands into a tree (Q2: a root can hold a few hundred B children) |
| `OMS_TRACE_TZ` | `America/New_York` | `Date` assignment and the daily window |
| `OMS_TRACE_FEED_STALE_SEC` | `120` | a topic silent for longer is `STALE` (during market hours; a quiet channel needs a producer heartbeat or a longer threshold) |
| `OMS_TRACE_FEED_CATCHUP_SEC` | `300` | a topic whose newest `EventTs` is further behind now is `CATCHING_UP` (Q12 makes event time comparable) |
| `OMS_TRACE_AGE_CLOCK` | `recv` | `recv` (receive time) or `event` (tag 60, GMT, Q12) as the clock the gap and unresolved ages are measured against |
| `OMS_TRACE_EXEC_PX_SCALE` | `1000000` | price precision for the execution match key (1e-6) |
| `OMS_TRACE_EXEC_TIME_BUCKET_MS` | `1` | transaction-time precision for the execution match key; set from the pre-cutover match-rate report |
| `OMS_TRACE_EXEC_SCOPE` | `aged_gaps` | `aged_gaps` (lazy, the default) or `all` (every edge; O(executions) heap, needs its own PQ — doc 06 §1.5) |
