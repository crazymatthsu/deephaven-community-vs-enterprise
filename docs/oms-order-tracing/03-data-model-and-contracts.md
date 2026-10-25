# 03 — Data model and contracts

Table names, column names, types and formulas for the design in doc 02. This document is
**binding** for the implementation (doc 10): a deviation updates it in the same change.
Naming follows Enterprise conventions (`Namespace.TableName`, PascalCase columns) for persisted
tables, and snake_case Python globals for in-worker tables.

---

## 1. Namespaces

| Namespace | Kind | Written by | Read by | Holds |
|---|---|---|---|---|
| `OMS_RAW` | system namespace | the three ingester PQs (each DIS claims its table) | `oms-lineage`, ops, forensics | `OrdersA`, `OrdersB`, `OrdersC` (intraday → historical) |
| `OMS_TRACE` | system namespace | `oms-drift-writer` (DTW) and the EOD snapshot step | desks, ops, reports | `DriftEvents`, `DailyDriftSummary` |
| `oms_trace_dev` | user namespace | developers | developers | scratch snapshots during P0/P1 |

System-namespace schemas are deployed with `dhconfig schema import` and owned by the platform
team; the user namespace is created by the first `add_partitioned_table_schema` call (doc 05
§8).

## 2. Raw per-system tables — `OMS_RAW.OrdersA`, `OrdersB`, `OrdersC`

One row per message as received (append-only within a partition). Same schema for all three;
`UpstreamRef` is null on A.

| Column | Type | Null | Meaning |
|---|---|---|---|
| `Date` | String (partitioning) | no | trading date the ingester assigned from `RecvTs` (A10) |
| `OrderId` | String | no | the system's own order id (A1) |
| `UpstreamRef` | String | A: null; B, C: no | B → the A order id; C → the B order id |
| `Symbol` | String | no | as sent; normalized in the adapter view if A7 fails |
| `Side` | String | no | as sent (`BUY`/`SELL` or FIX codes); normalized in the adapter |
| `OrderQty` | long | no | order quantity |
| `CumQty` | long | no | cumulative executed quantity — the value reconciled |
| `LeavesQty` | long | yes | open quantity, when the feed has it (Q14) |
| `AvgPx` | double | yes | optional; for a later notional view |
| `OrdStatus` | String | yes | the system's status (needed to read a canceled parent) |
| `EventTs` | Instant | yes | the OMS's event time |
| `RecvTs` | Instant | no | the ingester's receive time; used for aging (Q12) |
| `SeqNo` | long | yes | a per-order sequence from the feed, if any (A8) |
| `KafkaPartition` | int | no | provenance |
| `KafkaOffset` | long | no | provenance; with `KafkaPartition` it identifies the message |

Enterprise schema sketch (one per table; confirm attribute names against the installed
version's schema reference and generate from a prototype `TableDefinition` where the tooling
allows):

```xml
<Table namespace="OMS_RAW" name="OrdersB" storageType="NestedPartitionedOnDisk" defaultMergeFormat="DeephavenV1">
  <Partitions keyFormula="${autobalance_single}"/>
  <Column name="Date"           dataType="String"  columnType="Partitioning"/>
  <Column name="OrderId"        dataType="String"  columnType="Grouping"/>
  <Column name="UpstreamRef"    dataType="String"/>
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
  <Column name="KafkaPartition" dataType="int"/>
  <Column name="KafkaOffset"    dataType="long"/>
</Table>
```

`OrderId` is a grouping column so historical point lookups ("show me this order's day") read
one group, not the partition.

## 3. The adapter contract (canonical node columns)

The lineage DAG never reads `OMS_RAW` columns directly; one `view` per system maps them onto
this contract (doc 04 §1). It is the only code that changes when a feed changes.

| Column | Type | Rule |
|---|---|---|
| `System` | String | `A`, `B` or `C` (constant per adapter) |
| `OrderId` | String | as raw; never null |
| `LinkId` | String | `UpstreamRef`; `""` at A and whenever raw is null (`isNull(x) ? `` : x`) |
| `Symbol`, `Side` | String | normalized to one coding; null → `""` |
| `OrderQty`, `CumQty` | long | null → `0` |
| `LeavesQty` | long | null → `0`; `HasLeaves = false` when the feed lacks it |
| `Status` | String | `OrdStatus`, null → `""` |
| `RecvTs` | Instant | as raw |
| `Present` | boolean | constant `true`; joined onto children to tell "parent exists" from "parent exists with null columns" |

Nulls are turned into zeros and empty strings here, once, because a null poisons every sum
above it and `x == ``` is false on null (doc 14 §3's rule).

## 4. In-worker lineage tables (`oms-lineage` globals)

### 4.1 Last state per system

`orders_a`, `orders_b`, `orders_c`: the adapter view of `last_by(["OrderId"])` over the `Date`
window. One row per order. Columns = §3.

### 4.2 Linked children — `links_b`, `links_c`

Children with their parent's state joined on. One row per B (resp. C) order.

| Column | Source |
|---|---|
| §3 columns | the child |
| `ParentPresent` | parent `Present` (null when unresolved) |
| `ParentCumQty`, `ParentSymbol`, `ParentSide`, `ParentStatus`, `ParentRecvTs` | the parent |
| `RootId` | B: `LinkId`; C: parent B's `LinkId` (the A id), null while unresolved |
| `ParentState` | `LinkId == `` ? NO_LINK : isNull(ParentPresent) ? UNRESOLVED : LINKED` |
| `UnresolvedSince` | `ParentState == UNRESOLVED ? RecvTs : null` — the child's own first receive time is the earliest the gap was knowable |

### 4.3 Edges — `edge_ab`, `edge_bc`

Parents with the rollup of their direct children. One row per A (resp. B) order.

| Column | Formula |
|---|---|
| §3 columns | the parent |
| `ChildCount` | `count` of children by `LinkId = parent OrderId` (null → 0) |
| `ChildCumQty` | `sum(CumQty)` over direct children (null → 0) |
| `ChildLastTs` | `max(RecvTs)` over direct children |
| `FillGap` | `CumQty − ChildCumQty` (long) |
| `LastChangeTs` | `max(RecvTs, ChildLastTs)` |
| `Edge` | constant `AB` or `BC` |
| `RootId` | AB: `OrderId`; BC: `LinkId` (the A id) |

A parent with no children yet has `ChildCount = 0` and `FillGap = CumQty`: it is a gap
(the downstream has nothing) and ages like any other.

### 4.4 Aged gaps — `gaps`

`merge([edge_ab, edge_bc]).where("FillGap != 0")` joined with the clock (doc 04 §5):

| Column | Formula |
|---|---|
| `Now` | the clock's latest tick |
| `GapAgeSec` | `diffNanos(LastChangeTs, Now) / SECOND` |
| `BreakKind` | §6 |
| `IsDrift` | `BreakKind` is a red kind |

### 4.5 Outputs

| Table | Grain | Columns |
|---|---|---|
| `drift_edges` | one row per parent with an aged non-zero gap | `Edge`, `System`, `OrderId`, `RootId`, `Symbol`, `Side`, `Status`, `OrderQty`, `CumQty`, `ChildCount`, `ChildCumQty`, `FillGap`, `GapAgeSec`, `BreakKind`, `MissedBy` |
| `drift_by_a_order` | one row per root A order with any aged gap below it | `RootId`, `Symbol`, `Side`, `ACumQty`, `SumBCumQty`, `SumCCumQty`, `GapAB`, `SumGapBC`, `MissingAtB = max(GapAB, 0)`, `MissingAtC = max(SumGapBC, 0)`, `UnbookedAtA = max(−GapAB, 0)`, `UnbookedAtB = max(−SumGapBC, 0)`, `Breaks` |
| `drift_by_symbol` | one row per `Symbol`, `Side`, `Edge` | `Breaks` (parents with aged drift), `ParentCumQty`, `ChildCumQty`, `MissingDownstream = Σ max(FillGap, 0)`, `UnbookedUpstream = Σ max(−FillGap, 0)` |
| `unresolved_links` | one row per child with `ParentState != LINKED` | `System`, `OrderId`, `LinkId`, `ParentState`, `Symbol`, `Side`, `CumQty`, `Status`, `UnresolvedSince`, `AgeSec`, `Alerted = AgeSec >= threshold` |
| `break_events` | **blink**: one row per node whose `BreakKind` or `ParentState` changed this cycle | `Ts`, `System`, `OrderId`, `RootId`, `Edge`, `From`, `To`, `FillGap`, `GapAgeSec` |
| `trace_stats` | one row | per system: `Orders`, `Unresolved`, `UnresolvedAlerted`, `MaxUnresolvedAgeSec`; per edge: `Gaps`, `Drift`, `MissingDownstreamQty`, `UnbookedUpstreamQty`; `LastUpdate`, `WindowDates` |
| `trace_nodes` | UI only, over a **selection**: one row per node of the selected lineages | §3 columns + `NodeKey`, `ParentKey`, `RootId`, `Depth`, `FillGap`, `BreakKind` |
| `trace_tree` | `trace_nodes.tree("NodeKey", "ParentKey", promote_orphans=True)` | |

`MissedBy` names the system the shares are missing from: on edge AB a positive gap → `B`, on
BC → `C`; a negative gap → the parent's system (`A` or `B`) has not booked them.

## 5. Persisted derived tables — `OMS_TRACE`

### 5.1 `DriftEvents` (append-only, via the Derived Table Writer from `break_events`)

| Column | Type |
|---|---|
| `Date` | String (partitioning) |
| `Ts` | Instant |
| `System`, `OrderId`, `RootId`, `Edge` | String |
| `From`, `To` | String (the `BreakKind` / `ParentState` before and after) |
| `FillGap` | long |
| `GapAgeSec` | long |

This is the forensic record the source doc wanted from the pending buffer: when a C became
unresolved, when it linked, when a gap became drift, when it closed.

### 5.2 `DailyDriftSummary` (one partition per day, written at EOD by `add_table_partition`)

`drift_by_symbol` and `drift_by_a_order` as of the lineage PQ's last cycle before its daily
restart, plus `trace_stats`. Lets reports ask "what did B miss on 2026-10-24" without
replaying.

## 6. Break taxonomy and attribution

Evaluated per parent row (edges) and per child row (links), first match wins. Red kinds are
drift; amber are informational.

| `BreakKind` | Applies to | Condition | Meaning / attribution | Colour |
|---|---|---|---|---|
| `NO_LINK` | child | `LinkId == ``` on a B or C row | the upstream system never stamped the parent id; the child's quantity is unattributable | red |
| `UNRESOLVED` | child | `LinkId` set, no parent found | parent not arrived (R8) or never will (R9); alert on age | amber → red at alert age |
| `MISMATCH` | child | `Symbol != ParentSymbol || Side != ParentSide` | a child working the wrong instrument or direction | red |
| `OVERFILL` | any | `CumQty > OrderQty` | executed beyond order size in that system | red |
| `TRANSIENT` | parent | `FillGap != 0` and `GapAgeSec < AGE_SEC` | propagation lag, not drift (R6) | none |
| `MISSING_DOWNSTREAM` | parent | `FillGap > 0`, aged | the children's system is missing `FillGap` shares the parent executed: **dropped executions at B (edge AB) or C (edge BC)** | red |
| `UNBOOKED_UPSTREAM` | parent | `FillGap < 0`, aged | children executed more than the parent shows: **the parent's system has not booked them** (A on edge AB, B on edge BC) | red |
| `NONE` | any | | reconciles | — |

Two identities make the rollups add up (proved in doc 14 §5.3 for the same structure; unit
tested in doc 09): for a root whose every node is linked,

```
ACumQty − ΣC CumQty  =  GapAB + Σ GapBC        (telescoping over the tree)
MissingAtB − UnbookedAtA = GapAB ;  MissingAtC − UnbookedAtB = Σ GapBC
```

so `drift_by_a_order` is exactly the source doc's "the number system A needs to see", with the
system named.

## 7. Keys, types and null rules

- **Ids stay in their native column type.** Joins use `LinkId = OrderId` per edge; no
  concatenated `NodeKey` outside `trace_nodes` (D8). If all systems use numeric ids, the
  schema uses `long` and sizing drops ~5× (doc 06 §1).
- **Quantities are `long`.** Gaps are exact; `QTY_TOL` exists only for feeds that send
  fractional quantities (then `double` with a tolerance, as doc 14 does).
- **Nulls** are removed at the adapter (§3). A null parent column after a join means "no
  parent", never "parent with null value", because `ParentPresent` is checked first.
- **Time**: aging uses `RecvTs` (receive time in the ingesters, one clock) rather than
  `EventTs` (three clocks, Q12). `EventTs` is kept for display and forensics.
- **`Date`** is assigned by the ingester from `RecvTs` in the configured zone and is the only
  partitioning column everywhere.

## 8. Configuration keys referenced by these tables

| Key | Default | Used by |
|---|---|---|
| `OMS_TRACE_RAW_NAMESPACE` | `OMS_RAW` | adapters |
| `OMS_TRACE_OUT_NAMESPACE` | `OMS_TRACE` | drift writer, EOD snapshot |
| `OMS_TRACE_DATE_WINDOW_DAYS` | `2` | §4.1 window (today, yesterday, plus tomorrow for midnight rollover) |
| `OMS_TRACE_GAP_AGE_SEC` | `30` | `TRANSIENT` boundary |
| `OMS_TRACE_UNRESOLVED_ALERT_SEC` | `3600` | `unresolved_links.Alerted` |
| `OMS_TRACE_CLOCK_SEC` | `30` | the aging clock period |
| `OMS_TRACE_QTY_TOL` | `0` | tolerance (long quantities) |
| `OMS_TRACE_TZ` | `America/New_York` | `Date` assignment and the daily window |
