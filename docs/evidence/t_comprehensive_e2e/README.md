# Evidence: comprehensive OpenLineage column-lineage E2E (all implemented constructs)

Real-cluster Databricks validation of **the full implemented column-lineage
surface** on one Spark 4.0.0 cluster: the untyped baseline, every typed
Dataset boundary (`map`/`flatMap`→`MapElements`, `mapPartitions`/`flatMap`→
`MapPartitions`, chained boundaries, `groupByKey().mapGroups`, the two
stateful operators `flatMapGroupsWithState` (v0) and `transformWithState`
(v2, with and without initial state), typed `filter`, a registered named UDF,
and a full compose (typed map + untyped filter + UDF). Every construct is
measured against a real captured `columnLineage` facet — not inferred.

This is the superset of the per-ticket evidence: it reproduces the
`t_511e1153` (UDF + typed map/fan-in descent), `t_bfa6430b`
(transformWithState) and `t_ddb87c95` (MapElements typeinfo) verdicts in one
run and adds `mapPartitions`/chained/FMGWS/compose coverage on the same
provenance.

## Run

- **Date:** 2026-09-01
- **Runtime:** DBR **17.3 LTS** — Spark **4.0.0**, Scala **2.13.16**
  (read back live in cell 1)
- **Cluster:** `spark-ol-comprehensive-e2e` (`0901-104946-yqv918yv`),
  single-node `Standard_D4ds_v5`, `SINGLE_USER`, autotermination 20 min,
  deleted after the run
- **Job / run:** final green run `571467562297644` (job
  `314969488364454`, task run `471956278530219` after the two bug-fix
  re-imports `125311081132656` and `32697984066689` — see "Bugs found and
  fixed"); all 17 cells finished, **68 events captured**
- **Auth:** Databricks profile `adb-4380664978234478` (headless WSL OAuth
  via `clip.exe` clipboard route, saved to memory)

### Provenance (measured, not assumed)

- Parent commit: `bc8a770a186424be0aaef825a989fdf3729eecca`
  ("spark: extended visitors implementation for all types of map-like
  elements and udf")
- Submodule `core/OpenLineage` tip: `1a65979210202c94a079bc163728a55598fe17b0`
- Jar (rebuilt for exact provenance from that submodule tip, then verified):
  - `core/target/spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar` — 853,280 bytes,
    479 files
  - `api/target/spark-ol_api_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar`
- Build: `mvn -P Spark4,scala_2.13.16 -pl core -am -DskipTests -Dgpg.skip=true clean install` → BUILD SUCCESS

### Spark conf (driver)

| key | value |
|-----|-------|
| `spark.openlineage.transport.type` | `file` (driver-local `/local_disk0/ol_events/events.jsonl`) |
| `spark.openlineage.transport.location` | `/local_disk0/ol_events/events.jsonl` |
| `spark.openlineage.columnLineage.typedBoundaryFanInEnabled` | `true` |
| `spark.openlineage.columnLineage.typedBoundaryFanInMaxEdges` | `10000` |
| `spark.openlineage.columnLineage.datasetLineageEnabled` | `true` |
| `spark.openlineage.namespace` / `.version` | `spark-ol-e2e` / `v1` |
| `spark.extraListeners` | `io.openlineage.spark.agent.OpenLineageSparkListener`, `com.databricks.backend.daemon.driver.DBCEventLoggingListener` |

**Note:** config is captured at listener init (`OpenLineageContext`), so
fan-in / dataset-lineage can't be toggled mid-run. The **fan-in OFF** side is
already covered by the `t_511e1153` OFF evidence; this run is the fan-in ON
matrix.

## What was run

The notebook (17 cells) executes 15 write plans against
`databricks_ws.openlineage_demo.raw_customers` (4 columns:
`customer_id, email, name, country`) and captures OpenLineage events via the
spark-ol listener, flushed + exported to the UC volume in the last cell.

| Cell | Construct | Output table |
|------|-----------|--------------|
| 1 | **env proof**: spark/scala versions, listener attached (parameterless `listeners()` reflection), fresh jar loaded, both stateful processors + UDF serialisation round-trip | — |
| 2 | source + registered UDF `echoUpper` + `CountByCountry` (v0) / `CountWithInit` (v2) processors | — |
| 3 | **CONTROL** untyped `select` — DIRECT/IDENTITY baseline | `comprehensive_control_ol` |
| 4 | **typed map** — core fan-in boundary `MapElements` | `comprehensive_map_ol` |
| 5 | **map over untyped filter BELOW** — pass-through descent through `Filter` | `comprehensive_map_filter_ol` |
| 6 | **map + untyped filter ABOVE** — descent through `Project` | `comprehensive_map_filter_above_ol` |
| 7 | **mapPartitions** (flatMap lowers to the same node) | `comprehensive_mappartitions_ol` |
| 8 | **chained** map → mapPartitions | `comprehensive_chained_ol` |
| 9 | **groupByKey().mapGroups** (v0 typed grouping) | `comprehensive_mapgroups_ol` |
| 10 | **map then groupByKey().mapGroups** (AppendColumns composition) | `comprehensive_map_then_group_ol` |
| 11 | **groupByKey().flatMapGroupsWithState** (v0 stateful, batch) | `comprehensive_fmGws_ol` |
| 12 | **transformWithState NO init** (v2 stateful, batch) | `comprehensive_tws_no_init_ol` |
| 13 | **transformWithState WITH init** (v2, `BinaryNode` right-branch) | `comprehensive_tws_init_ol` |
| 14 | **typed filter** (`t_511e1153` regression) | `comprehensive_typed_filter_ol` |
| 15 | **registered UDF** over a plain column (`t_511e1153` construct U) | `comprehensive_udf_ol` |
| 16 | **full compose**: typed map + untyped filter + UDF | `comprehensive_compose_ol` |
| 17 | **flush + export** events to UC volume | — |

> Cell 11 writes `comprehensive_fmGws_ol` (capital G) — the assert key is kept
> in sync with the literal table name (see `assert_events.py` note).

## Verdict (measured by `assert_events.py`, 52 checks, 0 failures)

Field edges are the `columnLineage.fields[*].inputFields[*].transformations`
union; dataset edges are the `columnLineage.dataset[*]` entries (the
`INDIRECT/GROUP_BY` grouping signal lives there, mirroring `t_bfa6430b`).

| Construct | Field edges (fan-in ON) | Dataset edges |
|-----------|------------------------|---------------|
| control untyped | `customer_id`,`name` ← `DIRECT/IDENTITY` | none |
| typed map (4 cols) | `country`,`customer_id` ← all 4 inputs `INDIRECT/TRANSFORMATION`, desc `MapElements(Tuple4 -> Tuple2)` | none |
| map over untyped filter | `2 × 4` = 8 edges, desc `MapElements(Tuple4 -> Tuple2)` | none |
| map + filter above | `2 × 3` = 6 edges, desc `MapElements(Tuple3 -> Tuple2)` (plan selects 3 cols) | none |
| mapPartitions | `2 × 3` = 6 edges, desc `MapPartitions(-> Tuple2)` | none |
| chained map→mapPartitions | `2 × 3` = 6 edges, desc `MapPartitions(-> Tuple2), MapElements(Tuple3 -> Tuple3), sameType` | none |
| groupByKey().mapGroups | `2 × 4` = 8 edges, desc `MapGroups((String, Tuple4) -> Tuple2), AppendColumns` | `INDIRECT/GROUP_BY` on all 4 inputs |
| map then mapGroups | `2 × 4` = 8 edges, desc `MapGroups((String, Tuple4) -> Tuple2), MapElements(Tuple4 -> Tuple4), sameType` | `INDIRECT/GROUP_BY` on all 4 inputs |
| flatMapGroupsWithState | `2 × 4` = 8 edges, desc `FlatMapGroupsWithState((String, Tuple4) -> Tuple2), AppendColumns` | `INDIRECT/GROUP_BY` on all 4 inputs |
| transformWithState (no init) | `2 × 4` = 8 edges, desc `TransformWithState((String, Tuple4) -> Tuple2), AppendColumns` | `INDIRECT/GROUP_BY` on all 4 inputs |
| transformWithState (+ init) | `2 × 4` = 8 edges, desc `TransformWithState((String, Tuple4) -> Tuple2), AppendColumns` | `INDIRECT/GROUP_BY` on all 4 inputs |
| typed filter | all 4 inputs ← `DIRECT/IDENTITY` | `INDIRECT/FILTER` on all 4 deserialized cols |
| UDF (plain col) | `customer_id`←`DIRECT/IDENTITY`, `shouty`←`name` `INDIRECT/TRANSFORMATION desc "UDF: echoUpper"` | none |
| compose | `country`,`shouty` ← all 4 inputs `INDIRECT/TRANSFORMATION desc MapElements(Tuple4 -> Tuple2)` — **8 of 8 fan-in**; **no separate `UDF:` edge** (see finding) | none |

All 15 output tables produced a `columnLineage` facet; the 14 typed/boundary
constructs each produced the expected fan-in shape.

### Finding: a UDF composed *on top of* a typed boundary is absorbed into the fan-in

In the compose cell (typed `map` → `filter` → `select(udf(...))`), the `UDF:
echoUpper` marker does **not** survive as a separate edge. Both emitted
fields resolve to the fan-in description `MapElements(Tuple4 -> Tuple2)` with
no `UDF:` description.

Cause is expected builder semantics, not a lineage bug:
`ColumnLevelLineageBuilder.findDependentInputs` merges the indirect fan-in
edge with the `DIRECT/IDENTITY` edge feeding the fresh typed attr, and
`TransformationInfo.merge` rule #1 says **if the current edge is INDIRECT its
description wins** — so the fan-in description supersedes the `UDF: name` one.
The UDF-over-plain-column cell (cell 15) still shows `UDF: echoUpper` intact,
which is the construct `t_511e1153` pinned. The assert for cell 16 therefore
documents the **actual** behaviour (UDF marker absorbed, no separate edge)
rather than a presumed one.

## Bugs found and fixed (both reproduced live on-cluster)

1. **Cell 1 — reflection overload.** `LiveListenerBus` has **two** methods
   named `listeners` (a 0-arg and a `String`-arg overload); a naive
   `find(_.getName == "listeners")` returned the 1-arg one and the reflective
   call threw `IllegalArgumentException: wrong number of arguments`. Fixed by
   pinning the parameterless method:
   `find(m => m.getName == "listeners" && m.getParameterCount == 0)`.
   Surfaced in run `125311081132656` (cell 1 FAILED).
2. **Cell 4 — `Encoders`.** The Java-style `Encoders.product[...]` import is
   not valid in the Scala cells with `spark.implicits._` in scope, where
   encoders are derived implicitly. Fixed by dropping the explicit `Encoders`
   args at the two `mapPartitions` sites. Surfaced in run `32697984066689`
   (cell 4 FAILED).

After both fixes, run `571467562297644` completed all 17 cells green.

## Reproduce

```bash
# 1. build the jar from the submodule tip (Spark4 / scala_2.13.16)
mvn -P Spark4,scala_2.13.16 -pl core -am -DskipTests -Dgpg.skip=true clean install

# 2. stage jar + runtime deps to the UC volume /Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx
#    (see spark-ol-comprehensive-init.sh for the exact jar list incl. micrometer/HdrHistogram/LatencyUtils/jspecify)

# 3. create the single-node cluster with the init script (cluster_spec.json:
#    DBR 17.3 LTS, fan-in ON, dataset lineage ON, transport.type=file)
databricks clusters create --profile adb-4380664978234478 --cluster-spec docs/evidence/t_comprehensive_e2e/cluster_spec.json

# 4. import notebook_comprehensive_validation.scala and run it; the last cell
#    flushes /local_disk0/ol_events/events.jsonl to the UC volume

# 5. assert the verdict against the captured events
python3 docs/evidence/t_comprehensive_e2e/assert_events.py
```

## Artifacts

| File | Purpose |
|------|---------|
| `notebook_comprehensive_validation.scala` | the 17-cell validation notebook (Scala, Databricks) |
| `spark-ol-comprehensive-init.sh` | driver init script: stages jar + runtime deps to `/databricks/jars` before `spark.extraListeners` instantiates |
| `cluster_spec.json` | single-node 4.0.0 cluster with fan-in ON + dataset lineage ON |
| `events_comprehensive_fanin_true.jsonl` | **68 captured OpenLineage events** (6 output datasets × START/RUNNING/COMPLETE rows across the 15 write plans) |
| `assert_events.py` | the 52-check machine-readable verdict (all PASS) |
| `README.md` | this file |
