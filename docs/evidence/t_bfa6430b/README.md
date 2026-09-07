# Evidence: `transformWithState` batch executability + column-lineage coverage (kanban `t_bfa6430b`)

Real-cluster Databricks validation of the transformWithState lineage work:
that batch `groupByKey().transformWithState(StatefulProcessor, ...)` is
executable on Spark 4.0.0 (SPARK-46865), and that the fork's new
`TransformWithStateReflector` + `TypedGroupByVisitor` matching makes the
`TransformWithState` Catalyst node visible to column lineage — matching the
established `MapGroups` semantics.

Run date: 2026-08-31, DBR 17.3 LTS (Spark 4.0.0, Scala 2.13.16),
single-node `Standard_D4ds_v5`, cluster `spark-ol-tws-tbfa6430b`
(`0831-112032-l0ldhuav`), job run `1036970137248900` (task run
`275285528789721`), all 9 notebook cells finished.

## What was run

The notebook (9 cells) executes four write plans against
`databricks_ws.openlineage_demo.raw_customers` and captures OpenLineage
events via the spark-ol listener with column lineage + typed-boundary
fan-in enabled (`transport.type=file` on driver-local disk, exported to the
UC volume after the run):

| Cell | Construct | Output table |
|------|-----------|--------------|
| 0 | markdown header | — |
| 1 | env proof: spark/scala versions, listener attached, fresh jar loaded, serialization round-trip of both processors | — |
| 1 | `%sh` jars staged, no `NoClassDefFoundError` in driver logs, listener registration lines | — |
| 4 | **TWS-1**: `groupByKey(country).transformWithState(new CountByCountry, TimeMode.None(), OutputMode.Append())` | `tws_no_init_ol` |
| 5 | **TWS-2**: same grouping + `StatefulProcessorWithInitialState` (second `AppendColumns` branch, right side of `TransformWithState`) | `tws_init_ol` |
| 6 | **CONTROL mapGroups**: same source, same key lambda, v0 API already covered | `typed_groupby_tws_ol` |
| 7 | **CONTROL untyped**: `groupBy(country).count()` — established `AggregateV2Visitor` signal | `untyped_groupby_tws_ol` |
| 8 | flush + export events | — |

Optimized plans captured in cell output (see notebook export in
`docs/evidence/t_bfa6430b/notebook_transformwithstate_validation.scala`):
TWS-1 shows `TransformWithState ... NoTime, Append ...` directly over
`AppendColumns` (the `groupByKey` key-wrapper), TWS-2 shows the same over a
`BinaryNode` with a second `AppendColumns` on the right, mapGroups shows
`MapGroups` over one `AppendColumns`, untyped shows plain `HashAggregate`.

## Verdict (measured by `assert_events.py`, all 12 checks PASS)

| Construct | Field edges (fan-in ON) | Dataset edges |
|-----------|------------------------|--------------|
| TWS-1 no-init | `country`, `customer_count` ← all 4 inputs, `INDIRECT/TRANSFORMATION`, desc `TransformWithState((String, Tuple4) -> Tuple2), AppendColumns` | `INDIRECT/GROUP_BY` on all 4 inputs |
| TWS-2 with-init | same shape as TWS-1 (left-side resolution only; initial-state branch contributes no input fields) | `INDIRECT/GROUP_BY` on all 4 inputs |
| mapGroups control | same shape, desc `MapGroups((String, Tuple4) -> Tuple2), AppendColumns` | `INDIRECT/GROUP_BY` on all 4 inputs |
| untyped control | `country` `DIRECT/IDENTITY` | `INDIRECT/GROUP_BY` on `country` |

Interpretation:

- **The gap is closed.** Before the ticket, the `TransformWithState` node
  was invisible to the fork's typed visitors (no `instanceof` match, and
  spark3 can't reference the Spark 4 class at compile time), so a
  `groupByKey().transformWithState(...)` write emitted **zero** grouping
  edges. Now it emits exactly the same signal shape as the established
  `MapGroups` semantics — operator + child `AppendColumns` comma-joined in
  the description — via the new `TransformWithStateReflector` (matches by
  class name via `PlanUtils.safeIsInstanceOf` + reflection) feeding
  `TypedGroupByVisitor` / `TypedBoundaryFanInVisitor`.
- **Fan-in ON for all constructs.** `typedBoundaryFanInEnabled=true` on this
  run. Fan-in OFF is covered by the JUnit `edges()` assertions and prior
  evidence `t_511e1153` (row C).
- **Batch executability is proven on the real cluster**, confirming the
  premise analysis: `BasicOperators` routes `TransformWithState` to
  `TransformWithStateExec.generateSparkPlanForBatchQueries(...,
  isStreaming=false)` (SparkStrategies.scala:969), `checkForBatch` only
  rejects when `t.isStreaming`, so batch TWS plans and run to completion.

## Files

- `notebook_transformwithstate_validation.scala` — the executed notebook
  (imported to `/Users/ievgenii.korniichuk@ubsclouddisconnected.onmicrosoft.com/openlineage_demo/`)
- `spark-ol-tws-init.sh` — init script staging the jars to
  `/databricks/jars/zzz_*` (driver classpath before
  `spark.extraListeners` instantiation)
- `cluster_spec_tws.json` — cluster spec (DBR 17.3 LTS, single node,
  spark-ol confs: `transport.type=file`,
  `transport.location=/local_disk0/ol_events/events.jsonl`,
  `datasetLineageEnabled=true`, `typedBoundaryFanInEnabled=true`,
  `typedBoundaryFanInMaxEdges=10000`)
- `events_tws_fanin_true.jsonl` — the 22 captured events (6 datasets ×
  START/RUNNING/COMPLETE; 4 output tables with columnLineage facets)
- `assert_events.py` — the machine-checkable verdict above

## Reproduce

```bash
# jar: Spark4 profile, current worktree (temp/3 + TWS work)
mvn -P Spark4,scala_2.13.16 -pl core -am -DskipTests -Dgpg.skip=true clean install
# -> core/target/spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar (853,279 bytes)

databricks fs cp core/target/spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar \
  "dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar" --overwrite
databricks fs cp api/target/spark-ol_api_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar \
  "dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/spark-ol_api_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar" --overwrite

databricks workspace import "$USER/openlineage_demo/notebook_transformwithstate_validation" \
  --file docs/evidence/t_bfa6430b/notebook_transformwithstate_validation.scala \
  --language SCALA --format SOURCE --overwrite
databricks workspace import "$USER/openlineage_demo/spark-ol-tws-init.sh" \
  --file docs/evidence/t_bfa6430b/spark-ol-tws-init.sh --format RAW --overwrite

databricks clusters create --json @docs/evidence/t_bfa6430b/cluster_spec_tws.json
databricks jobs submit --json @... # existing_cluster_id + notebook_task
# after the run: fs cp the exported events file back, then:
python3 docs/evidence/t_bfa6430b/assert_events.py
```

Cluster permanently deleted after evidence collection (cost hygiene).