# Evidence: pandas operator column lineage on Databricks (kanban `t_38f39e29`)

Real-cluster Databricks validation that the pandas operator family —
`mapInPandas`, `mapInArrow`, `groupBy(...).applyInPandas`, `groupBy(...).applyInArrow`
— emits the expected `columnLineage` facet on a real Spark 4.0.0 cluster. This is
the E2E counterpart of the plan-level `PandasColumnLineageTest` (parent
`t_5a7b61d6`), which validated the visitor→builder→facet chain on a real analyzed
plan but never executed a Python worker. Here the constructs run end-to-end through
a real Unity Catalog write with a real Python UDF.

## Run

- **Date:** 2026-09-05
- **Runtime:** DBR **17.3 LTS** — Spark **4.0.0**, Scala **2.13.16**
- **Cluster:** `spark-ol-pandas-e2e` (`0905-080408-2mdoaht3`), single-node
  `Standard_D4ds_v5`, `SINGLE_USER`, autotermination 20 min, deleted after the run
- **Job / run:** `1002035698143565` (job `383660103697607`, task run
  `950462073652658`); all 9 cells finished, **33 events captured**
- **Auth:** Databricks profile `adb-4380664978234478` (headless WSL OAuth via
  `clip.exe` clipboard route)

### Provenance (measured, not assumed)

- Parent commit: `384a91e` ("added evidences")
- Submodule `core/OpenLineage` tip: `1a6597921` ("spark: extended visitors
  implementation for all types of map-like elements and udf")
- Jar (rebuilt for exact provenance, then verified):
  - `core/target/spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar` — 858,221 bytes, 480 files
  - `api/target/spark-ol_api_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar`
- Build: `mvn -P Spark4,scala_2.13.16 -pl core -am -DskipTests -Dgpg.skip=true clean install` → BUILD SUCCESS
- The jar carries the `PandasOperatorVisitor` class and the `Dependency.merge`
  fix (both verified in the jar via `unzip -l`).

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

**Note:** the pandas fan-in ships **unflagged** (exact marshalling, not a
pessimistic boundary fan-in), so `typedBoundaryFanInEnabled` is irrelevant to
these nodes. It is left ON here only to match the comprehensive run's config
surface; the no-flag assertion is covered by the JUnit `pandasFanInIsNotGatedBehindTheFlag`
test.

## What was run

The notebook (9 cells) executes six write plans against
`databricks_ws.openlineage_demo.raw_customers` (5 columns:
`customer_id, name, email, country, signup_date`) and captures OpenLineage events
via the spark-ol listener, flushed + exported to the UC volume in the last cell.

| Cell | Construct | Output table |
|------|-----------|--------------|
| 1 | **env proof**: spark/scala versions, listener attached (parameterless `listeners()` reflection), fresh jar loaded | — |
| 2 | source + pandas UDFs (`map_pandas`, `map_arrow`, `apply_pandas`, `apply_arrow`) | — |
| 3 | **CONTROL** untyped `select` — DIRECT/IDENTITY baseline | `pandas_control_ol` |
| 4 | **mapInPandas** | `pandas_mapinpandas_ol` |
| 5 | **mapInArrow** | `pandas_mapinarrow_ol` |
| 6 | **applyInPandas** `groupBy(country)` | `pandas_applyinpandas_ol` |
| 7 | **applyInPandas** expression key `ceil(customer_id/2)` | `pandas_applyexpr_ol` |
| 8 | **applyInArrow** `groupBy(country)` | `pandas_applyinarrow_ol` |
| 9 | **flush + export** events to UC volume | — |

## Verdict (measured by `assert_events.py`, 25 checks, 0 failures)

Field edges are the `columnLineage.fields[*].inputFields[*].transformations`
union; dataset edges are the `columnLineage.dataset[*]` entries (the
`INDIRECT/GROUP_BY` grouping signal lives there).

| Construct | Field edges | Dataset edges |
|-----------|-------------|---------------|
| control untyped | `customer_id`,`name` ← `DIRECT/IDENTITY` | none |
| mapInPandas | `o1`,`o2` ← all 3 inputs `INDIRECT/TRANSFORMATION`, desc `MapInPandas(map_pandas)` | none |
| mapInArrow | same, desc `MapInArrow(map_arrow)` | none |
| applyInPandas groupBy(col) | `o1`,`o2` ← all 3 inputs, desc `FlatMapGroupsInPandas(apply_pandas)` | `INDIRECT/GROUP_BY` on `country` |
| applyInPandas expr key | same fan-in, desc `FlatMapGroupsInPandas(apply_pandas)` | `INDIRECT/GROUP_BY` on `customer_id` (resolved through the `ceil` Alias) |
| applyInArrow | same fan-in, desc `FlatMapGroupsInArrow(apply_arrow)` | `INDIRECT/GROUP_BY` on `country` |

All six output tables produced a `columnLineage` facet. The pandas fan-in is
**exact** (every output ← every marshalled input column), and the grouping signal
lands on the real grouping column — including the expression-key case, where the
`GROUP_BY` resolves through the `Alias` in the `Project` below the node to
`customer_id` (the `Dependency.merge` fix from the parent ticket is what makes
this survive as `INDIRECT/GROUP_BY` rather than being downgraded to
`DIRECT/IDENTITY`).

## Bugs found and fixed (all reproduced live on-cluster)

1. **`DataFrame.queryExecution` removed in PySpark 4.0.** The Scala-style
   `df.queryExecution.optimizedPlan.treeString` used in the comprehensive Scala
   notebook is not a Python attribute in Spark 4.0 (raises
   `PySparkAttributeError: [ATTRIBUTE_NOT_SUPPORTED]`). Fixed by using the JVM
   accessor `df._jdf.queryExecution().optimizedPlan().treeString()`.
2. **Arrow UDF return types.** `mapInArrow`/`applyInArrow` do not accept pandas
   DataFrames: `mapInArrow` must yield `pyarrow.RecordBatch` (not a DataFrame),
   and `applyInArrow` must return a single `pyarrow.Table` (not a generator, not a
   DataFrame). The grouped-map pandas twin `applyInPandas` must return a single
   `DataFrame` (not a generator). Each was surfaced as a distinct
   `PySparkTypeError`/`PySparkRuntimeError` and fixed in turn.
3. **Arrow int width.** The `o2` constant defaulted to `int64` while the declared
   schema is `int32`, tripping `[RESULT_TYPE_MISMATCH_FOR_ARROW_UDF]`. Fixed by
   casting `o2` to `int32` before returning.
4. **Stale sink across runs.** The file transport appends, so re-running on a
   reused cluster left prior runs' events in the exported file (a stale
   `MapInArrow(map_pandas)` from an earlier failed run polluted the first green
   export). Fixed by deleting the cluster and re-running on a fresh one (empty
   `/local_disk0`), which is also the cost-hygiene posture the task requires.

## Reproduce

```bash
# 1. build the jar from the submodule tip (Spark4 / scala_2.13.16)
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
mvn -P Spark4,scala_2.13.16 -pl core -am -DskipTests -Dgpg.skip=true clean install

# 2. stage the jar to the UC volume (overwrite the existing one)
databricks fs cp core/target/spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar \
  "dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar" --overwrite

# 3. import the notebook + init script
databricks workspace import "/Users/<you>/openlineage_demo/notebook_pandas_validation" \
  --format SOURCE --language PYTHON --overwrite --file docs/evidence/t_38f39e29/notebook_pandas_validation.py
databricks workspace import "/Users/<you>/openlineage_demo/spark-ol-pandas-init.sh" \
  --format RAW --overwrite --file docs/evidence/t_38f39e29/spark-ol-pandas-init.sh

# 4. create the single-node cluster (cluster_spec.json) and submit the notebook
databricks clusters create --json @docs/evidence/t_38f39e29/cluster_spec.json
databricks jobs submit --json @docs/evidence/t_38f39e29/job_submit.json   # update existing_cluster_id

# 5. pull the exported events and assert the verdict
databricks fs cp "dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/pandas/events_pandas_fanin_true.jsonl" \
  docs/evidence/t_38f39e29/events_pandas_fanin_true.jsonl --overwrite
python3 docs/evidence/t_38f39e29/assert_events.py
```

## Artifacts

| File | Purpose |
|------|---------|
| `notebook_pandas_validation.py` | the 9-cell validation notebook (Python, Databricks) |
| `spark-ol-pandas-init.sh` | driver init script: stages jar + runtime deps to `/databricks/jars` before `spark.extraListeners` instantiates |
| `cluster_spec.json` | single-node 4.0.0 cluster with dataset lineage ON |
| `job_submit.json` | the one-time run spec (existing cluster + notebook task) |
| `events_pandas_fanin_true.jsonl` | **33 captured OpenLineage events** (6 output datasets × START/RUNNING/COMPLETE) |
| `assert_events.py` | the 25-check machine-readable verdict (all PASS) |
| `README.md` | this file |

Cluster `0905-080408-2mdoaht3` permanently deleted after evidence collection
(cost hygiene); the earlier iteration cluster `0905-074659-iljg05nr` was also
deleted.
