# End-to-end validation of our OpenLineage build on Databricks

Kanban card `t_511e1153`. First run of *our* `spark-ol` build on a real
Databricks cluster against real Unity Catalog tables, capturing real
`RunEvent` JSON. Everything before this card was verified only against
locally-built optimized plans in JUnit.

## Verdict

Our build works on DBR 17.3 LTS / Spark 4.0.0 / **Scala 2.13** and emits every
edge the visitors claim. The upstream blocker this project exists to route
around did **not** reproduce. Two divergences between what the JUnit suite
asserts and what a real cluster emits were found — both are about the
`dataset` section of the facet, neither is a wrong edge. See
[Divergences](#divergences-from-the-junit-suite).

## What was run

| | |
|---|---|
| Build | `mvn -P Spark4,scala_2.13.16 clean install -DskipTests -Dgpg.skip=true` → `BUILD SUCCESS` (57s) |
| Parent commit | `e98eb3a`, submodule `core/OpenLineage` at `35cf91682` |
| Artefact | `spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar` (846,351 bytes) |
| Runtime | DBR 17.3 LTS — Spark **4.0.0**, Scala **2.13.16** (both read back from the live session, cell 1) |
| Cluster | single-node `Standard_D4ds_v5`, `SINGLE_USER`, autotermination 20 min, **permanently deleted** after the run |
| Transport | `file` → `/local_disk0/ol_events/events.jsonl`, copied out to a UC Volume |
| Runs | fan-in **off**: 33 events, `SUCCESS` (38s) · fan-in **on**: 28 events, `SUCCESS` (82s) |
| Data | existing `databricks_ws.openlineage_demo.raw_customers` (6 rows); outputs written to new `*_ol` tables only |

Reproduce the verdict from the committed events:

```bash
python3 docs/evidence/t_511e1153/assert_events.py
# 16 checks, all PASS
```

Independently re-verified after the run, on the same commit: the build again
gave `BUILD SUCCESS` (42.8s) producing an identical 846,351-byte
`spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar`, `assert_events.py` again exited 0
with 16/16 PASS against the committed JSONL, and `databricks clusters list`
showed every cluster on the workspace `TERMINATED` — the cluster this card
created no longer exists at all, and the `openlineage-demo-212` straggler left
by the earlier session is terminated too.

## Packaging: the upstream Scala 2.13 blocker did not reproduce

The motivating failure was upstream `io.openlineage:openlineage-spark_2.13`
dying on DBR 16.4 / Scala 2.13 with `NoClassDefFoundError: scala/Serializable`
(OpenLineage/OpenLineage#4063, fix PR #4099 unreleased), which a prior session
could only escape by falling back to the `_2.12` artefact on a 2.12 runtime.

Our build needed no fallback. Measured on the cluster:

```
spark.version         = 4.0.0
scala.util.Properties = 2.13.16
grep -c NoClassDefFoundError /databricks/driver/logs/*.log
  active.log:0  git_agent.log:0  log4j-active.log:0  stacktrace.log:0
grep "scala/Serializable\|scala.Serializable" /databricks/driver/logs/*.log
  (no match)
INFO SparkContext: Registered listener io.openlineage.spark.agent.OpenLineageSparkListener
```

and the listener is genuinely attached to the live bus, not merely on the
classpath — `sc.listenerBus.listeners` contains
`io.openlineage.spark.agent.OpenLineageSparkListener`.

Every one of our five visitor classes resolved out of our own jar:

```
UserDefinedExpressionVisitor   -> file:/databricks/jars/zzz_spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar
TypedFilterVisitor             -> (same jar)
TypedGroupByVisitor            -> (same jar)
TypedBoundaryFanInVisitor      -> (same jar)
io.openlineage.client.transports.FileTransport -> file:/databricks/jars/zzz_openlineage-java-1.51.0.jar
```

### Runtime classpath that had to be assembled by hand

There is no shade/assembly plugin in any of the three poms, so a single-jar
install fails at listener init. Resolved with
`dependency:build-classpath -Dscope=runtime` and staged into
`/databricks/jars` by an init script (`init_script_spark-ol-dbx.sh`). Exact
list, all 17:

```
spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar      openlineage-java-1.51.0.jar
spark-ol_api_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar  openlineage-sql-java-1.51.0.jar
httpclient5-5.4.2.jar                           httpcore5-5.4.2.jar
httpcore5-h2-5.3.3.jar                          micrometer-core-1.17.0.jar
micrometer-commons-1.17.0.jar                   micrometer-observation-1.17.0.jar
jspecify-1.0.0.jar                              HdrHistogram-2.2.2.jar
LatencyUtils-2.0.3.jar                          jackson-module-blackbird-2.15.3.jar
jackson-datatype-jdk8-2.15.3.jar                jackson-datatype-jsr310-2.15.3.jar
jackson-dataformat-yaml-2.15.3.jar
```

The **micrometer** group is not in the card's predicted list and is not
optional. `openlineage-java` 1.51.0 declares `micrometer-core` as a *runtime*
dependency and `OpenLineageSparkListener.initializeMetrics` touches
`io.micrometer.core.instrument.MeterRegistry` unconditionally; DBR 17.3 does
not ship micrometer on the driver classpath and our reactor resolves it as
`provided`(optional) through Spark. Without those five jars the driver dies at
boot with `ClassNotFoundException: io.micrometer.core.instrument.MeterRegistry`
— i.e. a `dependency:list -Dscope=runtime` on *our* poms is necessary but not
sufficient; the transitive runtime closure of `openlineage-java` has to be
resolved too.

## Per-construct verdict

`fields` = the facet's per-output-field `inputFields`. `dataset` = the facet's
`dataset` section (dataset-level dependencies), which is where a row-scoped
signal like a filter or a grouping lands. Every observation below is quoted
from the committed JSONL and re-checked by `assert_events.py`.

| # | Construct | Expected | Observed (fan-in OFF) | Observed (fan-in ON) | |
|---|---|---|---|---|---|
| A | registered UDF `mask_email_ol(email)` → `masked_email` | `INDIRECT`/`TRANSFORMATION`, UDF name in description | `fields`: `INDIRECT/TRANSFORMATION` desc `"UDF: mask_email_ol"` | identical | PASS |
| A′ | `upper(country)` → `country_upper`, **same table, same event** | stays `DIRECT` | `fields`: `DIRECT/TRANSFORMATION` desc `""` | identical | PASS |
| B | typed `filter(t => …)` → `TypedFilter` | `INDIRECT`/`FILTER`, ships ON | `dataset`: 4 × `INDIRECT/FILTER`; `fields`: 4 × `DIRECT/IDENTITY` | identical | PASS |
| C | `groupByKey().mapGroups` → `MapGroups` | `INDIRECT`/`GROUP_BY`, ships ON | **no facet emitted at all** | `dataset`: 4 × `INDIRECT/GROUP_BY` (+ fan-in `fields`) | PASS-with-caveat |
| D | typed `map` → `MapElements` | nothing with the flag off | **no facet emitted** | `fields`: 2 × 4 = 8 × `INDIRECT/TRANSFORMATION` desc `"typed operation: MapElements, DeserializeToObject"` | PASS |
| E | control `upper(name)` + `concat(country,'-x')`, no UDF in the plan | every edge `DIRECT` | `fields`: 3 × `DIRECT` (`IDENTITY` + 2 × `TRANSFORMATION`), no `dataset` section | identical | PASS |

Rows A and A′ are the point of the exercise and they sit in **one output table
in one event**: the UDF edge is `INDIRECT` while the builtin edge next to it is
`DIRECT`. So `myUdf(email)` is genuinely distinguishable from `upper(country)`
on a real cluster, and the distinction is not "everything got downgraded".

Two details measured here that the JUnit assertions do not pin:

- A builtin's transformation carries an **empty** description (`""`), not an
  absent one. "Has a description" is therefore *not* the discriminator between
  a UDF edge and a builtin edge — the `type` is, and the description then names
  which UDF. `assert_events.py` pins `""` explicitly so a future change to
  `null` shows up as a diff.
- Row C's caveat is explained in [Divergence 2](#2-a-typed-grouping-is-invisible-in-real-events-unless-an-unrelated-flag-is-on)
  below. The `GROUP_BY` edge is correct when it appears; the problem is when it
  appears.

Row D's description is the plan nodes crossed
(`MapElements, DeserializeToObject`), and for row C's fan-in it is
`MapGroups, AppendColumns` — so a consumer can see *which* typed boundary
caused a pessimistic fan-out, not just that one did.

## Divergences from the JUnit suite

Both are about the `dataset` section, and both mean the JUnit suite is green on
a configuration the product never ships. Filed as follow-up card **`t_0a675656`**
rather than fixed here — this card's remit is measurement. That card carries the
three candidate contracts for divergence 2 and requires whichever is chosen to
be reflected back into `assert_events.py` in the same commit, so this evidence
stays truthful about what the code does.

### 1. The typed-lineage test harness pins `datasetLineageEnabled=false`; production defaults it to `true`

`TypedLineageTestBase.defaultConfig()` (core/src/test/…/TypedLineageTestBase.java:190)
does `config.getColumnLineageConfig().setDatasetLineageEnabled(false)`, and
`facetFields(...)` renders `builder.buildFields(false)` only — it never calls
`buildDatasetDependencies`. Meanwhile `ColumnLineageConfig.datasetLineageEnabled`
is an unset `Boolean` and `ColumnLevelLineageUtils` reads it as
`.orElse(true)`, so **the shipped default is enabled**.

Consequence: `INDIRECT/FILTER` and `INDIRECT/GROUP_BY` — the two edges that
land in the `dataset` section in production, per the events here — are never
observed through a facet by any typed-lineage test. They are observed only at
the lower-level `edges(...)` (raw `addDependency`) layer. The suite would stay
green if the `dataset` section were dropped from the facet entirely.

### 2. A typed grouping is invisible in real events unless an unrelated flag is on

`TypedGroupByVisitor` ships **ON** deliberately, and its javadoc argues at
length that a typed grouping must not be gated behind
`typedBoundaryFanInEnabled` because that flag exists to contain an *unfaithful*
pessimistic signal. On a real cluster it is gated anyway, in effect:

`ColumnLevelLineageUtils.buildColumnLineageDatasetFacet` finishes with
`if (facet.getFields().getAdditionalProperties().isEmpty()) return Optional.empty();`
— dataset-level dependencies alone cannot carry a facet. Across a typed
boundary no output field has any inputs unless the fan-in supplies them. So
with the fan-in off, `typed_groupby_ol` gets **no facet at all** (measured: zero
`columnLineage` on every event for that table), and with the fan-in on the same
`GROUP_BY` edges appear. The visitor is correct and unflagged; the facet
assembly downstream discards its output.

`MapGroupsColumnLineageTest` already asserts `assertThat(facetFields(ds)).isEmpty()`
for exactly this case and explains it in a comment, so this is known behaviour
rather than a surprise — but "known" is not the same as "intended", and the
practical effect contradicts the shipping decision. Worth noting the same
mechanism does *not* bite `TypedFilterVisitor`, because a filter sits under a
plain `Project` whose fields do have inputs; the `FILTER` dataset edges ride
along on those. It bites only where the boundary severs field linkage.

## Files

| File | What it is |
|---|---|
| `events_fanin_false.jsonl` | 33 `RunEvent`s, `typedBoundaryFanInEnabled=false` (the shipped default) |
| `events_fanin_true.jsonl` | 28 `RunEvent`s, same notebook, `typedBoundaryFanInEnabled=true` |
| `assert_events.py` | 16 assertions over both files; generates the verdict table above |
| `notebook_ol_typed_lineage_validation.scala` | the Scala notebook, one construct per cell |
| `init_script_spark-ol-dbx.sh` | stages the 17 runtime jars into `/databricks/jars` |
| `cluster_spec_faninoff.json`, `cluster_spec_faninon.json` | the two cluster specs, differing only in the fan-in flag |

Redaction: workspace/org id, `*.azuredatabricks.net` host, the account UPN,
driver IP and Azure subscription/resource ids are replaced with stable
placeholders. Unity Catalog table and catalog UUIDs are **kept** — they are the
join key between a dataset's `name` and its `symlinks` facet, so redacting them
would destroy the evidence, and they are opaque object ids rather than secrets.

Scala was required, not preferred: a typed `map` needs a JVM `Dataset` to
produce `MapElements`, which PySpark cannot do. The notebook uses tuple
encoders from `spark.implicits._` rather than notebook-local case classes,
whose REPL-wrapped encoders are unreliable, and `mapGroups` needs explicit
lambda parameter types because the overload (`MapGroupsFunction` vs
`Function2`) is otherwise ambiguous.
