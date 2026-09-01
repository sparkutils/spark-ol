// Databricks notebook source
// MAGIC %md
// MAGIC # spark-ol comprehensive e2e validation: the full typed/UDF column-lineage surface
// MAGIC
// MAGIC One notebook, one cluster run, every implemented feature exercised through a real
// MAGIC Unity Catalog write and captured as real OpenLineage events:
// MAGIC
// MAGIC | construct | visitor | signal |
// MAGIC |---|---|---|
// MAGIC | untyped select (control) | ProjectVisitor | DIRECT/IDENTITY |
// MAGIC | typed map | TypedBoundaryFanInVisitor | INDIRECT/TRANSFORMATION fan-in, `sameType` |
// MAGIC | untyped filter around typed map (descent) | fan-in descent | fan-in survives |
// MAGIC | mapPartitions / flatMap | TypedBoundaryFanInVisitor | fan-in, chained desc |
// MAGIC | chained map->mapPartitions | fan-in | single fan-in, chained desc |
// MAGIC | groupByKey().mapGroups | TypedGroupByVisitor | fan-in + INDIRECT/GROUP_BY |
// MAGIC | map then groupByKey().mapGroups | TypedGroupByVisitor | key via AppendColumnsWithObject |
// MAGIC | groupByKey().flatMapGroupsWithState (v0, batch) | TypedGroupByVisitor | fan-in + GROUP_BY |
// MAGIC | groupByKey().transformWithState (v2, batch) | TypedGroupByVisitor + reflector | fan-in + GROUP_BY |
// MAGIC | TWS with initial state (BinaryNode) | reflector left-side resolution | 4 GROUP_BY, not 8 |
// MAGIC | typed filter (t_511e1153 regression) | TypedFilterVisitor | INDIRECT/FILTER |
// MAGIC | registered UDF | UserDefinedExpressionVisitor | INDIRECT/TRANSFORMATION, `UDF: name` |
// MAGIC | UDF + typed map compose | both visitors | fan-in + UDF edge |
// MAGIC
// MAGIC Fan-in and dataset lineage are ON for the whole run (captured at listener init;
// MAGIC the fan-in-OFF side was already validated in t_511e1153).

// COMMAND ----------

// Cell 1 - environment, listener registration, jar provenance.
println("spark.version              = " + spark.version)
println("scala.util.Properties      = " + scala.util.Properties.versionNumberString)
println("spark.extraListeners       = " + spark.conf.get("spark.extraListeners", "<unset>"))

Seq(
  "spark.openlineage.transport.type",
  "spark.openlineage.transport.location",
  "spark.openlineage.namespace",
  "spark.openlineage.columnLineage.datasetLineageEnabled",
  "spark.openlineage.columnLineage.typedBoundaryFanInEnabled",
  "spark.openlineage.columnLineage.typedBoundaryFanInMaxEdges"
).foreach(k => println(f"$k%-58s = " + spark.conf.get(k, "<unset>")))

// Stale-jar disproof: every class this run depends on, loadable ONLY from the
// staged spark-ol jar. Also prints each code source, so jar provenance is in the
// run output, not just the init-script listing.
Seq(
  "io.openlineage.spark.agent.OpenLineageSparkListener",
  "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TransformWithStateReflector",
  "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedGroupByVisitor",
  "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedBoundaryFanInVisitor",
  "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedFilterVisitor",
  "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.UserDefinedExpressionVisitor",
  "io.openlineage.client.transports.FileTransport"
).foreach { cn =>
  val where =
    try {
      val c = Class.forName(cn)
      Option(c.getProtectionDomain.getCodeSource).map(_.getLocation.toString).getOrElse("<no codesource>")
    } catch { case t: Throwable => "LOAD FAILED: " + t.getClass.getName + ": " + t.getMessage }
  println(cn + "\n    -> " + where)
}

// Is the listener actually attached to the live SparkContext?
// NOTE: LiveListenerBus has TWO methods named "listeners" (0-arg and
// String-arg overloads); find may return either, so pin the parameterless one.
val attached = {
  val busField = sc.getClass.getDeclaredMethod("listenerBus")
  busField.setAccessible(true)
  val bus = busField.invoke(sc)
  val m = bus.getClass.getMethods.find(m => m.getName == "listeners" && m.getParameterCount == 0).get
  m.setAccessible(true)
  val l = m.invoke(bus).asInstanceOf[java.util.List[_]]
  import scala.collection.JavaConverters._
  l.asScala.map(_.getClass.getName).toList
}
println("attached listeners containing 'openlineage': " +
  attached.filter(_.toLowerCase.contains("openlineage")).mkString(", "))

// COMMAND ----------

// MAGIC %sh
// MAGIC echo "=== jars staged by the init script ==="
// MAGIC ls -la /databricks/jars/zzz_* 2>&1 | sed 's#/databricks/jars/##'
// MAGIC echo "=== NoClassDefFoundError in driver log? ==="
// MAGIC grep -c "NoClassDefFoundError" /databricks/driver/logs/*.log 2>/dev/null || echo "no log match"
// MAGIC echo "=== OpenLineage listener log lines ==="
// MAGIC grep -n "OpenLineage" /databricks/driver/logs/*.log 2>/dev/null | head -10 || true

// COMMAND ----------

// Cell 2 - source + registered UDF + stateful processors. Defined at top level,
// capturing nothing, so they serialize for the task closure. Round-trip each
// through Java serialization NOW: a serialization failure is a notebook bug, not
// a lineage bug, and we want it loud and early.
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor, StatefulProcessorWithInitialState, TimeMode, TimerValues}
import spark.implicits._
import org.apache.spark.sql.functions.call_udf

val CAT = "databricks_ws.openlineage_demo"

val srcDf = spark.table(s"$CAT.raw_customers")
println("source schema:")
srcDf.printSchema()
println("source rows: " + srcDf.count())

// Registered, named Scala UDF - construct U (t_511e1153) proven shape.
val echoUdf = udf((s: String) => s.toUpperCase)
spark.udf.register("echoUpper", echoUdf)

class CountByCountry
    extends StatefulProcessor[String, (Int, String, String, String), (String, Int)] {
  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = ()

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(Int, String, String, String)],
      timerValues: TimerValues): Iterator[(String, Int)] =
    Iterator.single((key, inputRows.size))
}

class CountByCountryWithInit
    extends StatefulProcessorWithInitialState[String, (Int, String, String, String), (String, Int), (Int, String, String, String)] {
  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = ()

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(Int, String, String, String)],
      timerValues: TimerValues): Iterator[(String, Int)] =
    Iterator.single((key, inputRows.size))

  // The initial state only seeds state; we count from the data rows.
  override def handleInitialState(
      key: String,
      initialState: (Int, String, String, String),
      timerValues: TimerValues): Unit = ()
}

def roundtripSer(obj: AnyRef): String = {
  val bos = new java.io.ByteArrayOutputStream()
  val oos = new java.io.ObjectOutputStream(bos)
  oos.writeObject(obj)
  oos.close()
  val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bos.toByteArray))
  val back = ois.readObject()
  back.getClass.getName + " (" + bos.size() + " bytes)"
}

println("roundtrip CountByCountry         : " + roundtripSer(new CountByCountry))
println("roundtrip CountByCountryWithInit : " + roundtripSer(new CountByCountryWithInit))

// COMMAND ----------

// Cell 3 - CONTROL untyped: select + write. Baseline: DIRECT/IDENTITY edges only.
// Every typed construct below must be read against this.
val controlDf = spark.table(s"$CAT.raw_customers").select($"customer_id", $"name")
println("=== optimized plan (expect plain Project, no typed nodes) ===")
println(controlDf.queryExecution.optimizedPlan.treeString)

controlDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_control_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_control_ol").count())

// COMMAND ----------

// Cell 4 - typed map: the core fan-in boundary. Expect INDIRECT/TRANSFORMATION
// fan-in (every output from every input), description `MapElements(Row -> Row), sameType`.
val typedMapDf = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .as[(Int, String, String, String)]
  .map(t => (t._4, t._1))
  .toDF("country", "customer_id")

println("=== optimized plan (expect MapElements between Serialize/Deserialize) ===")
println(typedMapDf.queryExecution.optimizedPlan.treeString)

typedMapDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_map_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_map_ol").count())

// COMMAND ----------

// Cell 5 - typed map over an untyped filter BELOW (pass-through descent,
// TypedBoundaryCompositionTest.filterBelowTypedNodeNoLongerSilencesFanIn):
// ds.filter(col-pred).map. The untyped Filter sits between MapElements and
// DeserializeToObject; the fan-in walk descends through it. Fan-in + the
// untyped FilterVisitor signal as ever.
val mapFilterDf = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .filter($"customer_id" > 100) // untyped Column predicate
  .as[(Int, String, String, String)]
  .map(t => (t._4, t._1))
  .toDF("country", "customer_id")

println("=== optimized plan (expect MapElements > Filter > DeserializeToObject) ===")
println(mapFilterDf.queryExecution.optimizedPlan.treeString)

mapFilterDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_map_filter_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_map_filter_ol").count())

// COMMAND ----------

// Cell 6 - typed map with untyped filter ABOVE (map.filter): Filter between
// SerializeFromObject and MapElements - the other descent direction
// (TypedBoundaryCompositionTest.filterBetweenBoundaryAndTypedNodeNoLongerSilencesFanIn).
// Expect fan-in edges AND the untyped filter FILTER dataset dep to coexist.
val mapFilterAboveDf = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"country")
  .as[(Int, String, String)]
  .map(t => (t._3, t._1))
  .toDF("country", "customer_id")
  .filter($"customer_id" > 100) // untyped, above the boundary

println("=== optimized plan (expect Filter > SerializeFromObject > MapElements) ===")
println(mapFilterAboveDf.queryExecution.optimizedPlan.treeString)

mapFilterAboveDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_map_filter_above_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_map_filter_above_ol").count())

// COMMAND ----------

// Cell 7 - mapPartitions (flatMap lowers to the same node). Single-node boundary.
val mapPartitionsDf = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"country")
  .as[(Int, String, String)]
  .mapPartitions { it => it.map(t => (t._3, t._1)) }
  .toDF("country", "customer_id")

println("=== optimized plan (expect MapPartitions) ===")
println(mapPartitionsDf.queryExecution.optimizedPlan.treeString)

mapPartitionsDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_mappartitions_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_mappartitions_ol").count())

// COMMAND ----------

// Cell 8 - chained boundaries: map -> mapPartitions. One SerializeFromObject
// boundary, two typed nodes; the fan-in fires ONCE with the chained description
// `MapPartitions(-> Row), MapElements(Row -> Row), sameType`
// (TypedBoundaryCompositionTest.mapThenMapPartitionsChainsDescriptions).
val chainedDf = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"country")
  .as[(Int, String, String)]
  .map(t => t)
  .mapPartitions { it => it.map(t => (t._3, t._1)) }
  .toDF("country", "customer_id")

println("=== optimized plan (expect MapPartitions > MapElements > DeserializeToObject) ===")
println(chainedDf.queryExecution.optimizedPlan.treeString)

chainedDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_chained_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_chained_ol").count())

// COMMAND ----------

// Cell 9 - groupByKey().mapGroups: the v0 typed grouping (t_511e1153 row D +
// t_bfa6430b control). GROUP_BY dataset dep resolves through AppendColumns to
// the real input columns; fan-in rides on the field edges.
val gkbSrc = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .as[(Int, String, String, String)]

val mapGroupsDf = gkbSrc
  .groupByKey(t => t._4)
  .mapGroups((country: String, it: Iterator[(Int, String, String, String)]) => (country, it.size))
  .toDF("country", "customer_count")

println("=== optimized plan (expect MapGroups over AppendColumns) ===")
println(mapGroupsDf.queryExecution.optimizedPlan.treeString)

mapGroupsDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_mapgroups_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_mapgroups_ol").count())

// COMMAND ----------

// Cell 10 - composition: map THEN groupByKey().mapGroups. The preceding typed
// map makes the intermediate object-typed, so the grouping's append node is
// AppendColumnsWithObject; TypedGroupByVisitor must resolve the key through the
// grouping operator's valueDeserializer (the real a/b/c/d columns), not the
// synthetic `value` attr. GROUP_BY lands on real columns.
val mapThenGroupDf = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .as[(Int, String, String, String)]
  .map(t => t)
  .groupByKey(t => t._4)
  .mapGroups((country: String, it: Iterator[(Int, String, String, String)]) => (country, it.size))
  .toDF("country", "customer_count")

println("=== optimized plan (expect MapGroups > AppendColumnsWithObject > MapElements) ===")
println(mapThenGroupDf.queryExecution.optimizedPlan.treeString)

mapThenGroupDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_map_then_group_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_map_then_group_ol").count())

// COMMAND ----------

// Cell 11 - groupByKey().flatMapGroupsWithState (v0 stateful, batch-executable
// per SparkStrategies.scala:958 generateSparkPlanForBatchQueries). The node is
// matched by TypedGroupByVisitor (GROUP_BY) and is IN the fan-in walk's opaque
// list. GROUP_BY + fan-in.
import org.apache.spark.sql.streaming.GroupStateTimeout

val fmGwsDf = gkbSrc
  .groupByKey(t => t._4)
  .flatMapGroupsWithState(
    OutputMode.Append(),
    GroupStateTimeout.NoTimeout())(
    (country: String, it: Iterator[(Int, String, String, String)], s: org.apache.spark.sql.streaming.GroupState[Int]) =>
      Iterator.single((country, it.size)))
  .toDF("country", "customer_count")

println("=== optimized plan (expect FlatMapGroupsWithState over AppendColumns) ===")
println(fmGwsDf.queryExecution.optimizedPlan.treeString)

fmGwsDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_fmGws_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_fmGws_ol").count())

// COMMAND ----------

// Cell 12 - groupByKey().transformWithState NO initial state (v2 stateful, batch;
// t_bfa6430b). GROUP_BY resolves left-side only + fan-in naming TransformWithState.
val twsDf = gkbSrc
  .groupByKey(t => t._4)
  .transformWithState(new CountByCountry(), TimeMode.None(), OutputMode.Append())
  .toDF("country", "customer_count")

println("=== optimized plan (expect TransformWithState over AppendColumns) ===")
println(twsDf.queryExecution.optimizedPlan.treeString)

twsDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_tws_no_init_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_tws_no_init_ol").count())

// COMMAND ----------

// Cell 13 - transformWithState WITH initial state: BinaryNode, left = grouped
// data, right = initial state's own plan. The grouping edges must resolve
// through the LEFT side only (4 GROUP_BY, not 8); initial-state columns ARE
// inputs for the fan-in.
val initState = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .as[(Int, String, String, String)]
  .groupByKey(t => t._4)

val twsInitDf = gkbSrc
  .groupByKey(t => t._4)
  .transformWithState(
    new CountByCountryWithInit(),
    TimeMode.None(),
    OutputMode.Append(),
    initState)
  .toDF("country", "customer_count")

println("=== optimized plan (expect TransformWithState over TWO AppendColumns branches) ===")
println(twsInitDf.queryExecution.optimizedPlan.treeString)

twsInitDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_tws_init_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_tws_init_ol").count())

// COMMAND ----------

// Cell 14 - typed filter (t_511e1153 regression, unflagged visitor): expect the
// identity fields DIRECT as ever PLUS INDIRECT/FILTER dataset dep naming every
// deserialized column.
val typedFilterDf = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .as[(Int, String, String, String)]
  .filter(t => t._1 > 100) // FilterFunction — TypedFilter
  .toDF("customer_id", "name", "email", "country")

println("=== optimized plan (expect TypedFilter above DeserializeToObject) ===")
println(typedFilterDf.queryExecution.optimizedPlan.treeString)

typedFilterDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_typed_filter_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_typed_filter_ol").count())

// COMMAND ----------

// Cell 15 - registered UDF (t_511e1153 construct U): INDIRECT/TRANSFORMATION
// with description `UDF: echoUpper`, masking=false. Control for cell 16.
val udfDf = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", call_udf("echoUpper", $"name").as("shouty"))

println("=== optimized plan (expect Project with ScalaUDF/Registered UDF expr) ===")
println(udfDf.queryExecution.optimizedPlan.treeString)

udfDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_udf_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_udf_ol").count())

// COMMAND ----------

// Cell 16 - full compose: typed map + untyped filter + UDF projection — fan-in
// AND UDF expression edge in one plan, proving the two visitors compose.
val composeDf = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .filter($"customer_id" > 100)
  .as[(Int, String, String, String)]
  .map(t => (t._4, t._2))
  .toDF("country", "name")
  .select($"country", call_udf("echoUpper", $"name").as("shouty"))

println("=== optimized plan (expect Project > MapElements > Filter > DeserializeToObject) ===")
println(composeDf.queryExecution.optimizedPlan.treeString)

composeDf.write.mode("overwrite").saveAsTable(s"$CAT.comprehensive_compose_ol")
println("rows written: " + spark.table(s"$CAT.comprehensive_compose_ol").count())

// COMMAND ----------

// Cell 17 - flush and export the emitted events. FileTransport appends with
// java.nio, which FUSE volume mounts do not support, so the sink is on driver
// local disk and is copied out here.
val src = "file:/local_disk0/ol_events/events.jsonl"
val dstDir = "dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/comprehensive/"
val dst = dstDir + "events_comprehensive_fanin_true.jsonl"

val local = new java.io.File("/local_disk0/ol_events/events.jsonl")
println("local sink exists=" + local.exists() + " bytes=" + (if (local.exists()) local.length() else 0L))

dbutils.fs.mkdirs(dstDir)
dbutils.fs.cp(src, dst, recurse = false)
println("copied to " + dst)
println(dbutils.fs.ls(dstDir).mkString("\n"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Expected event structure (what assert_events.py checks)
// MAGIC
// MAGIC | table | dataset events | column-lineage fields |
// MAGIC |---|---|---|
// MAGIC | comprehensive_control_ol | START/COMPLETE | `customer_id`,`name` DIRECT/IDENTITY |
// MAGIC | comprehensive_map_ol | 2 | `country`,`customer_id` fan-in 4x2 |
// MAGIC | comprehensive_map_filter_ol | 2 | fan-in survives Filter |
// MAGIC | comprehensive_map_filter_above_ol | 2 | fan-in + FILTER coexist |
// MAGIC | comprehensive_mappartitions_ol | 2 | `MapPartitions(-> Row)` desc |
// MAGIC | comprehensive_chained_ol | 2 | single fan-in, chained desc |
// MAGIC | comprehensive_mapgroups_ol | 2 | fan-in + GROUP_BY on 4 cols |
// MAGIC | comprehensive_map_then_group_ol | 2 | GROUP_BY resolves via AppendColumnsWithObject |
// MAGIC | comprehensive_fmGws_ol | 2 | GROUP_BY + fan-in (v0 stateful) |
// MAGIC | comprehensive_tws_no_init_ol | 2 | GROUP_BY + fan-in, desc names TransformWithState |
// MAGIC | comprehensive_tws_init_ol | 2 | 4 GROUP_BY left-side only + fan-in |
// MAGIC | comprehensive_typed_filter_ol | 2 | identity + INDIRECT/FILTER |
// MAGIC | comprehensive_udf_ol | 2 | `UDF: echoUpper` INDIRECT |
// MAGIC | comprehensive_compose_ol | 2 | fan-in + UDF edge together |