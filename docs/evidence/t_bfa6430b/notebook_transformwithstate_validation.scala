// Databricks notebook source
// MAGIC %md
// MAGIC # spark-ol e2e TransformWithState column-lineage validation (kanban t_bfa6430b)
// MAGIC
// MAGIC Runs our own `spark-ol` build (`Spark4,scala_2.13.16`, rewritten `temp/3`)
// MAGIC on DBR 17.3 LTS / Spark 4.0.0 / **Scala 2.13** against real Unity Catalog
// MAGIC tables and captures OpenLineage events for `groupByKey().transformWithState`
// MAGIC (arbitrary state API v2) — the construct that was completely invisible to
// MAGIC column lineage before this ticket.
// MAGIC
// MAGIC Executed as a BATCH query: SPARK-46865 added batch support for
// MAGIC TransformWithState in Spark 4.0 (BasicOperators ->
// MAGIC TransformWithStateExec.generateSparkPlanForBatchQueries, isStreaming=false,
// MAGIC per-partition RocksDB store in a temp dir), so no streaming source is needed.
// MAGIC
// MAGIC One cell per construct so a failure localises. Every cell prints the
// MAGIC optimized plan node it depends on, so a missing edge can be told apart
// MAGIC from a missing plan node.

// COMMAND ----------

// Cell 1 - environment, listener registration, stale-jar disproof, serialization self-test.
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

// Stale-jar disproof: TransformWithStateReflector exists ONLY in the build under
// test (added by t_bfa6430b). If the staged jar were an older build, this load
// fails. Also print the code source so the jar provenance is in the run output.
Seq(
  "io.openlineage.spark.agent.OpenLineageSparkListener",
  "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TransformWithStateReflector",
  "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedGroupByVisitor",
  "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedBoundaryFanInVisitor",
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
val attached = {
  val busField = sc.getClass.getDeclaredMethod("listenerBus")
  busField.setAccessible(true)
  val bus = busField.invoke(sc)
  val m = bus.getClass.getMethods.find(_.getName == "listeners").get
  m.setAccessible(true)
  val l = m.invoke(bus).asInstanceOf[java.util.List[_]]
  import scala.collection.JavaConverters._
  l.asScala.map(_.getClass.getName).toList
}
println("attached listeners containing 'openlineage': " +
  attached.filter(_.toLowerCase.contains("openlineage")).mkString(", "))

// COMMAND ----------

// Cell 2 - the stateful processors. Defined at top level of the cell, capturing
// nothing, so they serialize for the task closure. Round-trip each through Java
// serialization NOW: a serialization failure is a notebook bug, not a lineage
// bug, and we want it loud and early, not as a mystery task failure.
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor, StatefulProcessorWithInitialState, TimeMode, TimerValues}
import spark.implicits._

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

// MAGIC %sh
// MAGIC echo "=== jars staged by the init script ==="
// MAGIC ls -la /databricks/jars/zzz_* 2>&1 | sed 's#/databricks/jars/##'
// MAGIC echo "=== NoClassDefFoundError in driver log? ==="
// MAGIC grep -c "NoClassDefFoundError" /databricks/driver/logs/*.log 2>/dev/null || echo "no log match"
// MAGIC echo "=== OpenLineage listener log lines ==="
// MAGIC grep -n "OpenLineage" /databricks/driver/logs/*.log 2>/dev/null | head -10 || true

// COMMAND ----------

// Cell 4 - CONSTRUCT TWS-1: groupByKey().transformWithState(processor, TimeMode.None(),
// OutputMode.Append()) over a batch table — NO initial state. This was completely
// invisible to column lineage before t_bfa6430b: neither TypedGroupByVisitor nor
// TypedBoundaryFanInVisitor matched the TransformWithState node.
// Expect: 4 x INDIRECT/GROUP_BY dataset edges (one per input column, the
// over-breadth TypedGroupByVisitor documents) + fan-in TRANSFORMATION edges with
// a description naming TransformWithState.
val CAT = "databricks_ws.openlineage_demo"

val custDs = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .as[(Int, String, String, String)]

val twsDf = custDs
  .groupByKey(t => t._4)
  .transformWithState(new CountByCountry(), TimeMode.None(), OutputMode.Append())
  .toDF("country", "customer_count")

println("=== optimized plan (expect TransformWithState over AppendColumns) ===")
println(twsDf.queryExecution.optimizedPlan.treeString)

twsDf.write.mode("overwrite").saveAsTable(s"$CAT.tws_no_init_ol")
println("rows written: " + spark.table(s"$CAT.tws_no_init_ol").count())
spark.table(s"$CAT.tws_no_init_ol").show()

// COMMAND ----------

// Cell 5 - CONSTRUCT TWS-2: the same grouping WITH an initial state (a second
// KeyValueGroupedDataset over the same source, but a distinct AppendColumns and
// distinct exprIds). The node is a BinaryNode: left = grouped data, right = the
// initial state's own plan. The grouping edges must resolve through the LEFT
// side only (4, not 8); the initial state's columns ARE inputs for the fan-in.
val initState = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .as[(Int, String, String, String)]
  .groupByKey(t => t._4)

val twsInitDf = custDs
  .groupByKey(t => t._4)
  .transformWithState(
    new CountByCountryWithInit(),
    TimeMode.None(),
    OutputMode.Append(),
    initState)
  .toDF("country", "customer_count")

println("=== optimized plan (expect TransformWithState over TWO AppendColumns branches) ===")
println(twsInitDf.queryExecution.optimizedPlan.treeString)

twsInitDf.write.mode("overwrite").saveAsTable(s"$CAT.tws_init_ol")
println("rows written: " + spark.table(s"$CAT.tws_init_ol").count())

// COMMAND ----------

// Cell 6 - CONTROL mapGroups: the same groupByKey shape through the v0 API that
// was ALREADY covered before this ticket. Same source, same key lambda, same
// output shape — if TWS-1 shows the same edge structure as this control, the new
// visitor matches the established semantics.
val mapGroupsDf = custDs
  .groupByKey(t => t._4)
  .mapGroups((country: String, it: Iterator[(Int, String, String, String)]) => (country, it.size))
  .toDF("country", "customer_count")

println("=== optimized plan (expect MapGroups over AppendColumns) ===")
println(mapGroupsDf.queryExecution.optimizedPlan.treeString)

mapGroupsDf.write.mode("overwrite").saveAsTable(s"$CAT.typed_groupby_tws_ol")
println("rows written: " + spark.table(s"$CAT.typed_groupby_tws_ol").count())

// COMMAND ----------

// Cell 7 - CONTROL untyped groupBy().agg(): the equivalent UNTYPED grouping,
// which AggregateVisitor has always covered. Pins that the TWS GROUP_BY edges
// are the typed counterpart of an established signal, not a novel shape.
val untypedDf = spark.table(s"$CAT.raw_customers")
  .groupBy("country")
  .count()
  .select($"country", $"count".cast("int").as("customer_count"))

println("=== optimized plan (expect HashAggregate, no typed nodes) ===")
println(untypedDf.queryExecution.optimizedPlan.treeString)

untypedDf.write.mode("overwrite").saveAsTable(s"$CAT.untyped_groupby_tws_ol")
println("rows written: " + spark.table(s"$CAT.untyped_groupby_tws_ol").count())

// COMMAND ----------

// Cell 8 - flush and export the emitted events. FileTransport appends with
// java.nio, which FUSE volume mounts do not support, so the sink is on driver
// local disk and is copied out here.
val src = "file:/local_disk0/ol_events/events.jsonl"
val dst = "dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/tws/events_tws_fanin_true.jsonl"

val local = new java.io.File("/local_disk0/ol_events/events.jsonl")
println("local sink exists=" + local.exists() + " bytes=" + (if (local.exists()) local.length() else 0L))

dbutils.fs.mkdirs("dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/tws/")
dbutils.fs.cp(src, dst, recurse = false)
println("copied to " + dst)
println(dbutils.fs.ls("dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/tws/").mkString("\n"))