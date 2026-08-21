// Databricks notebook source
// MAGIC %md
// MAGIC # spark-ol e2e column-lineage validation (kanban t_511e1153)
// MAGIC
// MAGIC Runs our own `spark-ol` build (`Spark4,scala_2.13.16`, submodule `35cf91682`)
// MAGIC on DBR 17.3 LTS / Spark 4.0.0 / **Scala 2.13** against real Unity Catalog
// MAGIC tables and emits OpenLineage events through the `file` transport.
// MAGIC
// MAGIC One cell per construct so a failure localises. Every cell prints the
// MAGIC optimized plan node it depends on, so a missing edge can be told apart
// MAGIC from a missing plan node.

// COMMAND ----------

// Cell 1 - environment + listener registration proof.
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

// Hard proof the listener class and our new visitors are on the driver classpath.
Seq(
  "io.openlineage.spark.agent.OpenLineageSparkListener",
  "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.UserDefinedExpressionVisitor",
  "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedFilterVisitor",
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

// MAGIC %sh
// MAGIC echo "=== jars staged by the init script ==="
// MAGIC ls -la /databricks/jars/zzz_* 2>&1 | sed 's#/databricks/jars/##'
// MAGIC echo "=== NoClassDefFoundError / scala.Serializable in driver log? ==="
// MAGIC grep -c "NoClassDefFoundError" /databricks/driver/logs/*.log 2>/dev/null || echo "no log match"
// MAGIC grep -n "scala/Serializable\|scala.Serializable" /databricks/driver/logs/*.log 2>/dev/null | head -5 || true
// MAGIC echo "=== OpenLineage listener log lines ==="
// MAGIC grep -n "OpenLineage" /databricks/driver/logs/*.log 2>/dev/null | head -20 || true

// COMMAND ----------

// Cell 3 - CONSTRUCT A: registered (named) Spark UDF, with a plain upper() control
// in the SAME output table so the two edges appear in one event and can be
// compared directly. Expect masked_email -> INDIRECT/TRANSFORMATION "UDF: mask_email_ol"
// and country_upper -> DIRECT/TRANSFORMATION.
val CAT = "databricks_ws.openlineage_demo"

spark.udf.register("mask_email_ol", (s: String) => if (s == null) null else s.replaceAll("@.*", "@***"))

val udfDf = spark.table(s"$CAT.raw_customers")
  .selectExpr("customer_id", "mask_email_ol(email) as masked_email", "upper(country) as country_upper")

println("=== optimized plan (expect ScalaUDF + Upper) ===")
println(udfDf.queryExecution.optimizedPlan.treeString)

udfDf.write.mode("overwrite").saveAsTable(s"$CAT.udf_and_upper_ol")
println("rows written: " + spark.table(s"$CAT.udf_and_upper_ol").count())

// COMMAND ----------

// Cell 4 - CONSTRUCT B: typed filter(FilterFunction) -> TypedFilter.
// Tuple encoders come from spark.implicits._, so no case class is needed (case
// classes defined in a notebook cell are wrapped by the REPL and their encoders
// are unreliable).
import spark.implicits._

def custDs = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .as[(Int, String, String, String)]

val typedFilterDf = custDs.filter(t => t._4 != null && t._4.startsWith("G")).toDF()

println("=== optimized plan (expect TypedFilter) ===")
println(typedFilterDf.queryExecution.optimizedPlan.treeString)

typedFilterDf.write.mode("overwrite").saveAsTable(s"$CAT.typed_filter_ol")
println("rows written: " + spark.table(s"$CAT.typed_filter_ol").count())

// COMMAND ----------

// Cell 5 - CONSTRUCT C: groupByKey().mapGroups -> MapGroups over AppendColumns.
// The lambda needs explicit parameter types: mapGroups is overloaded
// (MapGroupsFunction vs Function2) so a pattern-matching anonymous function is
// ambiguous and does not compile.
val mapGroupsDf = custDs
  .groupByKey(t => t._4)
  .mapGroups((country: String, it: Iterator[(Int, String, String, String)]) => (country, it.size))
  .toDF("country", "customer_count")

println("=== optimized plan (expect MapGroups + AppendColumns) ===")
println(mapGroupsDf.queryExecution.optimizedPlan.treeString)

mapGroupsDf.write.mode("overwrite").saveAsTable(s"$CAT.typed_groupby_ol")
println("rows written: " + spark.table(s"$CAT.typed_groupby_ol").count())

// COMMAND ----------

// Cell 6 - CONSTRUCT D: typed map -> DeserializeToObject / MapElements /
// SerializeFromObject. With typedBoundaryFanInEnabled=false this must emit NO
// fan-in edges; the same cell is re-run on a restarted cluster with the flag on.
val typedMapDf = custDs
  .map(t => (t._1, if (t._3 == null) null else t._3.toUpperCase))
  .toDF("customer_id", "email_upper")

println("=== optimized plan (expect SerializeFromObject/MapElements/DeserializeToObject) ===")
println(typedMapDf.queryExecution.optimizedPlan.treeString)

typedMapDf.write.mode("overwrite").saveAsTable(s"$CAT.typed_map_ol")
println("rows written: " + spark.table(s"$CAT.typed_map_ol").count())

// COMMAND ----------

// Cell 7 - CONTROL: untyped upper() only, no UDF anywhere in the plan. Every
// edge here must stay DIRECT; if this comes back INDIRECT then the UDF result in
// cell 3 proves nothing (everything was downgraded).
val controlDf = spark.table(s"$CAT.raw_customers")
  .selectExpr("customer_id", "upper(name) as name_upper", "concat(country, '-x') as country_tag")

println("=== optimized plan (expect Upper + Concat, no ScalaUDF) ===")
println(controlDf.queryExecution.optimizedPlan.treeString)

controlDf.write.mode("overwrite").saveAsTable(s"$CAT.upper_control_ol")
println("rows written: " + spark.table(s"$CAT.upper_control_ol").count())

// COMMAND ----------

// Cell 8 - flush and export the emitted events. FileTransport appends with
// java.nio, which FUSE volume mounts do not support, so the sink is on driver
// local disk and is copied out here.
val tag = spark.conf.get("spark.openlineage.columnLineage.typedBoundaryFanInEnabled", "unset")
val src = "file:/local_disk0/ol_events/events.jsonl"
val dst = s"dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/events_fanin_$tag.jsonl"

val local = new java.io.File("/local_disk0/ol_events/events.jsonl")
println("local sink exists=" + local.exists() + " bytes=" + (if (local.exists()) local.length() else 0L))

dbutils.fs.cp(src, dst, recurse = false)
println("copied to " + dst)
println(dbutils.fs.ls("dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/").mkString("\n"))
