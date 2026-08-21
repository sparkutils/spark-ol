// Databricks notebook source
// MAGIC %md
// MAGIC # MapElements type-metadata validation on Databricks
// MAGIC
// MAGIC Follow-up to `t_511e1153`. Verifies @chris-twiner's review point on
// MAGIC sparkutils/OpenLineage#1 on a real cluster:
// MAGIC
// MAGIC > MapElements does however show inputs and output type:
// MAGIC > `argumentSchema: StructType`, `outputObjAttr: Attribute`,
// MAGIC > so we should be able to provide that information.
// MAGIC
// MAGIC Two things are proven separately here:
// MAGIC
// MAGIC 1. **The metadata exists on DBR's Catalyst.** Read directly off the live
// MAGIC    optimized plan by reflection (cells 3-5). Databricks forks Catalyst, so
// MAGIC    a `javap` against the vanilla 4.0.0 jar is not evidence about DBR.
// MAGIC 2. **Our visitor puts it in the emitted facet.** Read out of the actual
// MAGIC    OpenLineage `RunEvent` JSON (cells 6-9 write tables; cell 10 exports).
// MAGIC
// MAGIC Requires `typedBoundaryFanInEnabled=true` — the description under test is
// MAGIC carried on the fan-in edges, which ship dark.

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

Seq(
  "io.openlineage.spark.agent.OpenLineageSparkListener",
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

// Hard proof the DEPLOYED visitor is the version under test, not a stale jar:
// these three private methods only exist after the type-metadata change.
val visitorMethods =
  Class.forName("io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedBoundaryFanInVisitor")
    .getDeclaredMethods.map(_.getName).toSet
Seq("describeTypedOperator", "argumentTypeName", "objectTypeName").foreach { m =>
  println(f"visitor has $m%-22s = " + visitorMethods.contains(m))
}

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
// MAGIC echo "=== NoClassDefFoundError in driver log? ==="
// MAGIC grep -c "NoClassDefFoundError" /databricks/driver/logs/*.log 2>/dev/null || echo "no log match"

// COMMAND ----------

// Cell 3 - Does DBR's OWN Catalyst expose the two accessors Chris named?
// This is the claim that could not be checked locally: javap against a vanilla
// Apache 4.0.0 jar says nothing about Databricks' forked Catalyst. Read them by
// reflection off the real class on this runtime.
val meClass = Class.forName("org.apache.spark.sql.catalyst.plans.logical.MapElements")
val mpClass = Class.forName("org.apache.spark.sql.catalyst.plans.logical.MapPartitions")

def accessorReport(c: Class[_], names: Seq[String]): Unit = {
  val ms = c.getMethods.map(m => m.getName -> m.getReturnType.getName).toMap
  println("--- " + c.getSimpleName)
  names.foreach(n => println(f"  $n%-16s = " + ms.getOrElse(n, "<ABSENT>")))
}
accessorReport(meClass, Seq("argumentClass", "argumentSchema", "outputObjAttr"))
accessorReport(mpClass, Seq("argumentClass", "argumentSchema", "outputObjAttr"))

// COMMAND ----------

// Cell 4 - a case-class encoder: the case Chris's example actually describes
// (Person -> Contact), and the one where the metadata is worth having.
//
// Case classes must be defined in their OWN cell and used in a LATER cell:
// the Databricks REPL wraps a cell's classes, and an encoder derived in the same
// cell that defines the class can resolve against the wrapper. Splitting the
// definition from the use is what makes the encoder reliable.
case class PersonOl(customer_id: Int, name: String, email: String, country: String)
case class ContactOl(customer_id: Int, email_masked: String)
println("case classes defined: " + Seq(classOf[PersonOl], classOf[ContactOl]).map(_.getSimpleName).mkString(", "))

// COMMAND ----------

// Cell 5 - CONSTRUCT F: case-class typed map. PersonOl -> ContactOl.
// Print what the plan carries BEFORE looking at any event, so a missing
// description can be told apart from missing plan metadata.
import spark.implicits._
val CAT = "databricks_ws.openlineage_demo"

val personDs = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .as[PersonOl]

val caseClassMapDf = personDs
  .map(p => ContactOl(p.customer_id, if (p.email == null) null else p.email.replaceAll("@.*", "@***")))

println("=== optimized plan (expect SerializeFromObject/MapElements/DeserializeToObject) ===")
println(caseClassMapDf.queryExecution.optimizedPlan.treeString)

// The exact values our visitor reads, straight off this plan.
println("=== MapElements metadata as DBR reports it ===")
caseClassMapDf.queryExecution.optimizedPlan.foreach { node =>
  node.getClass.getSimpleName match {
    case "MapElements" =>
      val c = node.getClass
      val argClass = c.getMethod("argumentClass").invoke(node).asInstanceOf[Class[_]]
      val argSchema = c.getMethod("argumentSchema").invoke(node)
        .asInstanceOf[org.apache.spark.sql.types.StructType]
      val outAttr = c.getMethod("outputObjAttr").invoke(node)
        .asInstanceOf[org.apache.spark.sql.catalyst.expressions.Attribute]
      println("  argumentClass  = " + argClass.getName)
      println("  argumentSchema = " + argSchema.fieldNames.mkString("[", ", ", "]"))
      println("  outputObjAttr  = " + outAttr.name + " : " + outAttr.dataType)
    case "MapPartitions" =>
      val outAttr = node.getClass.getMethod("outputObjAttr").invoke(node)
        .asInstanceOf[org.apache.spark.sql.catalyst.expressions.Attribute]
      println("  MapPartitions.outputObjAttr = " + outAttr.name + " : " + outAttr.dataType)
    case _ => ()
  }
}

caseClassMapDf.write.mode("overwrite").saveAsTable(s"$CAT.typed_map_caseclass_ol")
println("rows written: " + spark.table(s"$CAT.typed_map_caseclass_ol").count())

// COMMAND ----------

// Cell 6 - CONSTRUCT D: tuple-encoder typed map, unchanged from t_511e1153.
// Kept because it is the shape the previous validated run used, and because it
// is the case where the added metadata degrades to positional names (_1, _2) -
// a real limitation that should be visible in the evidence, not hidden.
def custDs = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .as[(Int, String, String, String)]

val typedMapDf = custDs
  .map(t => (t._1, if (t._3 == null) null else t._3.toUpperCase))
  .toDF("customer_id", "email_upper")

println("=== optimized plan (expect MapElements) ===")
println(typedMapDf.queryExecution.optimizedPlan.treeString)

typedMapDf.write.mode("overwrite").saveAsTable(s"$CAT.typed_map_ol")
println("rows written: " + spark.table(s"$CAT.typed_map_ol").count())

// COMMAND ----------

// Cell 7 - CONSTRUCT G: mapPartitions -> MapPartitions, which has NO
// argumentSchema. Expect the argument side of the description to be empty
// rather than fabricated: "MapPartitions(-> ContactOl)".
val mapPartitionsDf = personDs
  .mapPartitions(it => it.map(p => ContactOl(p.customer_id, p.country)))

println("=== optimized plan (expect MapPartitions) ===")
println(mapPartitionsDf.queryExecution.optimizedPlan.treeString)

mapPartitionsDf.write.mode("overwrite").saveAsTable(s"$CAT.typed_mappartitions_ol")
println("rows written: " + spark.table(s"$CAT.typed_mappartitions_ol").count())

// COMMAND ----------

// Cell 8 - CONTROL: the UDF / builtin pair from t_511e1153, re-run so this
// evidence set also shows that naming the typed boundary did not disturb the
// UDF description or downgrade a builtin.
spark.udf.register("mask_email_ol", (s: String) => if (s == null) null else s.replaceAll("@.*", "@***"))

val udfDf = spark.table(s"$CAT.raw_customers")
  .selectExpr("customer_id", "mask_email_ol(email) as masked_email", "upper(country) as country_upper")

println("=== optimized plan (expect ScalaUDF + Upper) ===")
println(udfDf.queryExecution.optimizedPlan.treeString)

udfDf.write.mode("overwrite").saveAsTable(s"$CAT.udf_and_upper_ol")
println("rows written: " + spark.table(s"$CAT.udf_and_upper_ol").count())

// COMMAND ----------

// Cell 9 - CONSTRUCT C: groupByKey().mapGroups. Included as the known gap:
// MapGroups is NOT described by the type-metadata change, so its description
// must still read "typed operation: MapGroups, AppendColumns". Asserting the
// unchanged case keeps the scope of the change honest.
val mapGroupsDf = custDs
  .groupByKey(t => t._4)
  .mapGroups((country: String, it: Iterator[(Int, String, String, String)]) => (country, it.size))
  .toDF("country", "customer_count")

println("=== optimized plan (expect MapGroups + AppendColumns) ===")
println(mapGroupsDf.queryExecution.optimizedPlan.treeString)

mapGroupsDf.write.mode("overwrite").saveAsTable(s"$CAT.typed_groupby_ol")
println("rows written: " + spark.table(s"$CAT.typed_groupby_ol").count())

// COMMAND ----------

// Cell 10 - flush and export. FileTransport appends with java.nio, which FUSE
// volume mounts do not support, so the sink is driver-local and copied out here.
val tag = spark.conf.get("spark.openlineage.columnLineage.typedBoundaryFanInEnabled", "unset")
val src = "file:/local_disk0/ol_events/events.jsonl"
val dst = s"dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/events_typeinfo_fanin_$tag.jsonl"

val local = new java.io.File("/local_disk0/ol_events/events.jsonl")
println("local sink exists=" + local.exists() + " bytes=" + (if (local.exists()) local.length() else 0L))

dbutils.fs.cp(src, dst, recurse = false)
println("copied to " + dst)
println(dbutils.fs.ls("dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/").mkString("\n"))
