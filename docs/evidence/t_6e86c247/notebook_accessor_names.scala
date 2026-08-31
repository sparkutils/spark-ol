// Databricks notebook source
// MAGIC %md
// MAGIC # Simplified typed-boundary description on Databricks
// MAGIC
// MAGIC Follow-up to `t_ddb87c95` / `t_6e86c247`. Verifies on a real cluster
// MAGIC that the typed-boundary fan-in description is the minimal
// MAGIC `MapElements(<in> -> <out>)` form: no `typed operation: ` prose prefix,
// MAGIC no `DeserializeToObject` terminal node, no accessor-name suffix.
// MAGIC
// MAGIC The description is now encoder-flavour-independent — a bean map and a
// MAGIC case-class map both render as `MapElements(Type -> Type)`. The fan-in
// MAGIC edge set (the actual lineage) is unchanged.
// MAGIC
// MAGIC Requires `typedBoundaryFanInEnabled=true` — the description under test
// MAGIC is carried on the fan-in edges, which ship dark.

// COMMAND ----------

// Cell 1 - environment + listener registration proof.
println("spark.version              = " + spark.version)
println("scala.util.Properties      = " + scala.util.Properties.versionNumberString)

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

// COMMAND ----------

// Cell 2 - the bean class, in its OWN cell. The Databricks REPL wraps a cell's
// classes, so an encoder derived in the same cell that defines the class can
// resolve against the wrapper; splitting definition from use is what makes the
// encoder reliable. @BeanProperty generates the JavaBean getters/setters that
// Encoders.bean introspects.
import scala.beans.BeanProperty

class PersonBean {
  @BeanProperty var name: String = _
  @BeanProperty var age: Int = _
  def this(name: String, age: Int) = { this(); this.name = name; this.age = age }
}

println("bean defined: " + classOf[PersonBean].getSimpleName)

// COMMAND ----------

// Cell 3 - CONSTRUCT B: bean-typed map. The description must be the minimal
// `MapElements(PersonBean -> PersonBean)` — no prefix, no terminal node, no
// accessor suffix.
import org.apache.spark.sql.Encoders

val CAT = "databricks_ws.openlineage_demo"

// createDataset takes the encoder as an implicit (curried) parameter, not a
// second explicit argument.
implicit val personBeanEnc: org.apache.spark.sql.Encoder[PersonBean] =
  Encoders.bean(classOf[PersonBean])

// The map's input must be a real table read, not a LocalRelation: a
// DeserializeToObject only appears at the bottom of the plan when the source is
// deserialized from storage. createDataset(Seq(...)) is a LocalRelation, which
// has no deserializer, so the fan-in would find no input and emit nothing.
spark.createDataset(Seq(new PersonBean("alice", 30), new PersonBean("bob", 40)))
  .write.mode("overwrite").saveAsTable(s"$CAT.typed_map_bean_src_ol")

val personDs = spark.table(s"$CAT.typed_map_bean_src_ol").as[PersonBean]

val beanMapDf = personDs.map(p => new PersonBean(p.getName, p.getAge + 1))

println("=== optimized plan (expect SerializeFromObject/MapElements/DeserializeToObject) ===")
println(beanMapDf.queryExecution.optimizedPlan.treeString)

beanMapDf.write.mode("overwrite").saveAsTable(s"$CAT.typed_map_bean_ol")
println("rows written: " + spark.table(s"$CAT.typed_map_bean_ol").count())

// COMMAND ----------

// Cell 4 - CONSTRUCT F (control): case-class map, unchanged from t_ddb87c95.
// The description must be the same minimal shape — `MapElements(PersonOl ->
// ContactOl)` — proving the description is encoder-flavour-independent.
case class PersonOl(customer_id: Int, name: String, email: String, country: String)
case class ContactOl(customer_id: Int, email_masked: String)

val personDs2 = spark.table(s"$CAT.raw_customers")
  .select($"customer_id", $"name", $"email", $"country")
  .as[PersonOl]

val caseClassMapDf = personDs2
  .map(p => ContactOl(p.customer_id, if (p.email == null) null else p.email.replaceAll("@.*", "@***")))

println("=== optimized plan (expect MapElements) ===")
println(caseClassMapDf.queryExecution.optimizedPlan.treeString)

caseClassMapDf.write.mode("overwrite").saveAsTable(s"$CAT.typed_map_caseclass_ol")
println("rows written: " + spark.table(s"$CAT.typed_map_caseclass_ol").count())

// COMMAND ----------

// Cell 5 - flush and export. FileTransport appends with java.nio, which FUSE
// volume mounts do not support, so the sink is driver-local and copied out here.
val tag = spark.conf.get("spark.openlineage.columnLineage.typedBoundaryFanInEnabled", "unset")
val src = "file:/local_disk0/ol_events/events.jsonl"
val dst = s"dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/events_desc_simplified_$tag.jsonl"

val local = new java.io.File("/local_disk0/ol_events/events.jsonl")
println("local sink exists=" + local.exists() + " bytes=" + (if (local.exists()) local.length() else 0L))

dbutils.fs.cp(src, dst, recurse = false)
println("copied to " + dst)
println(dbutils.fs.ls("dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/").mkString("\n"))
