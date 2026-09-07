# Databricks notebook source
# MAGIC %md
# MAGIC # spark-ol pandas operator column-lineage E2E (mapInPandas / mapInArrow / applyInPandas / applyInArrow)
# MAGIC
# MAGIC Real-cluster validation that the pandas operator family emits the expected
# MAGIC `columnLineage` facet on DBR 17.3 LTS (Spark 4.0.0). One notebook, one cluster run,
# MAGIC every construct exercised through a real Unity Catalog write and captured as real
# MAGIC OpenLineage events.
# MAGIC
# MAGIC | construct | node | visitor | signal |
# MAGIC |---|---|---|---|
# MAGIC | untyped select (control) | Project | ProjectVisitor | DIRECT/IDENTITY |
# MAGIC | mapInPandas | MapInPandas | PandasOperatorVisitor | INDIRECT/TRANSFORMATION fan-in |
# MAGIC | mapInArrow | MapInArrow | PandasOperatorVisitor | INDIRECT/TRANSFORMATION fan-in |
# MAGIC | applyInPandas groupBy(col) | FlatMapGroupsInPandas | PandasOperatorVisitor | fan-in + INDIRECT/GROUP_BY |
# MAGIC | applyInPandas expression key (ceil) | FlatMapGroupsInPandas | PandasOperatorVisitor | GROUP_BY through Alias |
# MAGIC | applyInArrow | FlatMapGroupsInArrow | PandasOperatorVisitor | fan-in + INDIRECT/GROUP_BY |
# MAGIC
# MAGIC The pandas fan-in ships **unflagged** (exact marshalling, not a pessimistic boundary
# MAGIC fan-in), so `typedBoundaryFanInEnabled` is irrelevant to these nodes; it is left ON here
# MAGIC to match the comprehensive run's config surface.

# COMMAND ----------

# Cell 1 - environment, listener registration, jar provenance.
print("spark.version              = " + spark.version)
try:
    print("scala.util.Properties      = " + spark._jvm.scala.util.Properties.versionNumberString())
except Exception as e:
    print("scala.util.Properties      = <unreadable: %s>" % e)
print("spark.extraListeners       = " + spark.conf.get("spark.extraListeners", "<unset>"))

for k in [
    "spark.openlineage.transport.type",
    "spark.openlineage.transport.location",
    "spark.openlineage.namespace",
    "spark.openlineage.columnLineage.datasetLineageEnabled",
    "spark.openlineage.columnLineage.typedBoundaryFanInEnabled",
    "spark.openlineage.columnLineage.typedBoundaryFanInMaxEdges",
]:
    print("%-58s = " % k + spark.conf.get(k, "<unset>"))

# Stale-jar disproof: every class this run depends on, loadable ONLY from the staged
# spark-ol jar. Prints each code source, so jar provenance is in the run output.
for cn in [
    "io.openlineage.spark.agent.OpenLineageSparkListener",
    "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.PandasOperatorVisitor",
    "io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedGroupByVisitor",
    "io.openlineage.client.transports.FileTransport",
]:
    try:
        c = spark._jvm.Class.forName(cn)
        loc = c.getProtectionDomain().getCodeSource().getLocation().toString()
        print(cn + "\n    -> " + loc)
    except Exception as e:
        print(cn + "\n    -> LOAD FAILED: " + type(e).__name__ + ": " + str(e))

# Is the listener actually attached to the live SparkContext?
# LiveListenerBus has TWO methods named "listeners" (0-arg and String-arg overloads);
# pin the parameterless one.
try:
    scala_sc = spark.sparkContext._jsc.sc()
    bus_method = scala_sc.getClass().getDeclaredMethod("listenerBus")
    bus_method.setAccessible(True)
    bus = bus_method.invoke(scala_sc)
    m = [x for x in bus.getClass().getMethods()
         if x.getName() == "listeners" and x.getParameterCount() == 0][0]
    m.setAccessible(True)
    listeners = m.invoke(bus)
    names = [l.getClass().getName() for l in listeners]
    print("attached listeners containing 'openlineage': " +
          ", ".join([n for n in names if "openlineage" in n.lower()]))
except Exception as e:
    print("listener reflection failed: " + type(e).__name__ + ": " + str(e))

# COMMAND ----------

# MAGIC %sh
# MAGIC echo "=== jars staged by the init script ==="
# MAGIC ls -la /databricks/jars/zzz_* 2>&1 | sed 's#/databricks/jars/##'
# MAGIC echo "=== NoClassDefFoundError in driver log? ==="
# MAGIC grep -c "NoClassDefFoundError" /databricks/driver/logs/*.log 2>/dev/null || echo "no log match"
# MAGIC echo "=== OpenLineage listener log lines ==="
# MAGIC grep -n "OpenLineage" /databricks/driver/logs/*.log 2>/dev/null | head -10 || true

# COMMAND ----------

# Cell 2 - source + pandas UDFs. Defined at top level, capturing nothing, so they
# serialize for the task closure. A real applyInPandas needs an actual Python UDF;
# pyspark is on the standard-cluster classpath and pyarrow ships on DBR.
from pyspark.sql.functions import col, ceil
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyarrow as pa
import pandas as pd

CAT = "databricks_ws.openlineage_demo"

src_df = spark.table(f"{CAT}.raw_customers")
print("source schema:")
src_df.printSchema()
print("source rows: " + str(src_df.count()))

# Output schema for every pandas construct: o1 (string), o2 (int).
out_schema = StructType([
    StructField("o1", StringType()),
    StructField("o2", IntegerType()),
])

# mapInPandas: iterator of pandas DataFrames -> iterator of DataFrames.
# Input columns (customer_id, name, country); output o1=country, o2=customer_id.
def map_pandas(iterator):
    for pdf in iterator:
        out = pdf[["country", "customer_id"]].copy()
        out.columns = ["o1", "o2"]
        yield out

# mapInArrow: iterator of pyarrow RecordBatches -> iterator of RecordBatches.
# Convert to pandas, project, convert back to a RecordBatch.
def map_arrow(iterator):
    for batch in iterator:
        pdf = batch.to_pandas()
        out = pdf[["country", "customer_id"]].copy()
        out.columns = ["o1", "o2"]
        yield pa.RecordBatch.from_pandas(out)

# applyInPandas: (key, single pandas DataFrame) -> a single pandas DataFrame. The
# grouped pdf omits the grouping key column, so only "name" is guaranteed present
# across all three grouping cells (country key, ceil(customer_id/2) key, country key).
def apply_pandas(key, pdf):
    out = pdf[["name"]].copy()
    out.columns = ["o1"]
    out["o2"] = 1
    out["o2"] = out["o2"].astype("int32")
    return out

# applyInArrow: (key, single pyarrow RecordBatch) -> a single pyarrow.Table.
def apply_arrow(key, batch):
    pdf = batch.to_pandas()
    out = pdf[["name"]].copy()
    out.columns = ["o1"]
    out["o2"] = 1
    out["o2"] = out["o2"].astype("int32")
    return pa.Table.from_pandas(out)

print("pandas UDFs registered (map_pandas, map_arrow, apply_pandas, apply_arrow)")

# COMMAND ----------

# Cell 3 - CONTROL untyped select + write. Baseline: DIRECT/IDENTITY edges only.
control_df = spark.table(f"{CAT}.raw_customers").select("customer_id", "name")
print("=== optimized plan (expect plain Project, no pandas nodes) ===")
print(control_df._jdf.queryExecution().optimizedPlan().treeString())

control_df.write.mode("overwrite").saveAsTable(f"{CAT}.pandas_control_ol")
print("rows written: " + str(spark.table(f"{CAT}.pandas_control_ol").count()))

# COMMAND ----------

# Cell 4 - mapInPandas. Expect INDIRECT/TRANSFORMATION fan-in (o1,o2 <- all 3 inputs),
# description "MapInPandas(map_pandas)".
map_pandas_df = (spark.table(f"{CAT}.raw_customers")
                 .select("customer_id", "name", "country")
                 .mapInPandas(map_pandas, schema=out_schema))

print("=== optimized plan (expect MapInPandas over LogicalRDD/Relation) ===")
print(map_pandas_df._jdf.queryExecution().optimizedPlan().treeString())

map_pandas_df.write.mode("overwrite").saveAsTable(f"{CAT}.pandas_mapinpandas_ol")
print("rows written: " + str(spark.table(f"{CAT}.pandas_mapinpandas_ol").count()))

# COMMAND ----------

# Cell 5 - mapInArrow. Same fan-in shape on the MapInArrow node.
map_arrow_df = (spark.table(f"{CAT}.raw_customers")
                .select("customer_id", "name", "country")
                .mapInArrow(map_arrow, schema=out_schema))

print("=== optimized plan (expect MapInArrow) ===")
print(map_arrow_df._jdf.queryExecution().optimizedPlan().treeString())

map_arrow_df.write.mode("overwrite").saveAsTable(f"{CAT}.pandas_mapinarrow_ol")
print("rows written: " + str(spark.table(f"{CAT}.pandas_mapinarrow_ol").count()))

# COMMAND ----------

# Cell 6 - applyInPandas groupBy(real column). Expect fan-in PLUS dataset-level
# INDIRECT/GROUP_BY on the grouping column (country).
apply_pandas_df = (spark.table(f"{CAT}.raw_customers")
                   .select("customer_id", "name", "country")
                   .groupBy("country")
                   .applyInPandas(apply_pandas, schema=out_schema))

print("=== optimized plan (expect FlatMapGroupsInPandas over Project) ===")
print(apply_pandas_df._jdf.queryExecution().optimizedPlan().treeString())

apply_pandas_df.write.mode("overwrite").saveAsTable(f"{CAT}.pandas_applyinpandas_ol")
print("rows written: " + str(spark.table(f"{CAT}.pandas_applyinpandas_ol").count()))

# COMMAND ----------

# Cell 7 - applyInPandas expression key (ceil). GROUP_BY must resolve through the Alias
# in the Project below to the underlying column.
apply_expr_df = (spark.table(f"{CAT}.raw_customers")
                 .select("customer_id", "name", "country")
                 .groupBy(ceil(col("customer_id") / 2))
                 .applyInPandas(apply_pandas, schema=out_schema))

print("=== optimized plan (expect FlatMapGroupsInPandas with Alias grouping key) ===")
print(apply_expr_df._jdf.queryExecution().optimizedPlan().treeString())

apply_expr_df.write.mode("overwrite").saveAsTable(f"{CAT}.pandas_applyexpr_ol")
print("rows written: " + str(spark.table(f"{CAT}.pandas_applyexpr_ol").count()))

# COMMAND ----------

# Cell 8 - applyInArrow. Same as cell 6 on the FlatMapGroupsInArrow node.
apply_arrow_df = (spark.table(f"{CAT}.raw_customers")
                  .select("customer_id", "name", "country")
                  .groupBy("country")
                  .applyInArrow(apply_arrow, schema=out_schema))

print("=== optimized plan (expect FlatMapGroupsInArrow over Project) ===")
print(apply_arrow_df._jdf.queryExecution().optimizedPlan().treeString())

apply_arrow_df.write.mode("overwrite").saveAsTable(f"{CAT}.pandas_applyinarrow_ol")
print("rows written: " + str(spark.table(f"{CAT}.pandas_applyinarrow_ol").count()))

# COMMAND ----------

# Cell 9 - flush and export the emitted events. FileTransport appends with java.nio,
# which FUSE volume mounts do not support, so the sink is on driver-local disk and is
# copied out here.
src = "file:/local_disk0/ol_events/events.jsonl"
dst_dir = "dbfs:/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx/pandas/"
dst = dst_dir + "events_pandas_fanin_true.jsonl"

import os
local = "/local_disk0/ol_events/events.jsonl"
print("local sink exists=" + str(os.path.exists(local)) +
      " bytes=" + str(os.path.getsize(local) if os.path.exists(local) else 0))

dbutils.fs.mkdirs(dst_dir)
dbutils.fs.cp(src, dst, recurse=False)
print("copied to " + dst)
print("\n".join(str(x) for x in dbutils.fs.ls(dst_dir)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected event structure (what assert_events.py checks)
# MAGIC
# MAGIC | table | dataset events | column-lineage fields |
# MAGIC |---|---|---|
# MAGIC | pandas_control_ol | START/COMPLETE | `customer_id`,`name` DIRECT/IDENTITY |
# MAGIC | pandas_mapinpandas_ol | 2 | `o1`,`o2` fan-in 3x2, desc `MapInPandas(map_pandas)` |
# MAGIC | pandas_mapinarrow_ol | 2 | same, desc `MapInArrow(map_arrow)` |
# MAGIC | pandas_applyinpandas_ol | 2 | fan-in + GROUP_BY on `country` |
# MAGIC | pandas_applyexpr_ol | 2 | fan-in + GROUP_BY through Alias to `customer_id` |
# MAGIC | pandas_applyinarrow_ol | 2 | fan-in + GROUP_BY on `country` |