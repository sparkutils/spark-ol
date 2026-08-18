#!/bin/bash
# spark-ol e2e validation (kanban t_511e1153): stage our own OpenLineage Spark
# build plus its compile/runtime deps into /databricks/jars so the listener
# class, the OpenLineage java client and its own runtime deps are on the driver
# classpath before spark.extraListeners is instantiated.
#
# micrometer-* / HdrHistogram / LatencyUtils / jspecify are here because
# openlineage-java 1.51.0 declares micrometer-core as a RUNTIME dependency and
# OpenLineageSparkListener.initializeMetrics touches
# io.micrometer.core.instrument.MeterRegistry unconditionally. DBR 17.3 does not
# ship micrometer on the driver classpath, and our reactor resolves it as
# provided(optional) through Spark, so without these the driver dies at boot
# with ClassNotFoundException: io.micrometer.core.instrument.MeterRegistry.
set -euo pipefail

SRC=/Volumes/databricks_ws/default/hermes_libs/spark-ol-dbx

for j in \
  spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar \
  spark-ol_api_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar \
  openlineage-java-1.51.0.jar \
  openlineage-sql-java-1.51.0.jar \
  httpclient5-5.4.2.jar \
  httpcore5-5.4.2.jar \
  httpcore5-h2-5.3.3.jar \
  micrometer-core-1.17.0.jar \
  micrometer-commons-1.17.0.jar \
  micrometer-observation-1.17.0.jar \
  jspecify-1.0.0.jar \
  HdrHistogram-2.2.2.jar \
  LatencyUtils-2.0.3.jar \
  jackson-module-blackbird-2.15.3.jar \
  jackson-datatype-jdk8-2.15.3.jar \
  jackson-datatype-jsr310-2.15.3.jar \
  jackson-dataformat-yaml-2.15.3.jar ; do
  cp "$SRC/$j" "/databricks/jars/zzz_$j"
done

# The file transport writes with java.nio APPEND, which FUSE volume mounts do
# not support; keep the sink on driver-local disk and copy it out from the
# notebook afterwards.
mkdir -p /local_disk0/ol_events
chmod 777 /local_disk0/ol_events

ls -la /databricks/jars/zzz_* > /local_disk0/ol_events/init_script_jars.txt 2>&1 || true
