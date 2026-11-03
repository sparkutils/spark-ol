# spark-ol
Clean dependencies version of the OpenLineage Spark listeners

OL Spark is targeted against a specific Scala version only, which has led to a number of runtime issues as it attempts to support all Spark versions simultaneously.
This project re-packages against Spark versions directly so a Spark 4.1 version will not include incompatible Spark 3 artefacts.

Any additional dependency issues relating to Databricks are also handled.

This project is limited to packaging the sparkutils openlineage repo fork.

## Changes in 0.0.6

As of RC2, a proxy transport wrapper (enabled with spark.openlineage.useProxy true), which merges messages sending at least 3: START, RUNNING, COMPLETE with associated content.

#1 - UDF, map, flatmap and others transformation support, by default disabled, use spark.openlineage.columnLineage.typedBoundaryFanInEnabled true to enable

#4 - SparkSessions are used to cache messages for merging within the proxy transport, allowing Spark Connect usage.

> configuring 
> >spark.extraListeners io.openlineage.spark.agent.OpenLineageSparkListener,io.openlineage.client.transports.ProxyConnectCloseListener
> 
> with ProxyConnectCloseListener will trigger emmitAll for a Spark Connect client when the connect sparkSession.close() is called. 

#5 - spark.openlineage.parentRunId flows correctly from Spark Connect clients (the root / parent facets are the applicationId)
