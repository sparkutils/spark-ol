# spark-ol
Clean dependencies version of the OpenLineage Spark listeners

OL Spark is packaged against Scala version only, which has led to a number of runtime issues, this project re-packages against Spark versions directly so a Spark 4.1 version will not include Spark 3 artefacts.

Any additionaly dependency issues relating to Databricks are also handled.

This project is limited to packaging only, not to new functionality, and comes in two flavours - normal dependency and shaded.
