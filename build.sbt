name := "spark-apache-iceberg"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1" % "compile",
  "org.apache.spark" %% "spark-sql" % "3.0.1" % "compile",
  "org.apache.iceberg" % "iceberg-spark3-runtime" % "0.12.0",
  "org.apache.hive" % "hive-metastore" % "3.1.2"
)