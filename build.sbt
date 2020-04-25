name := "delta-ingestion"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % Test
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
