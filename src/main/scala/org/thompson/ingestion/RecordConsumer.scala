package org.thompson.ingestion

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats

object RecordConsumer extends App {
  //spark properties
  val appName = "streaming_consumer"
  val master = "local[*]"

  //kafka properties
  val bootstrapServer = args(0)
  val topic = args(1)

  val outputLocation = args(2)

  //spark conf
  val sparkConf = new SparkConf()
  sparkConf.setMaster(master)
  sparkConf.setAppName(appName)
  sparkConf.set("spark.sql.streaming.checkpointLocation", "./checkpoint")

  implicit val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("subscribe", topic)
    .load()

  def upsert(dataFrame: DataFrame, outputLocation: String)(implicit spark: SparkSession) = {
    implicit val formats = DefaultFormats

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    dataFrame.writeStream
      .foreachBatch((batch, id) => {
        BatchIngestUtil
          .batchIngest(batch)
          .foreach(x => {
            val tableName = x._1.getAs[String]("table_name")
            val tablePath = s"${outputLocation}/${tableName}"
            deltaUpsert(x._2, tablePath, fs)
          })
      })
  }

  def deltaUpsert(dataFrame: DataFrame, tablePath: String, fs: FileSystem) = {
//    val targetColumns = dataFrame.columns
    val primaryKeyColumns = List("id")
//    val nonKeyColumns = targetColumns.diff(primaryKeyColumns)

    val targetTableAlias = "events"
    val stageTableAlias = "updates"

    val mergeCondition = primaryKeyColumns
      .map(key => s"${targetTableAlias}.${key} = ${stageTableAlias}.${key}")
      .mkString(" AND ")

//    val updateExpression =
//      nonKeyColumns.map(column => (column -> s"${stageTableAlias}.${column}")).toMap
//
//    val insertExpression =
//      targetColumns.map(column => (column -> s"${stageTableAlias}.${column}")).toMap

    if (fs.exists(new Path(tablePath))) {
      val table = DeltaTable.forPath(tablePath)
      table.toDF.sparkSession.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
      table
        .as(targetTableAlias)
        .merge(dataFrame.as(stageTableAlias), mergeCondition)
        .whenMatched
        .updateAll()
//        .updateExpr(updateExpression)
        .whenNotMatched
        .insertAll()
//        .insertExpr(insertExpression)
        .execute()
    } else {
      dataFrame
        .write
        .format("delta").save(tablePath)
    }
  }

  upsert(df, outputLocation)
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .start()
    .awaitTermination()

}
