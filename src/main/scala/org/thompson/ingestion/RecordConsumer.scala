package org.thompson.ingestion

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

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

  implicit val formats = DefaultFormats

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("subscribe", topic)
    .load()

  import spark.implicits._

  val jsonDS = df.selectExpr("CAST(value AS STRING)").map(x => x.get(0).toString)
  val payloadDF = spark.read.json(jsonDS)
  val table = DeltaTable.forPath(spark, outputLocation)
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  if (!fs.exists(new Path(outputLocation))) {
    payloadDF
      .write
      .format("delta")
      .save(outputLocation)
  }

  getDataStreamWriter(payloadDF, outputLocation)
  .trigger(Trigger.ProcessingTime("5 seconds"))
    .start(outputLocation)
    .awaitTermination()

  def getDataStreamWriter(payload: DataFrame, targetFolder: String) = {
    payload
      .writeStream
      .foreachBatch((batch, id) => {
        table
          .as("events")
          .merge(batch.toDF().as("updates"), "events.id = updates.id")
          .whenMatched
          .updateExpr(Map("name" -> "updates.name"))
          .whenNotMatched
          .insertExpr(Map("id" -> "updates.id", "name" -> "updates.name"))
          .execute()
      })
  }
}
