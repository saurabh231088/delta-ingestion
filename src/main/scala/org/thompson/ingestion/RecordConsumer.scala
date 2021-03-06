package org.thompson.ingestion

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json4s.DefaultFormats
import org.thompson.ingestion.model.TableInfo
import org.thompson.ingestion.process.IngestionProcess

object RecordConsumer extends App {
  //spark properties
  val appName = "streaming_consumer"
  val master = "local[*]"

  //kafka properties
  val bootstrapServer = args(0)
  val topic = args(1)
  val tableInfoLocation = args(2)
  val baseOutputPath = args(3)

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

  import spark.implicits._
  val tableInfoDF = spark.read.json(tableInfoLocation).as[TableInfo]

  def upsert(dataFrame: DataFrame, tableInfoDS: Dataset[TableInfo])(
      implicit spark: SparkSession) = {
    implicit val formats = DefaultFormats

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    dataFrame.writeStream
      .foreachBatch((batch, _) => {
        BatchIngestUtil
          .splitDF(batch, tableInfoDS)
          .foreach(x => {
            val (tableInfo, df) = x
            IngestionProcess(
              baseDF = df,
              tableInfo = tableInfo,
              ingestAction = BatchIngestUtil.deltaUpsert,
              transformationFunction = None,
              validationFunctions = None
            ).ingest()
          })
      })
  }

  upsert(df, tableInfoDF)
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .start()
    .awaitTermination()

}
