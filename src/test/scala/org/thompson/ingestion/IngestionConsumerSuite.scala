package org.thompson.ingestion

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class IngestionConsumerSuite extends FunSuite with BeforeAndAfterEach {

  implicit val spark =
    SparkSession.builder().appName("stream_test").master("local[*]").getOrCreate()
  val outputLocation = "src/test/resources/stream-source/output"

  test("write stream records as delta") {
    val initialFileLocation = "src/test/resources/stream-source/initial"
    val updateFileLocation = "src/test/resources/stream-source/update"
    upsert(initialFileLocation)
    assert(
      spark.read.format("delta").load(s"${outputLocation}/employee").collect().length == 3)
    upsert(updateFileLocation)
    assert(
      spark.read.format("delta").load(s"${outputLocation}/employee").collect().length == 4)
  }

  def upsert(fileLocation: String)(implicit spark: SparkSession) = {
    val schema = spark.read.json(fileLocation).schema
    val initialDF: DataFrame = spark.readStream.schema(schema).json(fileLocation)
    RecordConsumer
      .upsert(initialDF, outputLocation)
      .trigger(Trigger.Once())
      .start()
      .awaitTermination()
  }

  override protected def afterEach(): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(outputLocation), true)
    super.afterEach()
  }
}
