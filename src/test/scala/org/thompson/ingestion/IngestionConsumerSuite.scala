package org.thompson.ingestion

import java.sql.Timestamp

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.thompson.ingestion.model.{Employee, IngestionRecord, KafkaObject}

class IngestionConsumerSuite extends FunSuite with BeforeAndAfterAll {

  implicit val spark =
    SparkSession.builder().appName("stream_test").master("local[*]").getOrCreate()
  val baseFilePath = "src/test/resources/stream-source"
  val initialFileLocation = s"$baseFilePath/initial"
  val outputLocation = s"$baseFilePath/output"
  val tableOptions = Map(
    "source_name" -> "dunder_mifflin",
    "table_name" -> "employee"
  )

  test("write stream records as delta") {
    val updateFileLocation = "src/test/resources/stream-source/update"
    upsert(initialFileLocation)
    assert(
      spark.read.format("delta")
        .load(s"${outputLocation}/employee").collect().length == 3)

    val employees = List(
      Employee(1, "Dwight Schrute", "sales"),
      Employee(2, "Jim Halpret", "sales"),
      Employee(3, "Andy Bernard", "sales"),
      Employee(4, "Phyllis", "sales")
    )

    getKafkObjectDF(employees)
      .write
      .json(updateFileLocation)
    upsert(updateFileLocation)

    assert(
      spark.read.format("delta")
        .load(s"${outputLocation}/employee").collect().length == 4)
  }


  override protected def beforeAll(): Unit = {
    val employees = List(
      Employee(1, "Dwight", "sales"),
      Employee(2, "Jim", "sales"),
      Employee(3, "Andy", "sales"))

    getKafkObjectDF(employees)
      .write
      .json(initialFileLocation)

    super.beforeAll()
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

  def getKafkObjectDF(employees: List[Employee]) = {
    import spark.implicits._
    implicit val formats = DefaultFormats
    employees.map(
      employee =>
        new KafkaObject(
          IngestionRecord(
            tableOptions,
            employee,
            new Timestamp(System.currentTimeMillis()))))
      .toDS()
  }

  override protected def afterAll(): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(baseFilePath), true)
    super.afterAll()
  }
}
