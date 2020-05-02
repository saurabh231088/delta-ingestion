package org.thompson.ingestion

import java.sql.Timestamp

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.thompson.ingestion.model.{Employee, IngestionRecord, KafkaObject, NewEmployee}

class IngestionConsumerSuite extends FunSuite with BeforeAndAfterAll {

  implicit val spark =
    SparkSession.builder().appName("stream_test").master("local[*]").getOrCreate()
  import spark.implicits._
  val baseFilePath = "src/test/resources/stream-source"
  val initialFileLocation = s"$baseFilePath/initial"
  val tableOptions = Map(
    "source_name" -> "dunder_mifflin",
    "table_name" -> "employee"
  )

  test("write stream records as delta") {
    val updateFileLocation = s"${baseFilePath}/update"
    val outputLocation = s"$baseFilePath/output"
    upsert(initialFileLocation, outputLocation)
    assert(
      spark.read.format("delta")
        .load(s"${outputLocation}/employee").collect().length == 3)

    val employees = List(
      Employee(1, "Dwight Schrute", "sales"),
      Employee(2, "Jim Halpret", "sales"),
      Employee(3, "Andy Bernard", "sales"),
      Employee(4, "Phyllis", "sales")
    )

    getEmployeeDF(employees)
      .write
      .json(updateFileLocation)
    upsert(updateFileLocation, outputLocation)

    assert(
      spark.read.format("delta")
        .load(s"${outputLocation}/employee").collect().length == 4)
  }

  test("check for updated schema") {
    val newSchemaLocation = s"${baseFilePath}/new_schema"
    val outputLocation = s"${baseFilePath}/new_schema_output"

    upsert(initialFileLocation, outputLocation)

    val employees = List(
      NewEmployee(1, "Dwight Schrute", "sales", "123"),
      NewEmployee(2, "Jim Halpret", "sales", "123"),
      NewEmployee(3, "Andy Bernard", "sales", "123")
    )

    getNewEmployeeDF(employees)
      .write
      .json(newSchemaLocation)
    upsert(newSchemaLocation, outputLocation)

    assert(
      spark.read.format("delta")
        .load(s"$outputLocation/employee").select("id", "name", "department", "mobile")
        .collect() ==
        employees.toDF("id", "name", "department", "mobile").collect())
  }


  override protected def beforeAll(): Unit = {
    val employees = List(
      Employee(1, "Dwight", "sales"),
      Employee(2, "Jim", "sales"),
      Employee(3, "Andy", "sales"))

    getEmployeeDF(employees)
      .write
      .json(initialFileLocation)

    super.beforeAll()
  }

  def upsert(source: String, sink: String)(implicit spark: SparkSession) = {
    val schema = spark.read.json(source).schema
    val initialDF: DataFrame = spark.readStream.schema(schema).json(source)
    RecordConsumer
      .upsert(initialDF, sink)
      .trigger(Trigger.Once())
      .start()
      .awaitTermination()
  }

  def getNewEmployeeDF(employees: List[NewEmployee]) = {
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

  def getEmployeeDF(employees: List[Employee]) = {
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
//    fs.delete(new Path(baseFilePath), true)
    super.afterAll()
  }
}
