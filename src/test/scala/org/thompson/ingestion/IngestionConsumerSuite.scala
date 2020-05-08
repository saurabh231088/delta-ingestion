package org.thompson.ingestion

import java.sql.Timestamp

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.slf4j.LoggerFactory
import org.thompson.ingestion.model._

class IngestionConsumerSuite extends FunSuite with BeforeAndAfterAll {

  val baseFilePath = "src/test/resources/stream-source"

  //spark conf
  val sparkConf = new SparkConf()
  sparkConf.setMaster("local[*]")
  sparkConf.setAppName("stream_test")
  sparkConf.set("spark.sql.streaming.checkpointLocation", s"${baseFilePath}/checkpoint")

  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  import spark.implicits._
  val initialFileLocation = s"$baseFilePath/initial"
  val tableOptions = Map("source_name" -> "dunder_mifflin", "table_name" -> "employee")

  test("write stream records as delta") {

    val updateFileLocation = s"${baseFilePath}/update"
    val outputLocation = s"$baseFilePath/output"

    val tableInfoDS =
      List(TableInfo(outputLocation, "dunder_mifflin", "employee", "master", List("id"), List()))
        .toDS()

    upsert(initialFileLocation, tableInfoDS)
    assert(
      spark.read
        .format("delta")
        .load(s"${outputLocation}/dunder_mifflin/employee")
        .collect()
        .length == 3)

    val employees = List(
      Employee(1, "Dwight Schrute", "sales"),
      Employee(2, "Jim Halpret", "sales"),
      Employee(3, "Andy Bernard", "sales"),
      Employee(4, "Phyllis", "sales"))

    getEmployeeDF(employees).write
      .json(updateFileLocation)
    upsert(updateFileLocation, tableInfoDS)

    assert(
      spark.read
        .format("delta")
        .load(s"${outputLocation}/dunder_mifflin/employee")
        .collect()
        .length == 4)
  }

  test("check for updated schema") {
    val newSchemaLocation = s"${baseFilePath}/new_schema"
    val outputLocation = s"${baseFilePath}/new_schema_output"

    val tableInfoDS =
      List(TableInfo(outputLocation, "dunder_mifflin", "employee", "master", List("id"), List()))
        .toDS()

    logger.info("Upserting initial file.")
    upsert(initialFileLocation, tableInfoDS)

    val employees = List(
      NewEmployee(1, "Dwight Schrute", "sales", "123"),
      NewEmployee(2, "Jim Halpret", "sales", "123"),
      NewEmployee(3, "Andy Bernard", "sales", "123"),
      NewEmployee(4, "Phyllis", "sales", "123"))

    getNewEmployeeDF(employees).write
      .json(newSchemaLocation)
    logger.info("Upserting new schema file.")
    upsert(newSchemaLocation, tableInfoDS)

    assert(
      spark.read
        .format("delta")
        .load(s"$outputLocation/dunder_mifflin/employee")
        .as[NewEmployee]
        .collect()
        .sortBy(x => x.id)
        .toSeq ==
        employees.sortBy(_.id))
  }

  override protected def beforeAll(): Unit = {
    val employees = List(
      Employee(1, "Dwight", "sales"),
      Employee(2, "Jim", "sales"),
      Employee(3, "Andy", "sales"))

    getEmployeeDF(employees).write
      .json(initialFileLocation)

    super.beforeAll()
  }

  def upsert(source: String, tableInfoDS: Dataset[TableInfo])(implicit spark: SparkSession) = {
    val schema = spark.read.json(source).schema
    val initialDF: DataFrame = spark.readStream.schema(schema).json(source)
    RecordConsumer
      .upsert(initialDF, tableInfoDS)
      .trigger(Trigger.Once())
      .start()
      .awaitTermination()
  }

  def getNewEmployeeDF(employees: List[NewEmployee]) = {
    import spark.implicits._
    implicit val formats = DefaultFormats
    employees
      .map(
        employee =>
          new KafkaObject(
            IngestionRecord(tableOptions, employee, new Timestamp(System.currentTimeMillis()))))
      .toDS()
  }

  def getEmployeeDF(employees: List[Employee]) = {
    import spark.implicits._
    implicit val formats = DefaultFormats
    employees
      .map(
        employee =>
          new KafkaObject(
            IngestionRecord(tableOptions, employee, new Timestamp(System.currentTimeMillis()))))
      .toDS()
  }

  override protected def afterAll(): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(baseFilePath), true)
    super.afterAll()
  }
}
