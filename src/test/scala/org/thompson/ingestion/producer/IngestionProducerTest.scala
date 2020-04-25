package org.thompson.ingestion.producer

import java.sql.Timestamp
import java.time.LocalDateTime

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterEach
import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.DefaultFormats
import org.thompson.ingestion.model.{Employee, IngestionRecord, KafkaObject}

class IngestionProducerSuite extends FunSuite with BeforeAndAfterEach {

  val mockProducer =
    new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())

  val employees = List(
    new Employee(1, "Dwight", "sales"),
    new Employee(2, "Jim", "sales"),
    new Employee(3, "Andy", "sales"))

  val TEST_TOPIC = "test"

  override def beforeEach(): Unit = {
    employees.foreach(employee => {
      val recordOptions = Map("source_name" -> "dunder_mifflin", "table_name" -> "employee")
      val kafkaObject = new KafkaObject[IngestionRecord[Employee]](
        new IngestionRecord[Employee](
          recordOptions,
          employee,
          Timestamp.valueOf(LocalDateTime.now())))
      mockProducer.send(
        new ProducerRecord[String, String](TEST_TOPIC, kafkaObject.key, kafkaObject.value))
    })
  }

  override def afterEach(): Unit = {
    mockProducer.clear()
  }

  implicit val formats = DefaultFormats

  test(s"producer should have written ${employees.size} records.") {
    assert(mockProducer.history.size == 3)
  }
}
