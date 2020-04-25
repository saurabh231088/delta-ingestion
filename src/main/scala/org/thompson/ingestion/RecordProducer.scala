package org.thompson.ingestion

import java.{util => ju}
import java.io.FileInputStream
import java.time.LocalDateTime
import java.sql.Timestamp

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.thompson.ingestion.model.{KafkaObject, IngestionRecord}
import org.json4s.DefaultFormats

object RecordProducer extends App {

  val propertiesFileName = args(0)
  val topicName = args(1)

  val kafkaProperties = new ju.Properties()
  kafkaProperties.load(new FileInputStream(propertiesFileName))
  val producer = new KafkaProducer[String, String](kafkaProperties)

  final case class Employee(id: Int, name: String, department: String)

  val employees = List(
    new Employee(1, "Dwight", "sales"),
    new Employee(2, "Jim", "sales"),
    new Employee(3, "Andy", "sales"))

  implicit val formats = DefaultFormats
  employees.foreach(employee => {
    val recordOptions = Map("source_name" -> "dunder_mifflin", "table_name" -> "employee")
    val kafkaObject = new KafkaObject[IngestionRecord[Employee]](
      new IngestionRecord[Employee](
        recordOptions,
        employee,
        Timestamp.valueOf(LocalDateTime.now())))
    producer.send(
      new ProducerRecord[String, String](topicName, kafkaObject.key, kafkaObject.value))
  })

  producer.close()

}
