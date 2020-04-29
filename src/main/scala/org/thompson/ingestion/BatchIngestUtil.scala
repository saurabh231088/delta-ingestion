package org.thompson.ingestion

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object BatchIngestUtil {

  val RECORD_INFO_KEY = "recordInfo"
  val PAYLOAD_INFO_KEY = "payload"
  val TABLE_NAME_KEY = "table_name"
  val EVENT_VALUE_KEY = "value"

  def batchIngest(dataFrame: DataFrame)(implicit spark: SparkSession): Array[(Row, DataFrame)] = {
    import spark.implicits._
    val df: DataFrame = spark.read.json(dataFrame.map(_.getAs[String](EVENT_VALUE_KEY)))
    val tables = df.select($"${RECORD_INFO_KEY}.${TABLE_NAME_KEY}")
      .distinct().collect().map(_.getAs[String](TABLE_NAME_KEY))
    tables.map(tableName => {
      val tableDF = df.filter($"recordInfo".getField(TABLE_NAME_KEY) === tableName)
      (
        tableDF.select($"${RECORD_INFO_KEY}.*").head,
        tableDF.select($"${PAYLOAD_INFO_KEY}.*"))
    })
  }
}
