package org.thompson.ingestion

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object BatchIngestUtil {
  def batchIngest(dataFrame: DataFrame)(implicit spark: SparkSession): Array[(Row, DataFrame)] = {
    import spark.implicits._
    val df: DataFrame = spark.read.json(dataFrame.map(_.get(1).toString))
    val tables = df.select($"recordInfo.table_name").distinct().collect().map(_.getString(0))
    tables.map(tableName => {
      val tableDF = df.filter($"recordInfo".getField("table_name") === tableName)
      (
        tableDF.select($"recordInfo.*").head,
        tableDF.select($"payload.*"))
    })
  }
}
