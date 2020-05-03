package org.thompson.ingestion

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.thompson.ingestion.model.TableInfo

object BatchIngestUtil {

  val RECORD_INFO_KEY = "recordInfo"
  val PAYLOAD_INFO_KEY = "payload"
  val TABLE_NAME_KEY = "table_name"
  val EVENT_VALUE_KEY = "value"

  def splitDF(dataFrame: DataFrame, tableInfoDF: Dataset[TableInfo])(
      implicit spark: SparkSession): Array[(TableInfo, DataFrame)] = {
    import spark.implicits._
    val df: DataFrame = spark.read.json(dataFrame.map(_.getAs[String](EVENT_VALUE_KEY)))
    val tables = df
      .select($"${RECORD_INFO_KEY}.${TABLE_NAME_KEY}")
      .distinct()
      .collect()
      .map(_.getAs[String](TABLE_NAME_KEY))
    tables.map(tableName => {
      val tableInfo = tableInfoDF.filter(_.tableName.equalsIgnoreCase(tableName)).collect()
      assert(tableInfo.length > 0, s"Unable to find ${tableName} in table configs.")
      val tableDF = df.filter($"recordInfo".getField(TABLE_NAME_KEY) === tableName)
      (tableInfo.head, tableDF.select($"${PAYLOAD_INFO_KEY}.*"))
    })
  }

  def deltaUpsert(dataFrame: DataFrame, basePath: String, tableInfo: TableInfo, fs: FileSystem) = {
    val primaryKeyColumns = tableInfo.primaryKeys
    val targetTableAlias = "events"
    val stageTableAlias = "updates"
    val tablePath = tableInfo.getOutputPath(basePath)

    val mergeCondition = primaryKeyColumns
      .map(key => s"${targetTableAlias}.${key} = ${stageTableAlias}.${key}")
      .mkString(" AND ")

    if (fs.exists(new Path(tablePath))) {
      val table = DeltaTable.forPath(tablePath)
      table.toDF.sparkSession.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
      table
        .as(targetTableAlias)
        .merge(dataFrame.as(stageTableAlias), mergeCondition)
        .whenMatched
        .updateAll()
        .whenNotMatched
        .insertAll()
        .execute()
    } else {
      dataFrame
        .write
        .format("delta").save(tablePath)
    }
  }
}
