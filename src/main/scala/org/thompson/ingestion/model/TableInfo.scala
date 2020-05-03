package org.thompson.ingestion.model

case class TableInfo(
    sourceName: String,
    tableName: String,
    tableType: String,
    primaryKeys: List[String],
    sortKeys: List[String]) {
  def getOutputPath(basePath: String) = s"$basePath/$sourceName/$tableName"
}
