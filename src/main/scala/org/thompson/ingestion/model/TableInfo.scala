package org.thompson.ingestion.model

case class TableInfo(
    basePath: String,
    sourceName: String,
    tableName: String,
    tableType: String,
    primaryKeys: List[String],
    sortKeys: List[String]) {
}
