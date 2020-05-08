package org.thompson.ingestion.process

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.thompson.ingestion.model.TableInfo

case class IngestionProcess(
    baseDF: DataFrame,
    tableInfo: TableInfo,
    transformationFunction: Option[(DataFrame, TableInfo) => DataFrame],
    validationFunctions: Option[(DataFrame => (DataFrame, DataFrame), (DataFrame, TableInfo) => Unit)],
    ingestAction: (DataFrame, TableInfo) => Unit) {

  def ingest(): Unit = {
    val transformedDF = if(transformationFunction.nonEmpty) {
      transformationFunction.get.apply(baseDF, tableInfo)
    } else {
      baseDF
    }

    if(validationFunctions.nonEmpty) {
      val (splitFunction, invalidAction) = validationFunctions.get
      val (validDF, invalidDF) = splitFunction(transformedDF)
      ingestAction(validDF, tableInfo)
      invalidAction(invalidDF, tableInfo)
    } else {
      ingestAction(transformedDF, tableInfo)
    }
  }
}
