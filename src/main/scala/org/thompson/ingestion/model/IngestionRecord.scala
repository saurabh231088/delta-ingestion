package org.thompson.ingestion.model

import java.sql.Timestamp

/**
 * Ingestion record structure.
 *
 * @param recordInfo Information about the record.
 * @param payLoad    The record to be ingested.
 * @param insertTime The time when the record was inserted to queue.
 * @tparam T payload class.
 */
case class IngestionRecord[T](recordInfo: Map[String, String], payLoad: T, insertTime: Timestamp)
    extends ModelWithKey {

  val SOURCE_NAME_KEY = "source_name"
  val TABLE_NAME_KEY = "table_name"

  // check all required info are available
  val requiredRecordInfo = List(SOURCE_NAME_KEY, TABLE_NAME_KEY)
  require(requiredRecordInfo.forall(recordInfo.keys.toList.contains(_)))

  def getKey: String = s"${SOURCE_NAME_KEY}.${TABLE_NAME_KEY}"
}
