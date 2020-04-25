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
