package org.thompson.ingestion.model

import java.sql.Timestamp

/**
 *
 * @param recordInfo
 * @param payLoad
 * @param insertTime
 * @tparam T
 */
case class IngestionRecord[T](recordInfo: Map[String, String],
                              payLoad: T,
                              insertTime: Timestamp)
