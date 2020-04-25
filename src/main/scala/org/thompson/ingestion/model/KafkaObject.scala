package org.thompson.ingestion.model

import org.json4s.Formats
import org.json4s.jackson.Serialization.write

case class KafkaObject[T <: ModelWithKey](key: String, value: String) {

  def this(t: T)(implicit format: Formats) = this(t.getKey, write(t))
}
