package org.wq.mvn4flink.datastream.kafka

import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

/**
  * Created by wq on 16/9/19.
  */
object KafkaStringSchema extends SerializationSchema[String] with DeserializationSchema[String] {

  import org.apache.flink.api.common.typeinfo.TypeInformation
  import org.apache.flink.api.java.typeutils.TypeExtractor

  override def serialize(t: String): Array[Byte] = t.getBytes("UTF-8")

  override def isEndOfStream(t: String): Boolean = false

  override def deserialize(bytes: Array[Byte]): String = new String(bytes, "UTF-8")

  override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
}
