package com.jada.processor

import org.apache.kafka.common.Cluster
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import java.nio.charset.Charset
import org.apache.kafka.streams.processor.StreamPartitioner
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.serialization.Serializer

object PlayerPartitioner {
  val Separator = "#"
  val CharsetKey = Charset.forName("UTF-8")

  val UUIDFrom = "00000000-0000-0000-0000-000000000000"
  val UUIDTo = "ZZZZZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZZZZZZZZZ"

}

class PlayerStreamPartitioner(val serializer: Serializer[String])
    extends StreamPartitioner[String, JsonNode] {

  val partitioner = new PlayerPartitioner()

  override def partition(
      topic: String,
      key: String,
      value: JsonNode,
      numPartitions: Int
  ): Integer = {

    Option(key) match {
      case None => null
      case Some(_) => {
        val keyInBytes = serializer.serialize(topic, key)
        partitioner.partition(
          topic,
          key,
          keyInBytes,
          value,
          null,
          null,
          numPartitions
        )
      }
    }
  }

}
class PlayerPartitioner extends DefaultPartitioner {

  override def partition(
      topic: String,
      key: Any,
      keyBytes: Array[Byte],
      value: Any,
      valueBytes: Array[Byte],
      cluster: Cluster,
      numPartitions: Int
  ): Int = {
    Option(key) match {
      case None => partition(topic, key, keyBytes, value, valueBytes, cluster)
      case Some(value) if value.isInstanceOf[String] => {
        val playerId =
          value.asInstanceOf[String].split(PlayerPartitioner.Separator)(0)
        super.partition(
          topic,
          playerId,
          playerId.getBytes(PlayerPartitioner.CharsetKey),
          value,
          valueBytes,
          cluster,
          numPartitions
        )
      }
      case _ =>
        super.partition(
          topic,
          key,
          keyBytes,
          value,
          valueBytes,
          cluster,
          numPartitions
        )
    }
  }

}
