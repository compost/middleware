package com.jada.processor

import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Clock
import org.jboss.logging.Logger
import java.util.UUID

class CheckerSupplier(
    val clock: Clock,
    val mapper: ObjectMapper,
    val storeCheckerName: String,
    val storeDepositName: String
) extends ValueTransformerWithKeySupplier[String, JsonNode, List[String]] {

  def get(): ValueTransformerWithKey[String, JsonNode, List[String]] =
    new CheckerTransformer(
      clock,
      mapper,
      storeCheckerName,
      storeDepositName
    )
}

class CheckerTransformer(
    val clock: Clock,
    val mapper: ObjectMapper,
    val storeCheckerName: String,
    val storeDepositName: String
) extends ValueTransformerWithKey[String, JsonNode, List[String]] {

  var checker: CheckerInside = _

  private val logger = Logger.getLogger(classOf[CheckerTransformer])
  var context: ProcessorContext = _
  def init(context: ProcessorContext): Unit = {
    this.context = context
    val checkerStore: KeyValueStore[String, _] =
      context.getStateStore(storeCheckerName)
    this.checker = new CheckerInside(
      clock,
      mapper,
      context.getStateStore(storeCheckerName),
      context.getStateStore(storeDepositName),
      (key: String) => checkerStore.delete(key)
    )
  }

  def transform(readOnlyKey: String, value: JsonNode): List[String] = {
    val traceId = UUID.randomUUID()
    logger.debugv(
      s"transform ${readOnlyKey} - ${traceId}",
      Array(
        kv("key", readOnlyKey),
        kv("traceId", traceId),
        kv("value", value),
        kv("partition", context.partition()),
        kv("topic", context.topic())
      ): _*
    )
    this.checker.check(readOnlyKey, traceId)
  }

  def close(): Unit = {}
}
