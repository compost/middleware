package com.jada.processor

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonFactoryBuilder
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.jada.Common
import com.jada.configuration.ApplicationConfiguration
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.w3c.dom.Node

import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import org.jboss.logging.Logger

class LeovegasTriggerJourneyTransformer(
    config: ApplicationConfiguration,
    topicInput: String,
    countryUK: java.util.List[String],
    countryNL: java.util.List[String],
    countryBR: java.util.List[String],
    client: software.amazon.awssdk.services.sqs.SqsClient,
    startStopStoreName: String
) extends Transformer[String, JsonNode, KeyValue[Unit, Unit]] {

  private final val logger =
    Logger.getLogger(classOf[LeovegasTriggerJourneyTransformer])
  private var processorContext: ProcessorContext = _
  private var startStopStore: KeyValueStore[String, JsonNode] = _

  override def init(processorContext: ProcessorContext): Unit = {
    logger.debug(f"${config}")
    this.processorContext = processorContext
    this.startStopStore = processorContext.getStateStore(startStopStoreName)
  }

  override def transform(k: String, v: JsonNode): KeyValue[Unit, Unit] = {
    val action: Option[JsonNode] = Option(
      startStopStore.get(processorContext.applicationId())
    )

    logger.debugv(
      "tranform-input",
      Array(kv("action", action), kv("input", v)): _*
    )
    processorContext.topic() match {
      case config.topicCoordinator =>
        startStopStore.put(processorContext.applicationId(), v)
      case _
          if action
            .map(a => a.get("action"))
            .map(v => v.asText() == Common.stop)
            .getOrElse(false) =>
        logger.debug("nothing to do")
      case topicInput =>
        val (playerId, obj) = buildObject(v)
        if (playerId != null && obj != null) {
          logger.debugv(
            "tranform-output",
            Array(kv("playerId", playerId), kv("output", obj)): _*
          )
          val sqs = getSQS(obj.get("properties"))
          this.sendMessageToSQS(playerId, obj, sqs)
          new KeyValue(k, obj)
        } else {
          logger.debug(f"${v.toPrettyString()} skipped")
        }
    }
    null
  }

  def getSQS(properties: JsonNode): String = {
    (
      Option(properties.get("licenseuid")),
      Option(properties.get("country")),
      Option(properties.get("operator"))
    ) match {
      case (_, Some(v: JsonNode), _)
          if countryBR.contains(v.asText("").toUpperCase()) =>
        config.sqsQueueBR
      case (Some(v: JsonNode), _, _) if v.asText("").toUpperCase() == "IOM" =>
        config.sqsQueueLicenseuid
      case (_, Some(v: JsonNode), _)
          if countryUK.contains(v.asText("").toUpperCase()) =>
        config.sqsQueueUK
      case (_, Some(country: JsonNode), Some(operator: JsonNode))
          if countryNL.contains(country.asText("").toUpperCase()) && operator.asText("").toUpperCase() == "MGM" =>
        config.sqsQueueOperatorNL
      case (_, _, _) => config.sqsQueue
    }
  }

  def buildObject(v: JsonNode): (String, JsonNode) = {
    val mappingSelector = Option(v.get("mappingSelector"))
      .map(v => v.textValue())
      .getOrElse(throw new RuntimeException("missing mappinSelector"))
    val properties = buildProperties(v.get("parameters"))
    if (properties != null) {
      val (prefix, playerId) = Option(properties.get("notification_PLAYER_ID"))
        .map(v => ("", v.textValue()))
        .getOrElse(("unknown", "unknown"))

      val obj = JsonNodeFactory.instance.objectNode()
      obj.put("type", "GENERIC_USER")
      obj.put("mappingSelector", s"${prefix}${mappingSelector}")
      obj.put("contactId", playerId)
      obj.set("properties", properties) //TODO filter parameters?

      (playerId, obj)
    } else {
      (null, null)
    }
  }

  def buildProperties(parameters: JsonNode): JsonNode = {
    logger.debug(f"parameters: ${parameters}")
    if (parameters.isTextual()) {
      Option(parameters)
        .map(v => v.textValue())
        .map(v => {
          if (v.trim() == "") {
            null
          } else {
            val objMapper = new ObjectMapper()
            val node = objMapper.readValue(v, classOf[JsonNode])
            node
          }
        })
        .getOrElse(throw new RuntimeException("missing parameters"))
    } else {
      parameters
    }
  }

  def sendMessageToSQS(
      playerId: String,
      payload: JsonNode,
      url: String
  ): Unit = {
    val body = payload.toPrettyString()
    logger.debugv(
      "send message to sqs",
      Array(kv("playerId", playerId), kv("url", url), kv("payload", payload)): _*
    )
    try {
      Option(client.sendMessage(request => {
        request
          .queueUrl(url)
          .messageBody(body)
          .messageGroupId(config.sqsGroupId)
          .messageDeduplicationId(
            s"${playerId}-${java.util.UUID.randomUUID().toString}"
          )

      })) match {
        case Some(result) =>
          logger.infov(
            "sent message to sqs",
            Array(
              kv("playerId", playerId),
              kv("url", url),
              kv("payload", payload),
              kv("message", result.messageId())
            ): _*
          )
        case _ =>
      }
    } catch {
      case e: Exception =>
        logger.errorv(
          "unable to send message",
          Array(
            kv("playerId", playerId),
            kv("url", url),
            kv("payload", payload),
            e
          ): _*
        )
    }
  }
  override def close(): Unit = {}
}
