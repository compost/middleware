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
import org.apache.kafka.streams.processor.{
  ProcessorContext,
  PunctuationType,
  Punctuator,
  StateStore
}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.w3c.dom.Node

import java.time.{Duration, Instant, LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import org.jboss.logging.Logger
import com.fasterxml.jackson.databind.json.JsonMapper
import com.jada.models.{
  Action,
  ActionTrigger,
  Body,
  Player,
  PlayerSQS,
  PlayerStatus,
  PlayerStatusBody,
  PlayerStore
}
import io.circe.Printer

import java.util.UUID
import io.circe.Encoder
import io.circe.Decoder
import com.jada.serdes.CirceSerdes
import io.circe._
import io.circe.generic.auto._

import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.kafka.streams.state.KeyValueIterator
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Path
import com.azure.storage.blob.BlobServiceClientBuilder
import java.time.OffsetDateTime
import com.azure.storage.blob.sas.BlobSasPermission
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues
import com.jada.models.WithdrawSuccessSQS
import com.jada.models.EmailVerificationSQS
import com.jada.models.DepositSQS
import com.jada.models.Login
import com.jada.models.LoginSQS
import com.jada.models.ForgotPasswordSQS

class LoginTransformer(
    config: ApplicationConfiguration,
    referentials: com.jada.Referentials,
    client: software.amazon.awssdk.services.sqs.SqsClient
) extends Transformer[String, Login, KeyValue[Unit, Unit]] {

  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[LoginTransformer])
  private var processorContext: ProcessorContext = _

  override def init(processorContext: ProcessorContext): Unit = {
    logger.debug(f"${config}")
    this.processorContext = processorContext
  }

  override def transform(key: String, v: Login): KeyValue[Unit, Unit] = {
    send(
      v.player_id.get,
      v.brand_id.get,
      "GENERIC_USER",
      "player_login",
      LoginSQS(v)
    )
    null
  }

  def send[T: Encoder](
      playerId: String,
      brandId: String,
      `type`: String,
      mappingSelector: String,
      value: T
  ): Unit = {
    val body = Body[T](
      `type`,
      playerId,
      computeMappingSelector(Option(brandId), mappingSelector),
      value
    )
    sendMessageToSQS(
      playerId,
      brandId,
      printer.print(Body.bodyEncoder[T].apply(body))
    )
  }

  def sendMessageToSQS(
      playerId: String,
      brandId: String,
      body: String
  ): Unit = {
    val queue = config.brandQueue.get(brandId)
    logger.debugv(
      "send message to sqs",
      Array(
        kv("playerId", playerId),
        kv("url", queue),
        kv("body", body)
      ): _*
    )
    try {
      Option(client.sendMessage(request => {
        request
          .queueUrl(queue)
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
              kv("url", queue),
              kv("body", body)
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
            kv("url", queue),
            kv("body", body),
            e
          ): _*
        )
        throw e
    }
  }

  def computeMappingSelector(brandId: Option[String], mp: String): String = {
    if (brandId.exists(v => config.mappingSelectorPrefix.contains(v))) {
      s"DEV_${mp}"
    } else {
      mp
    }
  }
  override def close(): Unit = {}
}
