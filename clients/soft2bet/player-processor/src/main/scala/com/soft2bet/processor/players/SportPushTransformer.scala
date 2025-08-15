package com.soft2bet.processor.players

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.soft2bet.Common
import com.soft2bet.model.{
  AccountFrozen,
  Achievement,
  DOBTempFix,
  Login,
  Player,
  PlayerStore,
  Wallet
}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.jboss.logging.Logger
import io.circe.syntax._

import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import scala.util.{Failure, Success, Try}
import com.soft2bet.model.PlayerStoreSQS
import com.jada.sqs.Body
import io.circe.Printer
import com.soft2bet.model.BonusUpdatedSQS
import com.soft2bet.model.Wagering
import com.soft2bet.model.WageringJSON
import java.time.Duration
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.common.serialization.StringSerializer
import java.io.FileWriter
import java.nio.file.Files
import org.apache.kafka.streams.state.KeyValueIterator
import java.nio.file.Path
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.sas.BlobSasPermission
import java.time.OffsetDateTime
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues
import java.time.Instant
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import com.soft2bet.model.SportPush
import com.soft2bet.model.SportPushSQS
import com.soft2bet.model.SportPushSQS._

class SportPushTransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient
) extends Transformer[String, SportPush, KeyValue[String, SportPush]] {

  override def init(context: ProcessorContext): Unit = {}

  private val sender = new Sender(config, sqs, ueNorthSQS, false, false)

  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )

  override def transform(
      key: String,
      value: SportPush
  ): KeyValue[String, SportPush] = {

    val body = Body[SportPushSQS](
      "GENERIC_USER",
      value.player_id.get,
      s"sportsPush_${value.`type`.get}",
      SportPushSQS(value)
    )
    sender.sendToSQS(
      printer.print(Body.bodyEncoder[SportPushSQS].apply(body)),
      value.player_id.get,
      value.brand_id.get
    )
    new KeyValue(key, value)
  }

  override def close(): Unit = {}
}
