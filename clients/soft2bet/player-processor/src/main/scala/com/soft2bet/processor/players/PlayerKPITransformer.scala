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
  PlayerKPI,
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
import com.jada.sqs.Body
import io.circe.Printer
import com.soft2bet.model.BonusUpdatedSQS
import com.soft2bet.model.Wagering
import com.soft2bet.model.WageringJSON
import org.apache.kafka.streams.processor.Punctuator
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
import java.util.UUID

class PlayerKPITransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    storeName: String
) extends Transformer[String, PlayerKPI, KeyValue[String, PlayerKPI]]
    with Punctuator {

  private var processorContext: ProcessorContext = _
  private var storePlayerKPIs: KeyValueStore[String, PlayerKPI] = _
  private val sender = new Sender(config, sqs, ueNorthSQS)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[PlayerKPITransformer])

  override def transform(
      key: String,
      value: PlayerKPI
  ): KeyValue[String, PlayerKPI] = {
    val stored = storePlayerKPIs.get(key)
    val playerToSave = Option(stored) match {
      case None => value
      case Some(inStore) => {
        inStore.copy(
          GGRAmount = value.GGRAmount.orElse(inStore.GGRAmount),
          GGRAmountEUR = value.GGRAmountEUR.orElse(inStore.GGRAmountEUR),
          NGRAmount = value.NGRAmount.orElse(inStore.NGRAmount),
          NGRAmountEUR = value.NGRAmountEUR.orElse(inStore.NGRAmountEUR),
          BonusGGRRatio = value.BonusGGRRatio.orElse(inStore.BonusGGRRatio),
          BonusDepositRatio =
            value.BonusDepositRatio.orElse(inStore.BonusDepositRatio),
          NGRDepositRatio =
            value.NGRDepositRatio.orElse(inStore.NGRDepositRatio)
        )

      }
    }
    storePlayerKPIs.put(key, playerToSave)
    new KeyValue(key, value)
  }

  override def close(): Unit = {}

  override def punctuate(timestamp: Long): Unit = {
    if (Utils.canBeExecuted(timestamp)) {
      Sender.Brands.foreach(prefix => {
        pushFile(prefix, timestamp)
      })
    }
  }

  def pushFile(prefix: String, timestamp: Long): Unit = {
    logger.debugv(
      s"push file",
      Array(
        kv("prefix", prefix)
      ): _*
    )
    val players =
      storePlayerKPIs.prefixScan(s"${prefix}-", new StringSerializer())
    try {
      if (players.hasNext()) {
        val file = writeFile(prefix, players)
        expose(prefix, timestamp, file)
        file.toFile().delete()
      }
    } finally {
      players.close()
    }
    cleanStore(prefix)
  }

  def cleanStore(prefix: String): Unit = {
    val it = storePlayerKPIs.prefixScan(s"${prefix}-", new StringSerializer())
    try {
      while (it.hasNext()) {
        storePlayerKPIs.delete(it.next().key)
      }
    } finally {
      it.close()
    }
  }

  def writeFile(
      prefix: String,
      players: KeyValueIterator[String, PlayerKPI]
  ): Path = {
    val tmp = Files.createTempFile(s"p-${prefix}", ".json")
    val writer = new FileWriter(tmp.toFile())
    logger.debugv(
      s"start - write file",
      Array(
        kv("prefix", prefix),
        kv("tmp", tmp)
      ): _*
    )
    try {

      while (players.hasNext()) {
        val keyAndValue = players.next()
        val player = keyAndValue.value
        if (
          player.GGRAmount.isDefined ||
          player.GGRAmountEUR.isDefined ||
          player.NGRAmount.isDefined ||
          player.NGRAmountEUR.isDefined ||
          player.BonusGGRRatio.isDefined ||
          player.BonusDepositRatio.isDefined ||
          player.NGRDepositRatio.isDefined
        ) {
          val ld = PlayerKPIData(
            GGRAmount = player.GGRAmount,
            GGRAmountEUR = player.GGRAmountEUR,
            NGRAmount = player.NGRAmount,
            NGRAmountEUR = player.NGRAmountEUR,
            BonusGGRRatio = player.BonusGGRRatio,
            BonusDepositRatio = player.BonusDepositRatio,
            NGRDepositRatio = player.NGRDepositRatio
          )

          val ldJson = printer.print(
            PlayerKPIData.lifetimeDepositSQSEncoder.apply(ld)
          )

          val line =
            s"""{"id":"${player.player_id.get}","properties":${ldJson}}\n"""
          writer.write(line)
        }
      }
    } finally {
      logger.debugv(
        s"end - write file",
        Array(
          kv("prefix", prefix),
          kv("tmp", tmp)
        ): _*
      )
      writer.close()
    }

    tmp
  }

  def expose(prefix: String, timestamp: Long, file: Path): Unit = {
    val client = new BlobServiceClientBuilder()
      .connectionString(config.connectionString)
      .buildClient()
    val blobClient = client
      .getBlobContainerClient(config.outputContainerName)
      .getBlobClient(
        s"data/${prefix}/players/kpi-${UUID.randomUUID().toString()}.json"
      )
    blobClient.uploadFromFile(file.toString(), true)
    val blobSasPermission = new BlobSasPermission().setReadPermission(true)
    val expiryTime = OffsetDateTime.now().plusDays(7)
    val values =
      new BlobServiceSasSignatureValues(expiryTime, blobSasPermission)
    val url = s"${blobClient.getBlobUrl}?${blobClient.generateSas(values)}"
    val body =
      s"""
         |{
         |  "type": "BATCH_NOTIFICATION",
         |  "mappingSelector": "batch-import",
         |  "downloadUri": "$url"
         |}
         |""".stripMargin

    sender.sendToSQSByPrefix(
      body,
      url,
      prefix
    )
  }

  override def init(processorContext: ProcessorContext): Unit = {
    this.storePlayerKPIs = processorContext.getStateStore(storeName)
    processorContext.schedule(
      config.punctuator,
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }
}

case class PlayerKPIData(
    GGRAmount: Option[String] = None,
    GGRAmountEUR: Option[String] = None,
    NGRAmount: Option[String] = None,
    NGRAmountEUR: Option[String] = None,
    BonusGGRRatio: Option[String] = None,
    BonusDepositRatio: Option[String] = None,
    NGRDepositRatio: Option[String] = None
)

object PlayerKPIData {

  import io.circe.Encoder
  import io.circe.generic.semiauto._
  implicit val lifetimeDepositSQSEncoder: Encoder[PlayerKPIData] = deriveEncoder
}
