package com.jada

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.SendMessageRequest

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.jboss.logging.Logger
import io.circe.syntax._

import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import scala.util.{Failure, Success, Try}
import io.circe.Printer
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
import java.time.ZonedDateTime
import com.jada.models.Wallet
import com.jada.processor.Sender

class BalanceTransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    balancesName: String
) extends Transformer[
      String,
      Wallet,
      KeyValue[Unit, Unit]
    ]
    with Punctuator {

  private var processorContext: ProcessorContext = _
  private var balances: KeyValueStore[String, String] = _
  private val sender = new Sender(config, sqs, ueNorthSQS)

  private final val logger =
    Logger.getLogger(classOf[BalanceTransformer])

  override def transform(
      key: String,
      value: Wallet
  ): KeyValue[Unit, Unit] = {
    val stored = balances.get(key)
    if (value.balance.isDefined) {
      balances.put(key, value.balance.get)
    }
    null
  }

  override def close(): Unit = {}

  override def punctuate(timestamp: Long): Unit = {
    config.brandQueue
      .keySet()
      .forEach(prefix => {
        pushFile(prefix, timestamp)
      })
  }

  def pushFile(prefix: String, timestamp: Long): Unit = {
    logger.debugv(
      s"push file",
      Array(
        kv("prefix", prefix)
      ): _*
    )
    val players = balances.prefixScan(s"${prefix}-", new StringSerializer())
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

  def writeFile(
      prefix: String,
      players: KeyValueIterator[String, String]
  ): Path = {
    val tmp = Files.createTempFile(s"ps-${prefix}", ".json")
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
        val playerId = keyAndValue.key.split("-", 2)
        val balance = keyAndValue.value
        val line =
          s"""{"id":"${playerId}","properties":{"balance":"${balance}"}}\n"""
        writer.write(line)
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

  def cleanStore(prefix: String): Unit = {
    val players = balances.prefixScan(s"${prefix}-", new StringSerializer())
    try {
      while (players.hasNext()) {
        balances.delete(players.next().key)
      }
    } finally {
      players.close()
    }
  }

  def expose(prefix: String, timestamp: Long, file: Path): Unit = {
    val client = new BlobServiceClientBuilder()
      .connectionString(config.connectionString)
      .buildClient()
    val blobClient = client
      .getBlobContainerClient(config.outputContainerName)
      .getBlobClient(
        s"data/${prefix}/players/segmentation-${timestamp}.json"
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

    sender.sendMessageToSQS(
      "none",
      prefix,
      body
    )
  }

  override def init(processorContext: ProcessorContext): Unit = {
    this.balances = processorContext.getStateStore(balancesName)
    processorContext.schedule(
      Duration.ofHours(1),
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }
}
