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
  Wagering
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
import io.circe.Encoder
import io.circe.generic.semiauto._

import com.soft2bet.model.BonusUpdatedSQS
import com.soft2bet.model.Wagering
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
import com.soft2bet.model.WageringJSON
import java.util.UUID

class BalanceTransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    wageringStore: String
) extends Transformer[String, Wagering, KeyValue[String, Wagering]]
    with Punctuator {

  private var storeWagering: KeyValueStore[String, Wagering] = _
  private val sender = new Sender(config, sqs, ueNorthSQS, false, false)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[BalanceTransformer])

  override def transform(
      key: String,
      value: Wagering
  ): KeyValue[String, Wagering] = {
    val previous = Option(storeWagering.get(key))

    val newValue = previous match {
      case Some(oldWagering) => {
        if (
          oldWagering.bet_datetime
            .getOrElse("") < value.bet_datetime.getOrElse("")
        ) {
          storeWagering.put(key, value)
        }
      }
      case None => storeWagering.put(key, value)
    }
    null
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
    val wagerings =
      storeWagering.prefixScan(s"${prefix}-", new StringSerializer())
    try {
      if (wagerings.hasNext()) {
        val file = writeFile(prefix, wagerings)
        expose(prefix, timestamp, file)
        file.toFile().delete()
      }
    } finally {
      wagerings.close()
    }
    cleanStore(prefix)
  }

  def cleanStore(prefix: String): Unit = {
    val it = storeWagering.prefixScan(s"${prefix}-", new StringSerializer())
    try {
      while (it.hasNext()) {
        storeWagering.delete(it.next().key)
      }
    } finally {
      it.close()
    }
  }

  def writeFile(
      prefix: String,
      wagerings: KeyValueIterator[String, Wagering]
  ): Path = {
    val tmp = Files.createTempFile(s"ldc-balance-${prefix}", ".json")
    val writer = new FileWriter(tmp.toFile())
    logger.debugv(
      s"start - write file",
      Array(
        kv("prefix", prefix),
        kv("tmp", tmp)
      ): _*
    )
    try {

      while (wagerings.hasNext()) {
        val keyAndValue = wagerings.next()

        if (
          keyAndValue.value.Sent.isEmpty || keyAndValue.value.Sent
            .filterNot(v => v == true)
            .isDefined
        ) {
          val id = keyAndValue.value.player_id.get
          val wageringJson = printer.print(
            WageringJSON.wageringSQSEncoder.apply(
              WageringJSON(keyAndValue.value)
            )
          )
          val line =
            s"""{"id":"${id}","properties":${wageringJson}}\n"""
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
        s"data/${prefix}/balance/balance-${UUID.randomUUID.toString()}.json"
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
    storeWagering = processorContext.getStateStore(wageringStore)
    processorContext.schedule(
      config.punctuator,
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }
}
