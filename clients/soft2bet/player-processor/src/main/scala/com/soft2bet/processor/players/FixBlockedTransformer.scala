package com.soft2bet.processor.players

import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.sas.{
  BlobSasPermission,
  BlobServiceSasSignatureValues
}
import com.soft2bet.model.{Login, LoginJSON, Wagering, WageringJSON}
import io.circe.Printer
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{
  ProcessorContext,
  PunctuationType,
  Punctuator
}
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore}
import org.jboss.logging.Logger

import java.io.FileWriter
import java.nio.file.{Files, Path}
import java.time.{Instant, OffsetDateTime}
import com.soft2bet.model.PlayerStore
import java.util.UUID

class FixBlockedTransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    loginStore: String
) extends Transformer[String, PlayerStore, KeyValue[String, PlayerStore]]
    with Punctuator {

  private var store: KeyValueStore[String, PlayerStore] = _
  private val sender = new Sender(config, sqs, ueNorthSQS)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[FixBlockedTransformer])

  override def transform(
      key: String,
      value: PlayerStore
  ): KeyValue[String, PlayerStore] = {
    store.put(key, value)
    new KeyValue(key, value)
  }

  override def close(): Unit = {}

  override def punctuate(timestamp: Long): Unit = {
    Sender.Brands.foreach(prefix => {
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
    val elements = store.prefixScan(s"${prefix}-", new StringSerializer())
    try {
      if (elements.hasNext()) {
        val file = writeFile(prefix, elements)
        expose(prefix, timestamp, file)
        file.toFile().delete()
      }
    } finally {
      elements.close()
    }
  }

  def writeFile(
      prefix: String,
      login: KeyValueIterator[String, PlayerStore]
  ): Path = {
    val tmp = Files.createTempFile(s"fix-blocked-${prefix}", ".json")
    val writer = new FileWriter(tmp.toFile())
    logger.debugv(
      s"start - write file",
      Array(
        kv("prefix", prefix),
        kv("tmp", tmp)
      ): _*
    )
    try {

      while (login.hasNext()) {
        val keyAndValue = login.next()
        if (
          keyAndValue.value.player_id.isDefined && keyAndValue.value.is_blocked.isDefined
        ) {
          val id = keyAndValue.value.player_id.get
          val line =
            s"""{"id":"${id}","properties":{"isBlocked":"${keyAndValue.value.is_blocked.get}"}}\n"""
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
        s"data/${prefix}/login/login-${UUID.randomUUID().toString()}.json"
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
    store = processorContext.getStateStore(loginStore)
    processorContext.schedule(
      config.punctuator,
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }
}
