package com.soft2bet.processor.players

import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.sas.{
  BlobSasPermission,
  BlobServiceSasSignatureValues
}
import com.soft2bet.model._
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
import com.soft2bet.model.PlayerStoreSQS
import java.util.UUID

class MissingDataTransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    storeName: String
) extends Transformer[String, PlayerStore, KeyValue[String, PlayerStore]]
    with Punctuator {

  private var store: KeyValueStore[String, PlayerStore] = _
  private val sender = new Sender(config, sqs, ueNorthSQS)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[MissingDataTransformer])

  override def transform(
      key: String,
      value: PlayerStore
  ): KeyValue[String, PlayerStore] = {
    if (Sender.CasinoInfinity.contains(value.brand_id.getOrElse(""))) {
      store.put(key, value)
    }
    new KeyValue(key, value)
  }

  override def close(): Unit = {}

  override def punctuate(timestamp: Long): Unit = {
    pushFile(Sender.CasinoInfinityPrefix, timestamp)
  }

  def pushFile(prefix: String, timestamp: Long): Unit = {
    logger.debugv(
      s"push file",
      Array(
        kv("prefix", prefix)
      ): _*
    )
    val elements = store.all()
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
      playerstore: KeyValueIterator[String, PlayerStore]
  ): Path = {
    val tmp = Files.createTempFile(s"missing-data-${prefix}", ".json")
    val writer = new FileWriter(tmp.toFile())
    logger.debugv(
      s"start - write file",
      Array(
        kv("prefix", prefix),
        kv("tmp", tmp)
      ): _*
    )
    try {

      while (playerstore.hasNext()) {
        val keyAndValue = playerstore.next()

        val id = keyAndValue.value.player_id.get
        val playerstoreJson = printer.print(
          PlayerStoreSQS.playerStoreSQSEncoder.apply(
            PlayerStoreSQS(keyAndValue.value)
          )
        )
        val line =
          s"""{"id":"${id}","properties":${playerstoreJson}}\n"""
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

  def expose(prefix: String, timestamp: Long, file: Path): Unit = {
    val client = new BlobServiceClientBuilder()
      .connectionString(config.connectionString)
      .buildClient()
    val blobClient = client
      .getBlobContainerClient(config.outputContainerName)
      .getBlobClient(
        s"data/${prefix}/missingdata/playerstore-${UUID.randomUUID().toString()}.json"
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
    store = processorContext.getStateStore(storeName)
    processorContext.schedule(
      config.punctuator,
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }
}
