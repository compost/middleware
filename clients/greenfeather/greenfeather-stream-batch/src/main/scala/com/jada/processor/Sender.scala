package com.jada.processor

import com.jada.configuration.ApplicationConfiguration
import com.jada.models.Body
import io.circe.{Encoder, Printer}
import org.jboss.logging.Logger
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import java.nio.file.Path
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.sas.BlobSasPermission
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues
import java.time.OffsetDateTime
import java.time.Instant
import java.util.UUID

class Sender(
    config: ApplicationConfiguration,
    client: software.amazon.awssdk.services.sqs.SqsClient
) {
  private final val logger =
    Logger.getLogger(classOf[Sender])

  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )

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
      mappingSelector,
      value
    )
    try {
      sendMessageToSQS(
        playerId,
        brandId,
        printer.print(Body.bodyEncoder[T].apply(body))
      )
    } catch {
      case e: Exception =>
        logger.errorv(
          "unable send body",
          Array(
            kv("playerId", playerId),
            kv("brandId", brandId),
            kv("body", body),
            kv("error", e)
          ): _*
        )
        throw e
    }
  }

  def sendMessageToSQS(
      playerId: String,
      brandId: String,
      body: String
  ): Unit = {
    val queue = config.brandQueue.get(brandId)
    logger.infov(
      "send message to sqs",
      Array(
        kv("playerId", playerId),
        kv("brandId", brandId),
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
              kv("brandId", brandId),
              kv("url", queue),
              kv("body", body)
            ): _*
          )
        case _ =>
      }
    } catch {
      case e: Exception =>
        logger.errorv(
          e,
          "unable to send message",
          Array(
            kv("playerId", playerId),
            kv("brandId", brandId),
            kv("url", queue),
            kv("body", body),
          ): _*
        )
        throw e
    }
  }

  def expose(
      brandId: String,
      file: Path,
  ): Unit = {
    val bclient = new BlobServiceClientBuilder()
      .connectionString(config.connectionString)
      .buildClient()
    val blobClient = bclient
      .getBlobContainerClient(config.outputContainerName)
      .getBlobClient(
        s"data/greenfeather/batch-players/${brandId}batch-players-${UUID.randomUUID().toString()}.json"
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
         |  "mappingSelector": "${computeMappingSelector(Some(brandId), "batch_update")}",
         |  "downloadUri": "$url"
         |}
         |""".stripMargin

    sendMessageToSQS(
      s"${Instant.now().toEpochMilli}", // no player_id
      brandId,
      body
    )
  }

  def computeMappingSelector(
      brandId: Option[String],
      mp: String
  ): String = {
    if (brandId.exists(v => config.mappingSelectorDevPrefix.contains(v))) {
      s"DEV_$mp"
    } else {
      mp
    }
  }
}
