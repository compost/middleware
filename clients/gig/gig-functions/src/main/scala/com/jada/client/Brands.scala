package com.jada.client

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.implementation.util.ModelHelper
import com.azure.storage.blob.sas.{
  BlobSasPermission,
  BlobServiceSasSignatureValues
}
import com.microsoft.azure.functions._
import com.microsoft.azure.functions.annotation.{FunctionName, TimerTrigger}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.time.{Instant, OffsetDateTime}
import java.util.logging.Logger
import scala.util.Try
import com.amazonaws.regions.Region
import com.microsoft.azure.functions.annotation.HttpTrigger
import com.microsoft.azure.functions.annotation.AuthorizationLevel
import java.util.Optional

class Brands(
    val ids: Set[Int],
    val folder: String,
    val queue: String,
    val mappingSelector: String = "batch-import",
    region: Regions
) {

  val filter = s" IN (${ids.mkString(",")})"

  lazy val sqsClient = AmazonSQSClientBuilder
    .standard()
    .withRegion(region)
    .withCredentials(
      new EnvironmentVariableCredentialsProvider()
    )
    .build()

  def sendNotification(send: Boolean, downloadUrl: String)(implicit
      logger: Logger
  ): Unit = {
    val body =
      s"""
         |{
         |  "type": "BATCH_NOTIFICATION",
         |  "mappingSelector": "${mappingSelector}",
         |  "downloadUri": "$downloadUrl"
         |}
         |""".stripMargin

    val request = new SendMessageRequest()
      .withQueueUrl(queue)
      .withMessageBody(body)
      .withMessageGroupId("gig")
      .withMessageDeduplicationId(s"${Instant.now().toEpochMilli}")

    try {
      if (send) {
        Option(sqsClient.sendMessage(request)) foreach { result =>
          logger.info(s"Sent queue message: $body => result: $result")
        }
      } else {
        logger.info(s"dry run $body in $downloadUrl")

      }
    } catch {
      case e: Exception =>
        logger.info(s"wasn't able to send notification. Reason $e")
    }
  }

}
