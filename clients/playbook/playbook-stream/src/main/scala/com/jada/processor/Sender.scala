package com.jada.processor

import com.jada.configuration.ApplicationConfiguration
import org.jboss.logging.Logger
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv

class Sender(
    val config: ApplicationConfiguration,
    val sqsClient: software.amazon.awssdk.services.sqs.SqsClient,
    val sqsClientNorth: software.amazon.awssdk.services.sqs.SqsClient
) {
  private final val logger =
    Logger.getLogger(classOf[Sender])

  def getSqsClient(brandId: String) = {
    if (config.brandsSqsNorth.contains(brandId)) {
      sqsClientNorth
    } else {
      sqsClient
    }
  }
  def sendMessageToSQS(
      playerId: String,
      brandId: String,
      body: String,
      messageId: String = java.util.UUID.randomUUID().toString
  ): Unit = {
    val queue = config.brandQueue.get(brandId)
    if(queue == null){
      logger.warn(s"unknown brand id ${brandId}")
      return
    }
    logger.debugv(
      "send message to sqs",
      java.util.Map.of(
        "playerId",
        playerId,
        "brandId",
        brandId,
        "url",
        queue,
        "messageId",
        messageId,
        "body",
        body
      )
    )
    try {
      val client = getSqsClient(brandId)
      Option(client.sendMessage(request => {
        request
          .queueUrl(queue)
          .messageBody(body)
          .messageGroupId(config.sqsGroupId)
          .messageDeduplicationId(messageId)

      })) match {
        case Some(result) =>
          logger.infov(
            "sent message to sqs",
            Array(
              kv("playerId", playerId),
              kv("brandId", brandId),
              kv("url", queue),
              kv("messageId", messageId),
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
            kv("brandId", brandId),
            kv("url", queue),
            kv("messageId", messageId),
            kv("body", body),
            e
          ): _*
        )
        throw e
    }
  }
}
