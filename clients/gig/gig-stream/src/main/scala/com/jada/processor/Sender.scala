package com.jada.processor

import com.jada.configuration.ApplicationConfiguration
import org.jboss.logging.Logger
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import java.util.UUID

class Sender {
  private final val logger =
    Logger.getLogger(classOf[Sender])

  def sendMessageToSQS(
      config: ApplicationConfiguration,
      client: software.amazon.awssdk.services.sqs.SqsClient,
      playerId: String,
      brandId: String,
      body: String,
      messageDeduplicationId: String = UUID.randomUUID().toString()
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
          .messageDeduplicationId(messageDeduplicationId) 
      })) match {
        case Some(result) =>
          logger.debugv(
            "sent message to sqs",
            Array(
              kv("playerId", playerId),
              kv("url", queue),
              kv("messageId", result.messageId()),
              kv("md5OfMessageBody", result.md5OfMessageBody()),
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
            kv("body", body),
            e
          ): _*
        )
        throw e
    }
  }
}
