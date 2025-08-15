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
  Verification,
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
import com.soft2bet.model.keepYYYYMMDD
import com.soft2bet.model.formatYYYYMMDD
import com.soft2bet.model.WithdrawalRequestSQS

class WageringTransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    sentStoreName: String
) extends Transformer[String, Wagering, KeyValue[String, PlayerStore]] {
  private var processorContext: ProcessorContext = _
  private var playerKVStore: KeyValueStore[String, PlayerStore] = _
  private var sent: KeyValueStore[String, Boolean] = _
  private var startStopKVStore: KeyValueStore[String, String] = _

  private val sender = new Sender(config, sqs, ueNorthSQS, false, false)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[PlayersTransformer])

  override def init(processorContext: ProcessorContext): Unit = {
    this.processorContext = processorContext
    this.sent = processorContext
      .getStateStore(sentStoreName)
      .asInstanceOf[KeyValueStore[String, Boolean]]
  }

  override def transform(
      k: String,
      input: Wagering
  ): KeyValue[String, PlayerStore] = {
    val exist= Option(sent.get(k))
    if (exist.isDefined) {
      return null
    }

    val playerToSaveInStore = processorContext.topic() match {
      case Common.wageringRepartitionedTopic =>
        if (
          input.BalanceEUR.map(_.toDouble).filter(v => v < 5).isDefined
          && input.ts.isDefined && input.LifetimeDepositCount
            .map(_.toInt)
            .filter(v => v == 1)
            .isDefined
        ) {
          val firstDepositLossMessage =
            s"""
                 |{"type": "GENERIC_USER",
                 |"mappingSelector": "first_deposit_loss",
                 |"contactId": "$k",
                 |"properties": {
                 | "first_deposit_loss_date": "${input.ts
                .map(v =>
                  if (v.length() > 10) { v.substring(0, 10) }
                  else { v }
                )
                .getOrElse("")}"
                 |}
                 |}
                 |""".stripMargin
          // send blocked message
          sender.sendToSQS(
            firstDepositLossMessage,
            k,
            input.brand_id.get
          )
          sent.put(k, true)
        }
        null
      case _ =>
        logger.debug(s"${processorContext.topic()} not handled for now")
        null
    }

    null
  }

  override def close(): Unit = {}

}
