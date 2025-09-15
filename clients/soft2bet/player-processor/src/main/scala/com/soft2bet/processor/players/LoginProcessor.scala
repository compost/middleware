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
import com.soft2bet.model.Verification._
import com.jada.sqs.Body
import io.circe.Printer
import com.soft2bet.model.BonusUpdatedSQS
import com.soft2bet.model.Wagering
import com.soft2bet.model.WageringJSON
import com.soft2bet.model.keepYYYYMMDD
import com.soft2bet.model.formatYYYYMMDD
import com.soft2bet.model.WithdrawalRequestSQS
import java.util.UUID
import com.soft2bet.model.VerificationSQS

class LoginProcessor(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient
) extends Transformer[String, Login, KeyValue[String, PlayerStore]] {
  private var processorContext: ProcessorContext = _

  private val sender = new Sender(config, sqs, ueNorthSQS, false, false)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[PlayersTransformer])

  override def init(processorContext: ProcessorContext): Unit = {
    this.processorContext = processorContext
  }

  override def transform(
      k: String,
      login: Login
  ): KeyValue[String, PlayerStore] = {

    val topicName = processorContext.topic()
    val playerToSaveInStore = topicName match {
      case Common.loginRepartitionedTopic =>
        if (login.login_success.getOrElse("") == "1") {
          PlayerStore(
            player_id = login.player_id,
            brand_id = login.brand_id,
            GDPR = login.GDPR,
            GlobalProductClassification = login.GlobalProductClassification,
            ProductClassification = login.ProductClassification,
            FavoriteGameIds = login.FavoriteGameIds,
            FavoriteGameNames = login.FavoriteGameNames,
            IsVerified = login.IsVerified,
            block_reason = login.BlockReason,
            VIPLevel = login.VIPLevel,
            VIPMaxLevel = login.VIPMaxLevel,
            BlockedCasino = login.BlockedCasino,
            BlockedSport = login.BlockedSport,
            CanBeContactedBySMS = login.CanBeContactedBySMS,
            CanBeContactedByEmail = login.CanBeContactedByEmail,
            CanBeContactedByPhoneOrDynamicMessage =
              login.CanBeContactedByPhoneOrDynamicMessage,
            CanReceivePromotions = login.CanReceivePromotions,
            BonusProgram = login.BonusProgram,
            IsOptInCashback = login.IsOptInCashback,
            IsOptInWeeklyReload = login.IsOptInWeeklyReload,
            IsOptInWeekendReload = login.IsOptInWeekendReload,
            IsOptInForLastFourPeriodsCashback =
              login.IsOptInForLastFourPeriodsCashback,
            IsOptInForLastFourPeriodsWeeklyReload =
              login.IsOptInForLastFourPeriodsWeeklyReload,
            IsOptInForLastFourPeriodsWeekendReload =
              login.IsOptInForLastFourPeriodsWeekendReload
          )
        } else {
          null
        }
      case _ =>
        logger.debug(s"${processorContext.topic()} not handled for now")
        null
    }

    if (playerToSaveInStore != null) {

      sender.sendPlayer(
        Some(processorContext.topic()),
        None,
        playerToSaveInStore
      )
    }
    new KeyValue(k, playerToSaveInStore)

  }

  override def close(): Unit = {}
}
