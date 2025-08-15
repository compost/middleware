package com.jada.processor

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonFactoryBuilder
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.jada.Common
import com.jada.configuration.ApplicationConfiguration
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{
  ProcessorContext,
  PunctuationType,
  Punctuator,
  StateStore
}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.w3c.dom.Node

import java.time.{Duration, Instant, LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import org.jboss.logging.Logger
import com.fasterxml.jackson.databind.json.JsonMapper
import com.jada.models.{
  Action,
  Body,
  Login,
  LoginSQS,
  Player,
  PlayerSQS,
  PlayerStore,
  UserConsentUpdate,
  Wagering,
  Wallet
}
import io.circe.Printer

import java.util.UUID
import io.circe.Encoder
import io.circe.Decoder
import com.jada.serdes.CirceSerdes
import io.circe._
import io.circe.generic.auto._

import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.kafka.streams.state.KeyValueIterator
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Path
import com.azure.storage.blob.BlobServiceClientBuilder
import java.time.OffsetDateTime
import com.azure.storage.blob.sas.BlobSasPermission
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues

object MappingTransformer {

  val DateTimeformatter3Digits = DateTimeFormatter
    .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
    .withZone(ZoneId.systemDefault())

  val DateTimeformatter2Digits = DateTimeFormatter
    .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SS")
    .withZone(ZoneId.systemDefault())

  val DateTimeformatter1Digit = DateTimeFormatter
    .ofPattern("yyyy-MM-dd'T'HH:mm:ss.S")
    .withZone(ZoneId.systemDefault())

  val DateTimeformatter3DigitsWithoutT = DateTimeFormatter
    .ofPattern("yyyy-MM-dd' 'HH:mm:ss.SSS")
    .withZone(ZoneId.systemDefault())

  val DateTimeformatter2DigitsWithoutT = DateTimeFormatter
    .ofPattern("yyyy-MM-dd' 'HH:mm:ss.SS")
    .withZone(ZoneId.systemDefault())

  val DateTimeformatter1DigitWithoutT = DateTimeFormatter
    .ofPattern("yyyy-MM-dd' 'HH:mm:ss.S")
    .withZone(ZoneId.systemDefault())

  def parseDate(date: String): Instant = {
    if (date.length == 23) {
      parseDateWithFallback(
        date,
        DateTimeformatter3Digits,
        DateTimeformatter3DigitsWithoutT
      )
    } else if (date.length == 22) {
      parseDateWithFallback(
        date,
        DateTimeformatter2Digits,
        DateTimeformatter2DigitsWithoutT
      )
    } else if (date.length == 21) {
      parseDateWithFallback(
        date,
        DateTimeformatter1Digit,
        DateTimeformatter1DigitWithoutT
      )
    } else {
      Instant.from(
        DateTimeformatter3Digits.parse(s"${date.substring(0, 10)}T00:00:00.000")
      )
    }
  }

  def parseDateWithFallback(
      date: String,
      formatOne: DateTimeFormatter,
      formatTwo: DateTimeFormatter
  ): Instant = {
    Try(Instant.from(formatOne.parse(date)))
      .orElse(Try(Instant.from(formatTwo.parse(date))))
      .getOrElse(
        throw new RuntimeException(s"unable to parse the date ${date}")
      )
  }

  def toBoolean(v: String): Boolean = {
    v match {
      case "true"  => true
      case "false" => false
      case "0"     => false
      case "1"     => true
      case _       => false
    }
  }

}
class MappingTransformer(
    config: ApplicationConfiguration,
    referentials: com.jada.Referentials,
    client: software.amazon.awssdk.services.sqs.SqsClient,
    startStopStoreName: String,
    playerStoreName: String
) extends Transformer[String, Array[Byte], KeyValue[Unit, Unit]]
    with Punctuator {

  import MappingTransformer._
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  val bootstrap = new AtomicBoolean(true)

  val GenericUser = "GENERIC_USER"
  val UserBlockedToggle = "USER_BLOCKED_TOGGLE"
  val WageringWin = "win"
  val WalletWithdraw = "withdraw"
  val WalletDeposit = "deposit"
  val FailedWallets = Set("rejected", "error", "cancelled", "voided")
  private final val logger =
    Logger.getLogger(classOf[MappingTransformer])
  private var processorContext: ProcessorContext = _
  private var startStopStore: KeyValueStore[String, Action] = _
  private var playerStore: KeyValueStore[String, PlayerStore] = _

  override def init(processorContext: ProcessorContext): Unit = {
    logger.debug(f"${config}")
    this.processorContext = processorContext
    this.startStopStore = processorContext.getStateStore(startStopStoreName)
    this.playerStore = processorContext.getStateStore(playerStoreName)
    this.processorContext.schedule(
      config.punctuator,
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }

  def userConsented(
      userConsentUpdate: UserConsentUpdate,
      kind: String
  ): Option[String] = {
    userConsentUpdate.channel
      .filter(_.toUpperCase.equals(kind))
      .map(_ => userConsentUpdate.consented)
      .flatten
  }

  override def transform(key: String, v: Array[Byte]): KeyValue[Unit, Unit] = {
    val topic = processorContext.topic()
    logger.debugv(
      "transform",
      Array(kv("topic", topic), kv("key", key), kv("value", new String(v))): _*
    )
    val playerFromStore = playerStore.get(key)

    val action: Option[Action] = Option(
      startStopStore.get(processorContext.applicationId())
    )

    val playerToSaveInStore = topic match {

      case config.topicCoordinator =>
        startStopStore.put(
          processorContext.applicationId(),
          deserialize[Action](v)
        )
        null

      case _ if action.exists(f => f.action == Common.stop) =>
        logger.debug("nothing to do")
        null

      case config.topicUserConsentUpdateFinnplayRepartitioned =>
        val userConsentUpdate = deserialize[UserConsentUpdate](v)

        if (
          userConsentUpdate.player_id.isDefined &&
          userConsentUpdate.brand_id.isDefined &&
          userConsentUpdate.channel.isDefined && userConsentUpdate.consented.isDefined &&
          userConsentUpdate.`type`.isDefined && userConsentUpdate.`type`.get.toUpperCase == "USER_CONSENT_EVENT"
        ) {
          logger.debug(s"user_consent_update ${userConsentUpdate}")

          val newPlayer = Option(playerFromStore) match {
            case Some(player) =>
              player.copy(
                email_consented = userConsented(userConsentUpdate, "EMAIL")
                  .orElse(player.email_consented),
                sms_consented = userConsented(userConsentUpdate, "SMS")
                  .orElse(player.sms_consented)
              )
            case None =>
              PlayerStore(
                player_id = userConsentUpdate.player_id,
                brand_id = userConsentUpdate.brand_id,
                email_consented = userConsented(userConsentUpdate, "EMAIL"),
                sms_consented = userConsented(userConsentUpdate, "SMS")
              )
          }

          if (
            playerFromStore == null ||
            playerFromStore.email_consented != newPlayer.email_consented ||
            playerFromStore.sms_consented != newPlayer.sms_consented
          ) {
            val consented =
              if (userConsentUpdate.consented.get == "0") "false" else "true"
            (
              userConsentUpdate.brand_id,
              userConsentUpdate.channel.map(_.toUpperCase())
            ) match {
              case (Some("106"), Some("EMAIL")) =>
                logger.debug("lvc both")
                sendConsent(
                  userConsentUpdate.brand_id,
                  userConsentUpdate.player_id,
                  Some("EMAIL"),
                  consented
                )
                sendConsent(
                  userConsentUpdate.brand_id,
                  userConsentUpdate.player_id,
                  Some("SMS"),
                  consented
                )
              case (Some("106"), _) =>
                logger.debug("lvc nothing")
              case _ =>
                logger.debug("other one")
                sendConsent(
                  userConsentUpdate.brand_id,
                  userConsentUpdate.player_id,
                  userConsentUpdate.channel,
                  consented
                )
            }
          }
          newPlayer
        } else {
          null
        }

      case config.topicUserConsentHistoryFinnplayRepartitioned =>
        val userConsentUpdate = deserialize[UserConsentUpdate](v)
        if (playerFromStore != null) {
          if (
            userConsentUpdate.channel.contains(
              "sms"
            ) && playerFromStore.receive_sms != userConsentUpdate.consented
          ) {
            logger.debugv(
              "difference sms",
              Array(
                kv("playerId", playerFromStore.player_id.getOrElse("none")),
                kv("brandId", playerFromStore.brand_id.getOrElse("none")),
                kv("receive_sms", playerFromStore.receive_sms),
                kv("consented", userConsentUpdate.consented)
              ): _*
            )
          }
          if (
            userConsentUpdate.channel.contains(
              "email"
            ) && playerFromStore.receive_email != userConsentUpdate.consented
          ) {
            logger.debugv(
              "difference email",
              Array(
                kv("playerId", playerFromStore.player_id.getOrElse("none")),
                kv("brandId", playerFromStore.brand_id.getOrElse("none")),
                kv("receive_email", playerFromStore.receive_email),
                kv("consented", userConsentUpdate.consented)
              ): _*
            )

          }
        }

        if (
          userConsentUpdate.player_id.isDefined &&
          userConsentUpdate.brand_id.isDefined &&
          userConsentUpdate.channel.isDefined &&
          userConsentUpdate.`type`.isDefined && userConsentUpdate.`type`.get.toUpperCase == "USER_CONSENT_EVENT"
        ) {
          logger.debug(s"user_consent_update ${userConsentUpdate}")

          val newPlayer = Option(playerFromStore) match {
            case Some(player) =>
              player.copy(
                receive_email = userConsented(userConsentUpdate, "EMAIL")
                  .orElse(player.email_consented),
                receive_sms = userConsented(userConsentUpdate, "SMS")
                  .orElse(player.sms_consented),
                email_consented = userConsented(userConsentUpdate, "EMAIL")
                  .orElse(player.email_consented),
                sms_consented = userConsented(userConsentUpdate, "SMS")
                  .orElse(player.sms_consented),
                history_email_consented =
                  userConsented(userConsentUpdate, "EMAIL")
                    .orElse(player.history_email_consented),
                history_sms_consented = userConsented(userConsentUpdate, "SMS")
                  .orElse(player.history_sms_consented)
              )
            case None =>
              PlayerStore(
                player_id = userConsentUpdate.player_id,
                brand_id = userConsentUpdate.brand_id,
                receive_email = userConsented(userConsentUpdate, "EMAIL"),
                receive_sms = userConsented(userConsentUpdate, "SMS"),
                email_consented = userConsented(userConsentUpdate, "EMAIL"),
                sms_consented = userConsented(userConsentUpdate, "SMS"),
                history_email_consented =
                  userConsented(userConsentUpdate, "EMAIL"),
                history_sms_consented = userConsented(userConsentUpdate, "SMS")
              )
          }

          if (
            playerFromStore == null ||
            playerFromStore.email_consented != newPlayer.email_consented ||
            playerFromStore.sms_consented != newPlayer.sms_consented
          ) {
            val consented =
              if (userConsentUpdate.consented.contains("0")) "false" else "true"
            (
              userConsentUpdate.brand_id,
              userConsentUpdate.channel.map(_.toUpperCase())
            ) match {
              case (Some("106"), Some("EMAIL")) =>
                logger.debug("lvc both")
                sendConsent(
                  userConsentUpdate.brand_id,
                  userConsentUpdate.player_id,
                  Some("EMAIL"),
                  consented
                )
                sendConsent(
                  userConsentUpdate.brand_id,
                  userConsentUpdate.player_id,
                  Some("SMS"),
                  consented
                )
              case (Some("106"), _) =>
                logger.debug("lvc nothing")
              case _ =>
                logger.debug("other one")
                sendConsent(
                  userConsentUpdate.brand_id,
                  userConsentUpdate.player_id,
                  userConsentUpdate.channel,
                  consented
                )
            }
          }
          newPlayer
        } else {
          null
        }

      case config.topicPlayersRepartitioned =>
        val inputPlayer = deserialize[Player](v)

        val newPlayer = Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              nick_name = inputPlayer.nick_name.orElse(player.nick_name),
              gender_id = inputPlayer.gender_id.orElse(player.gender_id),
              gender_description = inputPlayer.gender_description.orElse(
                player.gender_description
              ),
              address = inputPlayer.address.orElse(player.address),
              zip_code = inputPlayer.zip_code.orElse(player.zip_code),
              city = inputPlayer.city.orElse(player.city),
              device_type_id =
                inputPlayer.device_type_id.orElse(player.device_type_id),
              device_type_description = inputPlayer.device_type_description
                .orElse(player.device_type_description),
              player_status_id =
                inputPlayer.player_status_id.orElse(player.player_status_id),
              player_status_description = inputPlayer.player_status_description
                .orElse(player.player_status_description),
              SITE_ID = inputPlayer.SITE_ID
                .orElse(player.SITE_ID),
              user_validated =
                inputPlayer.user_validated.orElse(player.user_validated),
              user_validated_time = inputPlayer.user_validated_time.orElse(
                player.user_validated_time
              ),
              brand_id = inputPlayer.brand_id.orElse(player.brand_id),
              reg_datetime =
                inputPlayer.reg_datetime.orElse(player.reg_datetime),
              first_name = inputPlayer.first_name.orElse(player.first_name),
              last_name = inputPlayer.last_name.orElse(player.last_name),
              test_user = inputPlayer.test_user.orElse(player.test_user),
              email = inputPlayer.email.orElse(player.email),
              phone_number =
                inputPlayer.phone_number.orElse(player.phone_number),
              language = inputPlayer.language.orElse(player.language),
              affiliate_id =
                inputPlayer.affiliate_id.orElse(player.affiliate_id),
              is_self_excluded =
                inputPlayer.is_self_excluded.orElse(player.is_self_excluded),
              first_dep_datetime = inputPlayer.first_dep_datetime.orElse(
                player.first_dep_datetime
              ),
              dob = inputPlayer.dob.orElse(player.dob),
              vip = inputPlayer.vip.orElse(player.vip),
              currency_id = inputPlayer.currency_id.orElse(player.currency_id),
              country_id = inputPlayer.country_id.orElse(player.country_id),
              receive_email =
                inputPlayer.receive_email.orElse(player.receive_email),
              receive_sms = inputPlayer.receive_sms.orElse(player.receive_sms),
              Benefit = inputPlayer.Benefit.orElse(player.Benefit)
            )

          case None =>
            // partially register non null infos
            PlayerStore(
              Benefit = inputPlayer.Benefit,
              player_id = inputPlayer.player_id,
              nick_name = inputPlayer.nick_name,
              gender_id = inputPlayer.gender_id,
              gender_description = inputPlayer.gender_description,
              address = inputPlayer.address,
              zip_code = inputPlayer.zip_code,
              city = inputPlayer.city,
              device_type_id = inputPlayer.device_type_id,
              device_type_description = inputPlayer.device_type_description,
              player_status_id = inputPlayer.player_status_id,
              player_status_description = inputPlayer.player_status_description,
              SITE_ID = inputPlayer.SITE_ID,
              user_validated = inputPlayer.user_validated,
              user_validated_time = inputPlayer.user_validated_time,
              test_user = inputPlayer.test_user,
              brand_id = inputPlayer.brand_id,
              reg_datetime = inputPlayer.reg_datetime,
              first_name = inputPlayer.first_name,
              last_name = inputPlayer.last_name,
              receive_email = inputPlayer.receive_email,
              email = inputPlayer.email,
              phone_number = inputPlayer.phone_number,
              language = inputPlayer.language,
              affiliate_id = inputPlayer.affiliate_id,
              is_self_excluded = inputPlayer.is_self_excluded,
              first_dep_datetime = inputPlayer.first_dep_datetime,
              dob = inputPlayer.dob,
              vip = inputPlayer.vip,
              currency_id = inputPlayer.currency_id,
              country_id = inputPlayer.country_id,
              receive_sms = inputPlayer.receive_sms
            )
        }

        if (
          (playerFromStore == null || playerFromStore.user_validated != newPlayer.user_validated) && newPlayer.user_validated == Some(
            "1"
          )
        ) {
          val playerSQS = PlayerSQS(newPlayer, referentials)
          send(
            newPlayer.player_id.get,
            newPlayer.brand_id.get,
            GenericUser,
            "player_validated",
            playerSQS
          )

        }
        if (
          (playerFromStore == null || (playerFromStore != null && playerFromStore.reg_datetime.isEmpty)) && inputPlayer.reg_datetime.isDefined &&
          parseDate(inputPlayer.reg_datetime.get)
            .isAfter(Instant.now().minusSeconds(86400))
        ) {

          val playerSQS = PlayerSQS(newPlayer, referentials)
          send(
            newPlayer.player_id.get,
            newPlayer.brand_id.get,
            GenericUser,
            "player_registration",
            playerSQS
          )
        }

        if (
          (playerFromStore == null || (playerFromStore != null && playerFromStore.first_dep_datetime.isEmpty)) && inputPlayer.first_dep_datetime.isDefined &&
          parseDate(inputPlayer.first_dep_datetime.get)
            .isAfter(Instant.now().minusSeconds(86400))
        ) {
          val firstDepositMessage =
            s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector": "first_deposit",
               |"contactId": "${inputPlayer.player_id.get}",
               |"properties": {
               | "first_deposit_datetime": "${inputPlayer.first_dep_datetime
                .map(PlayerSQS.keepDate(inputPlayer.brand_id, _))
                .get}"
               |}
               |}
               |""".stripMargin
          sendMessageToSQS(
            inputPlayer.player_id.get,
            inputPlayer.brand_id.get,
            firstDepositMessage
          )
        }

        newPlayer

      case config.topicLoginsRepartitioned =>
        val login = deserialize[Login](v)

        val newPlayerWithLogin = Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              player_id = login.player_id.orElse(player.player_id),
              brand_id = login.brand_id.orElse(player.brand_id),
              last_login_datetime =
                login.login_datetime.orElse(player.last_login_datetime),
              last_login_success =
                login.login_success.orElse(player.last_login_success)
            )

          case None =>
            PlayerStore(
              player_id = login.player_id,
              last_login_datetime = login.login_datetime,
              last_login_success = login.login_success,
              brand_id = login.brand_id
            )
        }
        val loginSQS = LoginSQS(login)
        send(
          login.player_id.get,
          login.brand_id.get,
          GenericUser,
          "player_login",
          loginSQS
        )
        newPlayerWithLogin

      case config.topicWalletsRepartitioned =>
        val wallet = deserialize[Wallet](v)
        val newActivityDate = wallet.transaction_datetime.map(x => parseDate(x))

        val newPlayerWithWallet = Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              player_id = wallet.player_id.orElse(player.player_id),
              wallet_transaction_type_id = wallet.transaction_type_id.orElse(
                player.wallet_transaction_type_id
              ),
              wallet_transaction_status_id = wallet.transaction_status_id
                .orElse(player.wallet_transaction_status_id),
              last_activity_date =
                newActivityDate.orElse(player.last_activity_date),
              brand_id = wallet.brand_id.orElse(player.brand_id)
            )

          case None =>
            PlayerStore(
              player_id = wallet.player_id,
              brand_id = wallet.brand_id,
              wallet_transaction_type_id = wallet.transaction_type_id,
              wallet_transaction_status_id = wallet.transaction_status_id,
              last_activity_date =
                Some(newActivityDate.getOrElse(Instant.now()))
            )
        }

        if (
          newPlayerWithWallet.wallet_transaction_type_id
            .getOrElse(
              ""
            )
            .toLowerCase == WalletDeposit &&
          FailedWallets.contains(
            newPlayerWithWallet.wallet_transaction_status_id
              .getOrElse("")
              .toLowerCase()
          )

          && wallet.transaction_datetime.isDefined
        ) {
          // failed_deposit
          val failedDepositMessage =
            s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector": "failed_deposit",
               |"contactId": "${wallet.player_id.get}",
               |"properties": {
               | "failed_deposit_date": "${wallet.transaction_datetime
                .map(PlayerSQS.keepDate(wallet.brand_id, _))
                .get}"
               |}
               |}
               |""".stripMargin
          sendMessageToSQS(
            wallet.player_id.get,
            wallet.brand_id.get,
            failedDepositMessage
          )
        }

        if (
          newPlayerWithWallet.wallet_transaction_type_id
            .getOrElse(
              ""
            )
            .toLowerCase == WalletWithdraw &&
          FailedWallets.contains(
            newPlayerWithWallet.wallet_transaction_status_id
              .getOrElse("")
              .toLowerCase()
          )
          && wallet.transaction_datetime.isDefined
        ) {
          // failed_withdrawal
          val failedWithdrawalMessage =
            s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector": "failed_withdrawal",
               |"contactId": "${wallet.player_id.get}",
               |"properties": {
               | "failed_withdrawal_date": "${wallet.transaction_datetime
                .map(PlayerSQS.keepDate(wallet.brand_id, _))
                .get}"
               |}
               |}
               |""".stripMargin
          sendMessageToSQS(
            wallet.player_id.get,
            wallet.brand_id.get,
            failedWithdrawalMessage
          )
        }

        newPlayerWithWallet

      case config.topicWageringsRepartitioned =>
        val wagering = deserialize[Wagering](v)
        val newActivityDate =
          wagering.bet_datetime.map(x => parseDate(x))

        val newPlayerWithWagering = Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              player_id = wagering.player_id.orElse(player.player_id),
              wagering_transaction_type_id = wagering.transaction_type_id
                .orElse(player.wagering_transaction_type_id),
              wagering_ggr_amount =
                wagering.ggr_amount.orElse(player.wagering_ggr_amount),
              wagering_TOTAL_WIN_AMOUNT = wagering.TOTAL_WIN_AMOUNT.orElse(
                player.wagering_TOTAL_WIN_AMOUNT
              ),
              wagering_has_resulted =
                wagering.has_resulted.orElse(player.wagering_has_resulted),
              last_activity_date =
                newActivityDate.orElse(player.last_activity_date),
              brand_id = wagering.brand_id.orElse(player.brand_id)
            )

          case None =>
            PlayerStore(
              player_id = wagering.player_id,
              wagering_transaction_type_id = wagering.transaction_type_id,
              wagering_ggr_amount = wagering.ggr_amount,
              wagering_TOTAL_WIN_AMOUNT = wagering.TOTAL_WIN_AMOUNT,
              wagering_has_resulted = wagering.has_resulted,
              last_activity_date =
                Some(newActivityDate.getOrElse(Instant.now())),
              brand_id = wagering.brand_id
            )
        }

        if (
          wagering.transaction_type_id
            .getOrElse("")
            .toLowerCase() == WageringWin &&
          wagering.has_resulted
            .map(_.toLowerCase())
            .map { v => toBoolean(v) }
            .getOrElse(false) &&
          wagering.TOTAL_WIN_AMOUNT.getOrElse("0").toFloat >= 1000
        ) {
          // big_win
          val bigWinMessage =
            s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector":  "big_win",
               |"contactId": "${wagering.player_id.get}",
               |"properties": {
               | "ggr_amount": "${wagering.TOTAL_WIN_AMOUNT.get}"
               |}
               |}
               |""".stripMargin
          sendMessageToSQS(
            wagering.player_id.get,
            wagering.brand_id.get,
            bigWinMessage
          )
        }
        newPlayerWithWagering

    }

    if (playerToSaveInStore != null) {
      playerStore.put(key, playerToSaveInStore)

      val playerSQS = PlayerSQS(playerToSaveInStore, referentials)

      if (
        playerFromStore == null || playerSQS != PlayerSQS(
          playerFromStore,
          referentials
        )
      ) {
        send(
          playerToSaveInStore.player_id.get,
          playerToSaveInStore.brand_id.get,
          GenericUser,
          "player_update",
          playerSQS
        )

      }

    }

    if (
      playerToSaveInStore != null && (playerFromStore == null || playerToSaveInStore.is_self_excluded != playerFromStore.is_self_excluded)
    ) {
      val isBlocked =
        playerToSaveInStore.is_self_excluded
          .map(v => toBoolean(v))
          .getOrElse(false)
      sendUserBlockedToggle(
        playerToSaveInStore.player_id.get,
        playerToSaveInStore.brand_id.get,
        isBlocked
      )
    }

    null
  }

  def sendConsent(
      brandId: Option[String],
      playerId: Option[String],
      channel: Option[String],
      consented: String
  ): Unit = {
    val message = s"""
               |{
               |"type": "USER_CONSENT_UPDATE",
               |"mappingSelector": "consent_change",
               |"contactId": "${playerId.get}",
               |"channel": "${channel.get.toUpperCase}",
               |"consented": ${consented}
               |}
               |""".stripMargin
    sendMessageToSQS(
      playerId.get,
      brandId.get,
      message
    )
  }
  def sendUserBlockedToggle(
      playerId: String,
      brandId: String,
      isBlocked: Boolean
  ): Unit = {
    sendMessageToSQS(
      playerId,
      brandId,
      s"""{"type":"${UserBlockedToggle}","mappingSelector":"self_exclusion","contactId":"${playerId}","blocked":$isBlocked}"""
    )
  }

  def deserialize[T: Decoder](data: Array[Byte]): T = {
    parser.decode[T](new String(data)).right.get
  }

  def send[T: Encoder](
      playerId: String,
      brandId: String,
      `type`: String,
      mappingSelector: String,
      value: T,
      traceId: UUID = UUID.randomUUID()
  ): Unit = {
    val body = Body[T](
      `type`,
      playerId,
      mappingSelector,
      value
    )
    sendMessageToSQS(
      playerId,
      brandId,
      printer.print(Body.bodyEncoder[T].apply(body)),
      traceId
    )
  }

  def sendMessageToSQS(
      playerId: String,
      brandId: String,
      body: String,
      traceId: UUID = UUID.randomUUID()
  ): Unit = {
    val queue = config.brandQueue.get(brandId)
    logger.debugv(
      "send message to sqs",
      Array(
        kv("playerId", playerId),
        kv("url", queue),
        kv("traceId", traceId),
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
              kv("url", queue),
              kv("traceId", traceId),
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
            kv("url", queue),
            kv("traceId", traceId),
            kv("body", body)
          ): _*
        )
        throw e
    }
  }

  override def punctuate(timestamp: Long): Unit = {
    logger.debug("punctuate")

    // churn
    val players = playerStore.all()
    try {
      while (players.hasNext) {
        val keyValue = players.next()
        val p = keyValue.value
        if (
          p.player_id.isDefined && p.last_activity_date.isDefined &&
          ChronoUnit.DAYS.between(
            LocalDate.ofInstant(p.last_activity_date.get, ZoneId.of("UTC")),
            LocalDate.now()
          ) > 30 &&
          (p.sent_customer_churn.isEmpty || !p.sent_customer_churn.get)
        ) {

          val kafkaF = PlayerSQS.kafkaFormat.format(p.last_activity_date.get)
          val bigWinMessage =
            s"""
             |{"type": "GENERIC_USER",
             |"mappingSelector": "customer_churn",
             |"contactId": "${p.player_id.get}",
             |"properties": {
             | "last_activity_date": "${PlayerSQS.keepDate(p.brand_id, kafkaF)}"
             |}
             |}
             |""".stripMargin
          sendMessageToSQS(
            p.player_id.get,
            p.brand_id.get,
            bigWinMessage
          )

          // mark customer churn as already sent
          val newPlayerStore = p.copy(sent_customer_churn = Some(true))
          playerStore.put(keyValue.key, newPlayerStore)
        }
      }

    } finally {
      players.close()
    }

    if (config.bootstrapPlayers && bootstrap.compareAndSet(true, false)) {
      {
        val now = UUID.randomUUID().toString()
        logger.info(s"start-batch $now")
        val players = playerStore.all()
        import io.circe._, io.circe.generic.auto._, io.circe.parser._,
        io.circe.syntax._
        val brandFile =
          scala.collection.mutable.Map[String, (Path, FileWriter)]()

        try {
          while (players.hasNext) {
            val keyValue = players.next()
            val p = keyValue.value
            val newPlayerStore = p.gender_id match {
              case Some("0") =>
                Some(p.copy(gender_description = Some("Male")))
              case Some("1") =>
                Some(p.copy(gender_description = Some("Female")))
              case _ => None
            }
            if (newPlayerStore.isDefined) {
              playerStore.put(keyValue.key, newPlayerStore.get)
              if (newPlayerStore.get.brand_id.isDefined) {
                val filewrite = brandFile.getOrElseUpdate(
                  newPlayerStore.get.brand_id.get, {
                    val tmp = Files.createTempFile(
                      s"players-${now}-${keyValue.value.brand_id}",
                      ".json"
                    )
                    logger.debug(s"file: ${tmp}")
                    val writer = new FileWriter(tmp.toFile())
                    (tmp, writer)
                  }
                )
                filewrite._2.write(
                  s"""{"id":"${newPlayerStore.get.player_id.get}", "properties":{"gender_description":"${newPlayerStore.get.gender_description.get}"}}\n"""
                )
              }
            }
          }
          brandFile.foreach { case (key, value) => value._2.close() }
          brandFile.foreach {
            case (key, value) => {
              expose(key, now, value._1)
            }
          }
          logger.info(s"end-batch $now")
        } finally {
          brandFile.foreach { case (key, value) => value._2.close() }
          players.close()
        }

      }
    }
  }

  def sendAttributeAtNull(
      brandId: Option[String],
      playerId: Option[String],
      attribute: String
  ): Unit = {
    val msg =
      s"""
             |{"type": "GENERIC_USER",
             |"mappingSelector": "player_update",
             |"contactId": "${playerId.get}",
             |"properties": {"${attribute}":null}
             |}
             |""".stripMargin
    sendMessageToSQS(
      playerId.get,
      brandId.get,
      msg
    )
  }
  def sendPlayers(
      now: String,
      players: KeyValueIterator[String, PlayerStore]
  ): Unit = {

    val batchName = "2023-10-15"
    import io.circe._, io.circe.generic.auto._, io.circe.parser._,
    io.circe.syntax._

    val traceId = UUID.randomUUID()

    logger.debugv(
      "sendPlayers",
      Array(kv("traceId", traceId), kv("batchName", batchName)): _*
    )
    while (players.hasNext()) {
      val keyAndValue = players.next()
      val player = keyAndValue.value
      if (player.batchName.isEmpty || player.batchName.get != batchName) {
        val playerSQS = PlayerSQS(player, referentials)
        send(
          player.player_id.get,
          player.brand_id.get,
          GenericUser,
          "player_update",
          playerSQS
        )

      }

      playerStore.put(
        keyAndValue.key,
        keyAndValue.value.copy(batchName = Some(batchName))
      )
    }
  }

  def writeFile(
      now: String,
      players: KeyValueIterator[String, PlayerStore]
  ): Map[String, Path] = {

    import io.circe._, io.circe.generic.auto._, io.circe.parser._,
    io.circe.syntax._
    val brandFile = scala.collection.mutable.Map[String, (Path, FileWriter)]()

    try {
      while (players.hasNext()) {
        val keyAndValue = players.next()
        if (keyAndValue.value.brand_id.isDefined) {
          val filewrite = brandFile.getOrElseUpdate(
            keyAndValue.value.brand_id.get, {
              val tmp = Files.createTempFile(
                s"players-${now}-${keyAndValue.value.brand_id}",
                ".json"
              )
              logger.debug(s"file: ${tmp}")
              val writer = new FileWriter(tmp.toFile())
              (tmp, writer)
            }
          )

          val newPlayer = keyAndValue.value
          val p = PlayerSQS(newPlayer, referentials)
          filewrite._2.write(
            s"""{"id":"${newPlayer.player_id.get}", "properties":${printer
                .print(p.asJson)}}\n"""
          )
        }
      }
    } finally {
      brandFile.foreach { case (key, value) => value._2.close() }
    }
    brandFile.map { case (key, value) => (key, value._1) }.toMap
  }

  def expose(brandId: String, timestamp: String, file: Path): Unit = {
    val client = new BlobServiceClientBuilder()
      .connectionString(config.connectionString)
      .buildClient()
    val blobClient = client
      .getBlobContainerClient(config.outputContainerName)
      .getBlobClient(
        s"data/${brandId}/players/data-${timestamp}.json"
      )
    blobClient.uploadFromFile(file.toString(), true)
    val blobSasPermission = new BlobSasPermission().setReadPermission(true)
    val expiryTime = OffsetDateTime.now().plusDays(1)
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

    logger.debug(body)
    sendMessageToSQS("batch-import", brandId, body)
  }
  override def close(): Unit = {}
}
