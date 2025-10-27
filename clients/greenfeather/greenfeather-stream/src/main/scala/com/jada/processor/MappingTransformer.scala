package com.jada.processor

import com.jada.configuration.ApplicationConfiguration
import com.jada.models._
import io.circe._
import io.circe.generic.auto._
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{
  ProcessorContext,
  PunctuationType,
  Punctuator
}
import org.apache.kafka.streams.state.KeyValueStore
import org.jboss.logging.Logger

import java.io.FileWriter
import java.nio.file.Files
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try
import java.nio.file.Path
import java.util.UUID
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.TemporalUnit
import java.time.temporal.Temporal
import java.time.temporal.ChronoUnit
import com.jada.WebPush

class MappingTransformer(
    config: ApplicationConfiguration,
    client: software.amazon.awssdk.services.sqs.SqsClient,
    playerStoreName: String
) extends Transformer[String, Array[Byte], KeyValue[String, PlayerStore]] {

  val sender = new Sender(config, client)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )

  val GenericUser = "GENERIC_USER"
  val UserBlocked = "USER_BLOCKED"

  private final val logger =
    Logger.getLogger(classOf[MappingTransformer])
  private var processorContext: ProcessorContext = _
  private var playerStore: KeyValueStore[String, PlayerStore] = _

  override def init(processorContext: ProcessorContext): Unit = {
    this.processorContext = processorContext
    this.playerStore = processorContext.getStateStore(playerStoreName)
  }

  def deserialize[T: Decoder](data: Array[Byte]): T = {
    parser.decode[T](new String(data)).right.get
  }

  def alert(
      topic: String,
      key: String,
      body: Array[Byte],
      kafkaTimestamp: Option[String]
  ): Unit = {
    kafkaTimestamp.foreach(t => {
      val kafkaDate = MappingTransformer.parseDate(t)
      val kafkaSecond = kafkaDate.getEpochSecond()
      val now = LocalDateTime.now(ZoneId.of("UTC"))
      val nowSecond = now.toEpochSecond(ZoneOffset.UTC)

      val diff = nowSecond - kafkaSecond
      if (diff >= 30) {
        logger.warnv(
          "alert process message after 30 secondes",
          Array(
            kv("topic", topic),
            kv("key", key),
            kv("contextDate", processorContext.timestamp()),
            kv("diff", diff),
            kv("kafkaTimestamp", t),
            kv("kafkaDate", kafkaDate),
            kv("now", now)
          ): _*
        )
      }
    })
  }

  override def transform(
      key: String,
      v: Array[Byte]
  ): KeyValue[String, PlayerStore] = {
    val playerFromStore = playerStore.get(key)
    val playerToSaveInStore = processorContext.topic() match {
      case config.topicPlayersBatch =>
        val inputPlayerBatch = deserialize[PlayerBatch](v)
        alert(
          config.topicPlayersBatch,
          key,
          v,
          inputPlayerBatch.kafka_timestamp
        )
        processPlayerBatch(playerFromStore, inputPlayerBatch)

      case config.topicPlayerStatus =>
        val inputPlayerStatus = deserialize[PlayerStatus](v)
        alert(
          config.topicPlayerStatus,
          key,
          v,
          inputPlayerStatus.kafka_timestamp
        )
        processPlayerStatus(playerFromStore, inputPlayerStatus)

      case config.topicPlayers =>
        val inputPlayer = deserialize[Player](v)
        alert(
          config.topicPlayers,
          key,
          v,
          inputPlayer.kafka_timestamp
        )
        processPlayer(playerFromStore, inputPlayer)

      case config.topicActionTrigger =>
        val actionTrigger = deserialize[ActionTrigger](v)
        processActionTrigger(actionTrigger)
        null

      case config.topicLogins =>
        val inputLogin = deserialize[Login](v)
        alert(
          config.topicLogins,
          key,
          v,
          inputLogin.kafka_timestamp
        )
        processLogin(inputLogin)
        if (playerFromStore != null) {
          processWebPush(
            playerFromStore,
            inputLogin.login_datetime.filterNot(v => v.isEmpty())
          )
        } else {
          null
        }

      case config.topicWagerings =>
        val input = deserialize[Wagering](v)
        val pl = Option(playerFromStore) match {
          case None =>
            PlayerStore(
              player_id = input.player_id,
              brand_id = input.brand_id,
              wagering_total_balance = input.total_balance
            )
          case Some(toUpdate) =>
            toUpdate.copy(
              wagering_total_balance =
                input.total_balance.orElse(toUpdate.wagering_total_balance)
            )
        }
        processWebPush(
          pl,
          input.result_datetime
            .filterNot(v => v.isEmpty())
            .orElse(input.bet_datetime.filterNot(v => v.isEmpty()))
        )

      case config.topicWallets =>
        val inputWallet = deserialize[Wallet](v)
        processWallet(inputWallet)
        null

      case config.topicBonusTransaction =>
        val inputBonus = deserialize[BonusTransaction](v)
        processBonusTransaction(inputBonus)
        null

      case config.topicUserConsentUpdate =>
        val inputConsent = deserialize[UserConsentUpdate](v)
        processConsent(playerFromStore, inputConsent)
    }
    if (playerToSaveInStore != null) {
      playerStore.put(key, playerToSaveInStore)
      new KeyValue(key, playerToSaveInStore)
    } else {
      null
    }
  }

  def processConsent(
      playerFromStore: PlayerStore,
      inputConsent: UserConsentUpdate
  ): PlayerStore = {
    val message =
      s"""
         |{
         |"type": "USER_CONSENT_UPDATE",
         |"mappingSelector": "${sender.computeMappingSelector(
          inputConsent.brand_id,
          "consent_change"
        )}",
         |"contactId": "${inputConsent.player_id.get}",
         |"channel": "${inputConsent.channel.getOrElse("").toUpperCase}",
         |"consented": ${toBoolean(inputConsent.consented.getOrElse(""))}
         |}
         |""".stripMargin

    Option(playerFromStore) match {
      case Some(player) =>
        if (
          inputConsent.channel.getOrElse("").toUpperCase == "EMAIL"
          && (player.email_consent.isEmpty
            || (player.email_consent.isDefined && player.email_consent != inputConsent.consented))
        ) {
          sender.sendMessageToSQS(
            inputConsent.player_id.get,
            inputConsent.brand_id.get,
            message
          )
          player.copy(email_consent = inputConsent.consented)

        } else if (
          inputConsent.channel.getOrElse("").toUpperCase == "SMS"
          && (player.sms_consent.isEmpty
            || (player.sms_consent.isDefined && playerFromStore.sms_consent != inputConsent.consented))
        ) {
          sender.sendMessageToSQS(
            inputConsent.player_id.get,
            inputConsent.brand_id.get,
            message
          )
          player.copy(sms_consent = inputConsent.consented)
        } else null
      case _ =>
        sender.sendMessageToSQS(
          inputConsent.player_id.get,
          inputConsent.brand_id.get,
          message
        )
        if (inputConsent.channel.getOrElse("").toUpperCase == "EMAIL")
          PlayerStore(
            player_id = inputConsent.player_id,
            brand_id = inputConsent.brand_id,
            email_consent = inputConsent.consented
          )
        else if (inputConsent.channel.getOrElse("").toUpperCase == "SMS")
          PlayerStore(
            player_id = inputConsent.player_id,
            brand_id = inputConsent.brand_id,
            sms_consent = inputConsent.consented
          )
        else null
    }
  }

  def toBoolean(v: String): Boolean = {
    v.toLowerCase() match {
      case "true"  => true
      case "false" => false
      case "0"     => false
      case "1"     => true
      case _       => false
    }
  }

  def processBonusTransaction(inputBonus: BonusTransaction) = {
    if (
      inputBonus.bonus_type.getOrElse("") == "deposit" || inputBonus.bonus_type
        .getOrElse("") == "manual"
    ) {
      val bonusReleasedMessage =
        s"""
           |{"type": "GENERIC_USER",
           |"mappingSelector":"${sender.computeMappingSelector(
            inputBonus.brand_id,
            "bonus_released"
          )}",
           |"contactId": "${inputBonus.player_id.get}",
           |"properties": {
           | "bonus_transaction_id": "${inputBonus.bonus_transaction_id
            .getOrElse("")}",
           | "bonus_transaction_date": "${inputBonus.transaction_datetime
            .map(x => Try(x.substring(0, 10)).getOrElse(x))
            .getOrElse("")}",
           | "bonus_type": "${inputBonus.bonus_type.getOrElse("")}",
           | "bonus_amount": "${inputBonus.bonus_amount_cur.get}",
           | "bonus_currency": "${inputBonus.bonus_currency.get}",
           | "frb_rounds": "${inputBonus.frb_rounds.get}",
           | "supported_games": "${inputBonus.supported_games.get}"
           |}
           |}
           |""".stripMargin
      sender.sendMessageToSQS(
        inputBonus.player_id.get,
        inputBonus.brand_id.get,
        bonusReleasedMessage
      )
    }
  }

  def processWallet(inputWallet: Wallet) = {

    val messageToSend =
      if (inputWallet.transaction_status_id.getOrElse("") == "dep_declined") {
        s"""
         |{"type": "GENERIC_USER",
         |"mappingSelector":"${sender.computeMappingSelector(
            inputWallet.brand_id,
            "failed_deposit"
          )}",
         |"contactId": "${inputWallet.player_id.get}",
         |"properties": {
         | "last_failed_deposit_date": "${inputWallet.transaction_datetime
            .map(x => Try(x.substring(0, 10)).getOrElse(x))
            .getOrElse("")}",
         | "last_failed_dep_transaction_id": "${inputWallet.transaction_id
            .getOrElse("")}",
         | "last_failed_deposit_currency": "${inputWallet.currency_description
            .getOrElse("")}",
         | "last_failed_deposit_amount": "${inputWallet.amount_cur.getOrElse(
            ""
          )}",
         | "brand_id": "${inputWallet.brand_id.get}"
         |}
         |}
         |""".stripMargin
      } else if (
        inputWallet.transaction_status_id.getOrElse("") == "dep_success"
      ) {
        s"""
         |{"type": "GENERIC_USER",
         |"mappingSelector":"${sender.computeMappingSelector(
            inputWallet.brand_id,
            "deposit_success"
          )}",
         |"contactId": "${inputWallet.player_id.get}",
         |"properties": {
         | "last_success_deposit_date": "${inputWallet.transaction_datetime
            .map(x => Try(x.substring(0, 10)).getOrElse(x))
            .getOrElse("")}",
         | "last_success_dep_transaction_id": "${inputWallet.transaction_id
            .getOrElse("")}",
         | "last_success_deposit_currency": "${inputWallet.currency_description
            .getOrElse("")}",
         | "last_success_deposit_amount": "${inputWallet.amount_cur.getOrElse(
            ""
          )}",
         | "brand_id": "${inputWallet.brand_id.get}"
         |}
         |}
         |""".stripMargin
      } else if (
        inputWallet.transaction_status_id.getOrElse("") == "wd_success"
      ) {
        s"""
         |{"type": "GENERIC_USER",
         |"mappingSelector":"${sender.computeMappingSelector(
            inputWallet.brand_id,
            "withdrawal_approved"
          )}",
         |"contactId": "${inputWallet.player_id.get}",
         |"properties": {
         | "last_approved_withdrawal_date": "${inputWallet.transaction_datetime
            .map(x => Try(x.substring(0, 10)).getOrElse(x))
            .getOrElse("")}",
         | "last_approved_wd_transaction_id": "${inputWallet.transaction_id
            .getOrElse("")}",
         | "last_approved_withdrawal_currency": "${inputWallet.currency_description
            .getOrElse("")}",
         | "last_approved_withdrawal_amount": "${inputWallet.amount_cur
            .getOrElse("")}",
         | "brand_id": "${inputWallet.brand_id.get}"
         |}
         |}
         |""".stripMargin
      } else if (
        inputWallet.transaction_status_id.getOrElse("") == "wd_pending"
      ) {
        s"""
         |{"type": "GENERIC_USER",
         |"mappingSelector":"${sender.computeMappingSelector(
            inputWallet.brand_id,
            "withdrawal_pending"
          )}",
         |"contactId": "${inputWallet.player_id.get}",
         |"properties": {
         | "last_pending_withdrawal_date": "${inputWallet.transaction_datetime
            .map(x => Try(x.substring(0, 10)).getOrElse(x))
            .getOrElse("")}",
         | "last_pending_withdrawal_currency": "${inputWallet.currency_description
            .getOrElse("")}",
         | "last_pending_wd_transaction_id": "${inputWallet.transaction_id
            .getOrElse("")}",
         | "last_pending_withdrawal_amount": "${inputWallet.amount_cur
            .getOrElse("")}",
         | "brand_id": "${inputWallet.brand_id.get}"
         |}
         |}
         |""".stripMargin
      } else null

    if (messageToSend != null) {
      sender.sendMessageToSQS(
        inputWallet.player_id.get,
        inputWallet.brand_id.get,
        messageToSend
      )
    }
  }

  def processWebPush(
      pl: PlayerStore,
      inputDateTime: Option[String]
  ): PlayerStore = {

    val (mp, updatedPlayer) = WebPush.getMappingSelectorPlayerStore(
      pl,
      inputDateTime,
      java.time.ZonedDateTime.now(java.time.ZoneId.of("GMT"))
    )
    if (mp.isDefined && updatedPlayer.isDefined) {
      val payload =
        s"""
         |{"type": "GENERIC_USER",
         |"mappingSelector":"${sender.computeMappingSelector(
            pl.brand_id,
            mp.get
          )}",
         |"contactId": "${pl.player_id.get}",
         |"properties": {
         | "originalId": "${pl.player_id.get}",
         | "brand_id": "${pl.brand_id.get}",
         | "total_balance": "${pl.wagering_total_balance.get}"
         |}
         |}
         |""".stripMargin
      sender.sendMessageToSQS(
        pl.player_id.get,
        pl.brand_id.get,
        payload
      )

      updatedPlayer.orNull
    } else {
      pl
    }
  }
  def processLogin(inputLogin: Login) = {
    val playerLogin =
      s"""
         |{"type": "GENERIC_USER",
         |"mappingSelector":"${sender.computeMappingSelector(
          inputLogin.brand_id,
          "player_login"
        )}",
         |"contactId": "${inputLogin.player_id.get}",
         |"properties": {
         | "login_date": "${inputLogin.login_datetime
          .map(x => Try(x.substring(0, 10)).getOrElse(x))
          .getOrElse("")}",
         | "login_success": "${toBoolean(
          inputLogin.login_success.getOrElse("")
        )}",
         | "brand_id": "${inputLogin.brand_id.get}"
         |}
         |}
         |""".stripMargin
    sender.sendMessageToSQS(
      inputLogin.player_id.get,
      inputLogin.brand_id.get,
      playerLogin
    )
  }

  def processActionTrigger(actionTrigger: ActionTrigger) = {
    if (actionTrigger.action.getOrElse("") == "reg_resend") {
      val playerResendEmailVerif =
        s"""
           |{"type": "GENERIC_USER",
           |"mappingSelector":"${sender.computeMappingSelector(
            actionTrigger.brand_id,
            "player_action_triggers_resend_email_verification"
          )}",
           |"contactId": "${actionTrigger.player_id.get}",
           |"properties": {
           | "email_verification_url": "${actionTrigger.action_value.getOrElse(
            ""
          )}",
           | "brand_id": "${actionTrigger.brand_id.get}"
           |}
           |}
           |""".stripMargin
      sender.sendMessageToSQS(
        actionTrigger.player_id.get,
        actionTrigger.brand_id.get,
        playerResendEmailVerif
      )
    }
    if (actionTrigger.action.getOrElse("") == "reset_password") {
      val playerResetPassword =
        s"""
           |{"type": "GENERIC_USER",
           |"mappingSelector":"${sender.computeMappingSelector(
            actionTrigger.brand_id,
            "player_action_triggers_reset_password"
          )}",
           |"contactId": "${actionTrigger.player_id.get}",
           |"properties": {
           | "reset_password_url": "${actionTrigger.action_value.getOrElse(
            ""
          )}",
           | "brand_id": "${actionTrigger.brand_id.get}"
           |}
           |}
           |""".stripMargin
      sender.sendMessageToSQS(
        actionTrigger.player_id.get,
        actionTrigger.brand_id.get,
        playerResetPassword
      )
    }
  }

  def processPlayerStatus(
      playerFromStore: PlayerStore,
      inputPlayerStatus: PlayerStatus
  ): PlayerStore = {

    if (
      (inputPlayerStatus.player_status_id.getOrElse(
        ""
      ) == "3" || inputPlayerStatus.player_status_id.getOrElse(
        ""
      ) == "2") && inputPlayerStatus.blocked_start_date.isDefined
    ) {
      val mappingSelector = sender.computeMappingSelector(
        inputPlayerStatus.brand_id,
        "self_exclusion_communication"
      )
      val add20Years = inputPlayerStatus.blocked_start_date
        .map(v => {
          if (v.trim().length == 19) {
            val date = Instant.from(
              MappingTransformer.DateTimeDigitWithoutT.parse(v.trim)
            )
            val plus20Years = date.plus(20 * 365, ChronoUnit.DAYS)
            MappingTransformer.DateTimeDigitWithoutT.format(plus20Years)
          } else {
            v
          }
        })
        .getOrElse("")
      val enddate = inputPlayerStatus.blocked_end_date match {
        case Some(v) if v.trim() == "" =>
          MappingTransformer.onlyDate(add20Years)
        case None => MappingTransformer.onlyDate(add20Years)
        case _ =>
          MappingTransformer.onlyDate(inputPlayerStatus.blocked_end_date.get)
      }

      val playerSelfExclusionCommunication =
        s"""
           |{"type": "GENERIC_USER",
           |"mappingSelector":"$mappingSelector",
           |"contactId": "${inputPlayerStatus.player_id.get}",
           |"properties": {
           | "is_self_exclude": "true",
           | "is_self_excluded_startdate": "${inputPlayerStatus.blocked_start_date
            .map(MappingTransformer.onlyDate(_))
            .getOrElse("")}",
           | "is_self_excluded_enddate": "${enddate}",
           | "brand_id": "${inputPlayerStatus.brand_id.get}"
           |}
           |}
           |""".stripMargin
      sender.sendMessageToSQS(
        inputPlayerStatus.player_id.get,
        inputPlayerStatus.brand_id.get,
        playerSelfExclusionCommunication
      )
      if (playerFromStore != null) {
        playerFromStore.copy(
          player_send_self_exclusion = true,
          nb_player_send_self_exclusion =
            playerFromStore.nb_player_send_self_exclusion
              .map(i => i + 1)
              .orElse(Some(1)),
          is_self_excluded_until = inputPlayerStatus.blocked_end_date,
          player_status_id = inputPlayerStatus.player_status_id
        )
      } else {
        PlayerStore(
          player_id = inputPlayerStatus.player_id,
          brand_id = inputPlayerStatus.brand_id,
          player_send_self_exclusion = true,
          nb_player_send_self_exclusion = Some(1),
          is_self_excluded_until = inputPlayerStatus.blocked_end_date,
          player_status_id = inputPlayerStatus.player_status_id
        )
      }

    } else if (
      inputPlayerStatus.player_status_id.getOrElse(
        ""
      ) == "1" && inputPlayerStatus.blocked_start_date.isDefined
    ) { // Timeout
      val mappingSelector = sender.computeMappingSelector(
        inputPlayerStatus.brand_id,
        "timeout_communication"
      )
      val add20Years = inputPlayerStatus.blocked_start_date
        .map(v => {
          if (v.trim().length == 19) {
            val date = Instant.from(
              MappingTransformer.DateTimeDigitWithoutT.parse(v.trim)
            )
            val plus20Years = date.plus(20 * 365, ChronoUnit.DAYS)
            MappingTransformer.DateTimeDigitWithoutT.format(plus20Years)
          } else {
            v
          }
        })
        .getOrElse("")
      val enddate = inputPlayerStatus.blocked_end_date match {
        case Some(v) if v.trim() == "" =>
          MappingTransformer.onlyDate(add20Years)
        case None => MappingTransformer.onlyDate(add20Years)
        case _ =>
          MappingTransformer.onlyDate(inputPlayerStatus.blocked_end_date.get)
      }

      val playerTimeOutCommunication =
        s"""
           |{"type": "GENERIC_USER",
           |"mappingSelector":"$mappingSelector",
           |"contactId": "${inputPlayerStatus.player_id.get}",
           |"properties": {
           | "is_timeout": "true",
           | "is_timeout_startdate": "${inputPlayerStatus.blocked_start_date
            .map(MappingTransformer.onlyDate(_))
            .getOrElse("")}",
           | "is_timeout_enddate": "${enddate}",
           | "brand_id": "${inputPlayerStatus.brand_id.get}"
           |}
           |}
           |""".stripMargin
      sender.sendMessageToSQS(
        inputPlayerStatus.player_id.get,
        inputPlayerStatus.brand_id.get,
        playerTimeOutCommunication
      )

      if (playerFromStore != null) {
        playerFromStore.copy(
          nb_player_send_timeout = playerFromStore.nb_player_send_timeout
            .map(i => i + 1)
            .orElse(Some(1)),
          player_send_timeout = true,
          is_timeout_until = inputPlayerStatus.blocked_end_date,
          player_status_id = inputPlayerStatus.player_status_id
        )
      } else {
        PlayerStore(
          player_id = inputPlayerStatus.player_id,
          brand_id = inputPlayerStatus.brand_id,
          player_send_timeout = true,
          nb_player_send_timeout = Some(1),
          is_timeout_until = inputPlayerStatus.blocked_end_date,
          player_status_id = inputPlayerStatus.player_status_id
        )
      }
    } else if (inputPlayerStatus.player_status_id.isEmpty) {
      if (
        playerFromStore.player_status_id.isDefined && (playerFromStore.player_status_id.get == "3" || playerFromStore.player_status_id.get == "1")
      ) {
        val (mappingSelector, blockProperty) =
          if (playerFromStore.player_status_id.get == "3")
            ("self_exclusion_expired", "is_self_excluded")
          else ("timeout_expired", "is_timeout")

        // send blocking expired
        val playerBlockExpired =
          s"""{"type":"${UserBlocked}",
             |"mappingSelector":"${sender.computeMappingSelector(
              inputPlayerStatus.brand_id,
              mappingSelector
            )}",
             |"contactId":"${inputPlayerStatus.player_id.get}",
             |"blockedUntil": "${Instant.now.toString.substring(0, 10)}",
             |"properties": {
             |  "$blockProperty":"false"
             |}
             |}""".stripMargin
        sender.sendMessageToSQS(
          inputPlayerStatus.player_id.get,
          inputPlayerStatus.brand_id.get,
          playerBlockExpired
        )
        // set back player status to null
        playerFromStore.copy(player_status_id = None)
      } else null
    } else null
  }

  def processPlayer(playerFromStore: PlayerStore, inputPlayer: Player) = {
    val newPlayer = Option(playerFromStore) match {
      case Some(player) =>
        val np = player.copy(
          player_id = inputPlayer.player_id,
          brand_id = inputPlayer.brand_id.orElse(player.brand_id),
          reg_datetime = inputPlayer.reg_datetime.orElse(player.reg_datetime),
          first_name = inputPlayer.first_name.orElse(player.first_name),
          last_name = inputPlayer.last_name.orElse(player.last_name),
          email = inputPlayer.email.orElse(player.email),
          phone_number = inputPlayer.phone_number.orElse(player.phone_number),
          language = inputPlayer.language.orElse(player.language),
          currency_id = inputPlayer.currency_id.orElse(player.currency_id),
          currency_description = inputPlayer.currency_description.orElse(
            player.currency_description
          ),
          brand_description =
            inputPlayer.brand_description.orElse(player.brand_description),
          login_name = inputPlayer.login_name.orElse(player.login_name),
          domain_url = inputPlayer.domain_url.orElse(player.domain_url),
          verification_url =
            inputPlayer.verification_url.orElse(player.verification_url),
          reg_bonus_amount_cash = inputPlayer.reg_bonus_amount_cash.orElse(
            player.reg_bonus_amount_cash
          ),
          reg_bonus_amount_frb = inputPlayer.reg_bonus_amount_frb.orElse(
            player.reg_bonus_amount_frb
          ),
          ver_bonus_amount_cash = inputPlayer.ver_bonus_amount_cash.orElse(
            player.ver_bonus_amount_cash
          ),
          ver_bonus_amount_frb = inputPlayer.ver_bonus_amount_frb.orElse(
            player.ver_bonus_amount_frb
          ),
          reg_supported_games_frb = inputPlayer.reg_supported_games_frb.orElse(
            player.reg_supported_games_frb
          ),
          ver_supported_games_frb = inputPlayer.ver_supported_games_frb.orElse(
            player.ver_supported_games_frb
          ),
          player_info_from_batch = false
        )

        if (
          player.email.isDefined && inputPlayer.email.isDefined && inputPlayer.email != player.email
        ) {
          // if we receive the player a second time, it means they have requested email change:
          val mappingSelector = sender.computeMappingSelector(
            inputPlayer.brand_id,
            "player_email_change_request"
          )
          val playerEmailChangeRequest =
            s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector":"$mappingSelector",
               |"contactId": "${inputPlayer.player_id.get}",
               |"properties": {
               | "emailAddress": "${inputPlayer.email.getOrElse("")}",
               | "email_verification_url": "${inputPlayer.verification_url
                .getOrElse("")}",
               | "brand_id": "${inputPlayer.brand_id.get}"
               |}
               |}
               |""".stripMargin
          sender.sendMessageToSQS(
            inputPlayer.player_id.get,
            inputPlayer.brand_id.get,
            playerEmailChangeRequest
          )
        }

        np

      case None =>
        // partially register non null infos
        PlayerStore(
          player_id = inputPlayer.player_id,
          brand_id = inputPlayer.brand_id,
          reg_datetime = inputPlayer.reg_datetime,
          first_name = inputPlayer.first_name,
          last_name = inputPlayer.last_name,
          email = inputPlayer.email,
          phone_number = inputPlayer.phone_number,
          language = inputPlayer.language,
          currency_id = inputPlayer.currency_id,
          currency_description = inputPlayer.currency_description,
          brand_description = inputPlayer.brand_description,
          login_name = inputPlayer.login_name,
          domain_url = inputPlayer.domain_url,
          verification_url = inputPlayer.verification_url,
          reg_bonus_amount_cash = inputPlayer.reg_bonus_amount_cash,
          reg_bonus_amount_frb = inputPlayer.reg_bonus_amount_frb,
          ver_bonus_amount_cash = inputPlayer.ver_bonus_amount_cash,
          ver_bonus_amount_frb = inputPlayer.ver_bonus_amount_frb,
          ver_supported_games_frb = inputPlayer.ver_supported_games_frb,
          reg_supported_games_frb = inputPlayer.reg_supported_games_frb,
          player_info_from_batch = false
        )
    }
    if (
      (playerFromStore == null || (playerFromStore != null && playerFromStore.reg_datetime.isEmpty)) && inputPlayer.reg_datetime.isDefined
    ) {
      val playerSQS = PlayerSQS(newPlayer)
      sender.send(
        newPlayer.player_id.get,
        newPlayer.brand_id.get,
        GenericUser,
        sender
          .computeMappingSelector(newPlayer.brand_id, "player_registration"),
        playerSQS
      )

      if (
        inputPlayer.verification_url.isDefined && inputPlayer.verification_url
          .getOrElse("")
          .nonEmpty
      ) {
        val mappingSelector = sender.computeMappingSelector(
          inputPlayer.brand_id,
          "player_email_verification"
        )
        val playerEmailVerificationMessage =
          s"""
             |{"type": "GENERIC_USER",
             |"mappingSelector":"$mappingSelector",
             |"contactId": "${inputPlayer.player_id.get}",
             |"properties": {
             | "email_verification_url": "${inputPlayer.verification_url.get}",
             | "brand_id": "${inputPlayer.brand_id.get}"
             |}
             |}
             |""".stripMargin
        sender.sendMessageToSQS(
          inputPlayer.player_id.get,
          inputPlayer.brand_id.get,
          playerEmailVerificationMessage
        )
      }

    }
    newPlayer
  }

  def processPlayerBatch(
      playerFromStore: PlayerStore,
      inputPlayerBatch: PlayerBatch
  ): PlayerStore = {
    Option(playerFromStore) match {
      case Some(player) =>
        player.copy(
          player_id = inputPlayerBatch.player_id,
          brand_id = inputPlayerBatch.brand_id.orElse(player.brand_id),
          brand_description =
            inputPlayerBatch.brand_description.orElse(player.brand_description),
          reg_datetime =
            inputPlayerBatch.reg_datetime.orElse(player.reg_datetime),
          first_name = inputPlayerBatch.first_name.orElse(player.first_name),
          last_name = inputPlayerBatch.last_name.orElse(player.last_name),
          email = inputPlayerBatch.email.orElse(player.email),
          phone_number =
            inputPlayerBatch.phone_number.orElse(player.phone_number),
          language = inputPlayerBatch.language.orElse(player.language),
          affiliate_id =
            inputPlayerBatch.affiliate_id.orElse(player.affiliate_id),
          dob = inputPlayerBatch.dob.orElse(player.dob),
          country_description = inputPlayerBatch.country_description.orElse(
            player.country_description
          ),
          currency_description = inputPlayerBatch.currency_description.orElse(
            player.currency_description
          ),
          account_status =
            inputPlayerBatch.account_status.orElse(player.account_status),
          vip_status = inputPlayerBatch.vip_status.orElse(player.vip_status),
          login_name = inputPlayerBatch.login_name.orElse(player.login_name),
          domain_url = inputPlayerBatch.domain_url.orElse(player.domain_url),
          gender = inputPlayerBatch.gender.orElse(player.gender),
          mobile_status =
            inputPlayerBatch.mobile_status.orElse(player.mobile_status),
          email_verified =
            inputPlayerBatch.email_verified.orElse(player.email_verified),
          la_bcasinocom =
            inputPlayerBatch.la_bcasinocom.orElse(player.la_bcasinocom),
          la_boocasino =
            inputPlayerBatch.la_boocasino.orElse(player.la_boocasino),
          la_boocasinoca =
            inputPlayerBatch.la_boocasinoca.orElse(player.la_boocasinoca),
          la_galacticwins =
            inputPlayerBatch.la_galacticwins.orElse(player.la_galacticwins),
          la_mrfortune =
            inputPlayerBatch.la_mrfortune.orElse(player.la_mrfortune),
          marketing_seg =
            inputPlayerBatch.marketing_seg.orElse(player.marketing_seg),
          total_bonus_given =
            inputPlayerBatch.total_bonus_given.orElse(player.total_bonus_given),
          total_wd_amount =
            inputPlayerBatch.total_wd_amount.orElse(player.total_wd_amount),
          avg_deposit = inputPlayerBatch.avg_deposit.orElse(player.avg_deposit),
          avg_bet = inputPlayerBatch.avg_bet.orElse(player.avg_bet),
          avg_bet_eur = inputPlayerBatch.avg_bet_eur.orElse(player.avg_bet_eur),
          favorite_game =
            inputPlayerBatch.favorite_game.orElse(player.favorite_game),
          fav_game_cat =
            inputPlayerBatch.fav_game_cat.orElse(player.fav_game_cat),
          bonus_ratio = inputPlayerBatch.bonus_ratio.orElse(player.bonus_ratio),
          wd_ratio = inputPlayerBatch.wd_ratio.orElse(player.wd_ratio),
          turnover_ratio =
            inputPlayerBatch.turnover_ratio.orElse(player.turnover_ratio),
          ngr = inputPlayerBatch.ngr.orElse(player.ngr),
          chargeback = inputPlayerBatch.chargeback.orElse(player.chargeback),
          vip_seg = inputPlayerBatch.vip_seg.orElse(player.vip_seg),
          bonus_optout =
            inputPlayerBatch.bonus_optout.orElse(player.bonus_optout),
          total_number_of_deposits = inputPlayerBatch.total_number_of_deposits
            .orElse(player.total_number_of_deposits),
          total_deposit_amount = inputPlayerBatch.total_deposit_amount.orElse(
            player.total_deposit_amount
          ),
          last_game_played =
            inputPlayerBatch.last_game_played.orElse(player.last_game_played),
          total_balance =
            inputPlayerBatch.total_balance.orElse(player.total_balance),
          bonus_balance =
            inputPlayerBatch.bonus_balance.orElse(player.bonus_balance),
          cp_balance = inputPlayerBatch.cp_balance.orElse(player.cp_balance),
          ftd_date = inputPlayerBatch.ftd_date.orElse(player.ftd_date),
          last_deposit_date =
            inputPlayerBatch.last_deposit_date.orElse(player.last_deposit_date),
          last_bonus_given =
            inputPlayerBatch.last_bonus_given.orElse(player.last_bonus_given),
          last_login_date =
            inputPlayerBatch.last_login_date.orElse(player.last_login_date),
          pending_withdrawal = inputPlayerBatch.pending_withdrawal.orElse(
            player.pending_withdrawal
          ),
          pending_withdrawal_euro = inputPlayerBatch.pending_withdrawal_euro
            .orElse(player.pending_withdrawal_euro),
          total_deposit_amount_eur = inputPlayerBatch.total_deposit_amount_eur
            .orElse(player.total_deposit_amount_eur),
          total_bonus_given_eur = inputPlayerBatch.total_bonus_given_eur.orElse(
            player.total_bonus_given_eur
          ),
          total_balance_eur =
            inputPlayerBatch.total_balance_eur.orElse(player.total_balance_eur),
          bonus_balance_eur =
            inputPlayerBatch.bonus_balance_eur.orElse(player.bonus_balance_eur),
          total_wd_amount_eur = inputPlayerBatch.total_wd_amount_eur.orElse(
            player.total_wd_amount_eur
          ),
          avg_deposit_eur =
            inputPlayerBatch.avg_deposit_eur.orElse(player.avg_deposit_eur),
          cp_accumulated =
            inputPlayerBatch.cp_accumulated.orElse(player.cp_accumulated),
          player_info_from_batch = true,
          player_batch_to_send = true,
          id_status = inputPlayerBatch.id_status.orElse(player.id_status),
          poa_status = inputPlayerBatch.poa_status.orElse(player.poa_status)
        )

      case None =>
        // partially register non null infos
        PlayerStore(
          player_id = inputPlayerBatch.player_id,
          brand_id = inputPlayerBatch.brand_id,
          brand_description = inputPlayerBatch.brand_description,
          reg_datetime = inputPlayerBatch.reg_datetime,
          first_name = inputPlayerBatch.first_name,
          last_name = inputPlayerBatch.last_name,
          email = inputPlayerBatch.email,
          phone_number = inputPlayerBatch.phone_number,
          language = inputPlayerBatch.language,
          affiliate_id = inputPlayerBatch.affiliate_id,
          dob = inputPlayerBatch.dob,
          country_description = inputPlayerBatch.country_description,
          currency_description = inputPlayerBatch.currency_description,
          account_status = inputPlayerBatch.account_status,
          vip_status = inputPlayerBatch.vip_status,
          login_name = inputPlayerBatch.login_name,
          domain_url = inputPlayerBatch.domain_url,
          gender = inputPlayerBatch.gender,
          mobile_status = inputPlayerBatch.mobile_status,
          email_verified = inputPlayerBatch.email_verified,
          la_bcasinocom = inputPlayerBatch.la_bcasinocom,
          la_boocasino = inputPlayerBatch.la_boocasino,
          la_boocasinoca = inputPlayerBatch.la_boocasinoca,
          la_galacticwins = inputPlayerBatch.la_galacticwins,
          la_mrfortune = inputPlayerBatch.la_mrfortune,
          marketing_seg = inputPlayerBatch.marketing_seg,
          total_bonus_given = inputPlayerBatch.total_bonus_given,
          total_wd_amount = inputPlayerBatch.total_wd_amount,
          avg_deposit = inputPlayerBatch.avg_deposit,
          avg_bet = inputPlayerBatch.avg_bet,
          avg_bet_eur = inputPlayerBatch.avg_bet_eur,
          favorite_game = inputPlayerBatch.favorite_game,
          fav_game_cat = inputPlayerBatch.fav_game_cat,
          bonus_ratio = inputPlayerBatch.bonus_ratio,
          wd_ratio = inputPlayerBatch.wd_ratio,
          turnover_ratio = inputPlayerBatch.turnover_ratio,
          ngr = inputPlayerBatch.ngr,
          chargeback = inputPlayerBatch.chargeback,
          vip_seg = inputPlayerBatch.vip_seg,
          bonus_optout = inputPlayerBatch.bonus_optout,
          total_number_of_deposits = inputPlayerBatch.total_number_of_deposits,
          total_deposit_amount = inputPlayerBatch.total_deposit_amount,
          last_game_played = inputPlayerBatch.last_game_played,
          total_balance = inputPlayerBatch.total_balance,
          bonus_balance = inputPlayerBatch.bonus_balance,
          cp_balance = inputPlayerBatch.cp_balance,
          ftd_date = inputPlayerBatch.ftd_date,
          last_deposit_date = inputPlayerBatch.last_deposit_date,
          last_bonus_given = inputPlayerBatch.last_bonus_given,
          last_login_date = inputPlayerBatch.last_login_date,
          pending_withdrawal = inputPlayerBatch.pending_withdrawal,
          pending_withdrawal_euro = inputPlayerBatch.pending_withdrawal_euro,
          total_deposit_amount_eur = inputPlayerBatch.total_deposit_amount_eur,
          total_bonus_given_eur = inputPlayerBatch.total_bonus_given_eur,
          total_balance_eur = inputPlayerBatch.total_balance_eur,
          bonus_balance_eur = inputPlayerBatch.bonus_balance_eur,
          total_wd_amount_eur = inputPlayerBatch.total_wd_amount_eur,
          avg_deposit_eur = inputPlayerBatch.avg_deposit_eur,
          cp_accumulated = inputPlayerBatch.cp_accumulated,
          player_info_from_batch = true,
          player_batch_to_send = true,
          id_status = inputPlayerBatch.id_status,
          poa_status = inputPlayerBatch.poa_status
        )
    }
  }

  override def close(): Unit = {}

}

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

  val DateTimeDigitWithoutT = DateTimeFormatter
    .ofPattern("yyyy-MM-dd' 'HH:mm:ss")
    .withZone(ZoneId.systemDefault())

  def onlyDate(date: String): String = {
    if (date.length() > 10) {
      date.substring(0, 10)
    } else {
      date
    }
  }
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

}
