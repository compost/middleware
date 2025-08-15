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
  ActionTrigger,
  Body,
  Player,
  PlayerSQS,
  PlayerStatus,
  PlayerStatusBody,
  PlayerStore
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
import com.jada.models.WithdrawSuccessSQS
import com.jada.models.EmailVerificationSQS
import com.jada.models.DepositSQS
import com.jada.models.ForgotPasswordSQS

object MappingTransformer {

  val GenericUser = "GENERIC_USER"
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
) extends Transformer[String, Array[Byte], KeyValue[Unit, Unit]] {

  import MappingTransformer._
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
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
  }

  override def transform(key: String, v: Array[Byte]): KeyValue[Unit, Unit] = {
    val playerFromStore = Option(playerStore.get(key))

    val action: Option[Action] = Option(
      startStopStore.get(processorContext.applicationId())
    )

    val playerToSaveInStore = processorContext.topic() match {
      case config.topicCoordinator =>
        startStopStore.put(
          processorContext.applicationId(),
          deserialize[Action](v)
        )
        None
      case _ if action.exists(f => f.action == Common.stop) =>
        logger.debug("nothing to do")
        None
      case config.topicPlayersRepartitioned =>
        val inputPlayer = deserialize[Player](v)
        processPlayer(playerFromStore, inputPlayer)
      case config.topicActionTriggersRepartitioned => {
        val inputActionTrigger = deserialize[ActionTrigger](v)
        processActionTrigger(key, playerFromStore, inputActionTrigger)
      }
    }

    playerToSaveInStore.foreach { p =>
      if (
        (playerFromStore.isEmpty ||
          !(playerFromStore.get.player_id == p.player_id &&
            playerFromStore.get.reg_datetime == p.reg_datetime &&
            playerFromStore.get.brand_id == p.brand_id &&
            playerFromStore.get.brand_name == p.brand_name &&
            playerFromStore.get.first_name == p.first_name &&
            playerFromStore.get.last_name == p.last_name &&
            playerFromStore.get.email == p.email &&
            playerFromStore.get.phone_number == p.phone_number &&
            playerFromStore.get.language == p.language &&
            playerFromStore.get.affiliate_id == p.affiliate_id &&
            playerFromStore.get.is_self_excluded == p.is_self_excluded &&
            playerFromStore.get.first_dep_datetime == p.first_dep_datetime &&
            playerFromStore.get.dob == p.dob &&
            playerFromStore.get.country_id == p.country_id &&
            playerFromStore.get.consent_email == p.consent_email &&
            playerFromStore.get.consent_sms == p.consent_sms &&
            playerFromStore.get.vip == p.vip &&
            playerFromStore.get.vip_level == p.vip_level &&
            playerFromStore.get.currency_id == p.currency_id &&
            playerFromStore.get.test_user == p.test_user)) && p.isInit
      ) {
        val playerSQS = PlayerSQS(p, referentials)
        send(
          p.player_id.get,
          p.brand_id.get,
          GenericUser,
          "player_updated",
          playerSQS
        )
      }

      val toSave = if (p.isInit) {
        p.actionTriggers.foreach { at =>
          processActionTrigger(key, Some(p), at)
        }
        p.copy(actionTriggers = List.empty)
      } else {
        p
      }
      playerStore.put(key, toSave)

    }

    null
  }

  def deserialize[T: Decoder](data: Array[Byte]): T = {
    parser.decode[T](new String(data)).right.get
  }

  def processActionTrigger(
      key: String,
      playerFromStore: Option[PlayerStore],
      inputActionTrigger: ActionTrigger
  ): Option[PlayerStore] = {
    playerFromStore match {
      case Some(player) if player.isInit =>
        (
          inputActionTrigger.action,
          inputActionTrigger.action_value,
          inputActionTrigger.action_timestamp
        ) match {
          case (Some("withdrawal"), Some("SUCCESS"), _) =>
            send(
              player.player_id.get,
              player.brand_id.get,
              GenericUser,
              "player_action_triggers_withdrawal_success",
              WithdrawSuccessSQS(inputActionTrigger)
            )
          case (Some("withdrawal"), Some("IN_PROGRESS"), _) =>
            send(
              player.player_id.get,
              player.brand_id.get,
              GenericUser,
              "player_action_triggers_withdrawal_in_progress",
              WithdrawSuccessSQS(inputActionTrigger)
            )
          case (Some("email_verification"), Some(_), _) =>
            send(
              player.player_id.get,
              player.brand_id.get,
              GenericUser,
              "player_action_triggers_email_verification",
              EmailVerificationSQS(inputActionTrigger)
            )
          case (Some("deposit"), Some("SUCCESS"), Some(_)) =>
            send(
              player.player_id.get,
              player.brand_id.get,
              GenericUser,
              "player_action_triggers_deposit",
              DepositSQS(inputActionTrigger)
            )
          case (Some("self_exclusion"), Some("TRUE"), _) =>
            sendMessageToSQS(
              player.player_id.get,
              player.brand_id.get,
              s"""{"type":"GENERIC_USER","mappingSelector":"player_action_triggers_self_exclusion","contactId":"${player.player_id.get}","blocked":true}"""
            )
          case (Some("panic_button"), Some("TRUE"), _) =>
            sendMessageToSQS(
              player.player_id.get,
              player.brand_id.get,
              s"""{"type":"GENERIC_USER","mappingSelector":"player_action_triggers_panic_button","contactId":"${player.player_id.get}","properties":{"is_self_excluded":"1"}}"""
            )
          case (Some("forgot_password"), Some(_), _) =>
            send(
              player.player_id.get,
              player.brand_id.get,
              GenericUser,
              "player_action_triggers_forgot_password",
              ForgotPasswordSQS(inputActionTrigger)
            )
          case _ =>
            logger.debugv(
              "processActionTrigger not handled",
              Array(kv("key", key), kv("actionTrigger", inputActionTrigger)): _*
            )
        }
        None
      case Some(player) if !player.isInit =>
        Option(
          player.copy(actionTriggers =
            player.actionTriggers :+ inputActionTrigger
          )
        )
      case _ =>
        Option(
          new PlayerStore(
            player_id = inputActionTrigger.player_id,
            brand_id = inputActionTrigger.brand_id,
            isInit = false,
            actionTriggers = List(inputActionTrigger)
          )
        )
    }
  }

  def processPlayer(
      playerFromStore: Option[PlayerStore],
      inputPlayer: Player
  ): Option[PlayerStore] = {
    playerFromStore match {
      case Some(player) =>
        Option(
          player.copy(
            brand_id = inputPlayer.brand_id.orElse(player.brand_id),
            brand_name = inputPlayer.brand_id.orElse(player.brand_name),
            reg_datetime = inputPlayer.reg_datetime.orElse(player.reg_datetime),
            first_name = inputPlayer.first_name.orElse(player.first_name),
            last_name = inputPlayer.last_name.orElse(player.last_name),
            test_user = inputPlayer.test_user.orElse(player.test_user),
            email = inputPlayer.email.orElse(player.email),
            phone_number = inputPlayer.phone_number.orElse(player.phone_number),
            language = inputPlayer.language.orElse(player.language),
            affiliate_id = inputPlayer.affiliate_id.orElse(player.affiliate_id),
            is_self_excluded =
              inputPlayer.is_self_excluded.orElse(player.is_self_excluded),
            first_dep_datetime = inputPlayer.first_dep_datetime.orElse(
              player.first_dep_datetime
            ),
            dob = inputPlayer.dob.orElse(player.dob),
            vip = inputPlayer.vip.orElse(player.vip),
            vip_level = inputPlayer.vip_level.orElse(player.vip_level),
            currency_id = inputPlayer.currency_id.orElse(player.currency_id),
            country_id = inputPlayer.country_id.orElse(player.country_id),
            consent_sms = inputPlayer.consent_sms.orElse(player.consent_sms),
            consent_email =
              inputPlayer.consent_email.orElse(player.consent_email),
            isInit = true
          )
        )

      case None =>
        Option(
          PlayerStore(
            player_id = inputPlayer.player_id,
            test_user = inputPlayer.test_user,
            brand_id = inputPlayer.brand_id,
            brand_name = inputPlayer.brand_name,
            reg_datetime = inputPlayer.reg_datetime,
            first_name = inputPlayer.first_name,
            last_name = inputPlayer.last_name,
            email = inputPlayer.email,
            phone_number = inputPlayer.phone_number,
            language = inputPlayer.language,
            affiliate_id = inputPlayer.affiliate_id,
            is_self_excluded = inputPlayer.is_self_excluded,
            first_dep_datetime = inputPlayer.first_dep_datetime,
            dob = inputPlayer.dob,
            vip = inputPlayer.vip,
            vip_level = inputPlayer.vip_level,
            currency_id = inputPlayer.currency_id,
            country_id = inputPlayer.country_id,
            consent_sms = inputPlayer.consent_sms,
            consent_email = inputPlayer.consent_email,
            isInit = true
          )
        )
    }

  }

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
      computeMappingSelector(Option(brandId), mappingSelector),
      value
    )
    sendMessageToSQS(
      playerId,
      brandId,
      printer.print(Body.bodyEncoder[T].apply(body))
    )
  }

  def sendMessageToSQS(
      playerId: String,
      brandId: String,
      body: String
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
          .messageDeduplicationId(
            s"${playerId}-${java.util.UUID.randomUUID().toString}"
          )

      })) match {
        case Some(result) =>
          logger.debugv(
            "sent message to sqs",
            Array(
              kv("playerId", playerId),
              kv("url", queue),
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
            kv("url", queue),
            kv("body", body),
            e
          ): _*
        )
        throw e
    }
  }

  def computeMappingSelector(brandId: Option[String], mp: String): String = {
    if (brandId.exists(v => config.mappingSelectorPrefix.contains(v))) {
      s"DEV_${mp}"
    } else {
      mp
    }
  }
  override def close(): Unit = {}
}
