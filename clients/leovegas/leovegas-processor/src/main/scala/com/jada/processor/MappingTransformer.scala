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
  BetAndResult,
  Body,
  Player,
  PlayerExtended,
  PlayerStore,
  Wagering
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
import java.time.temporal.TemporalUnit

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

  def toBoolean(v: Option[String]): Boolean = {
    v match {
      case Some("true")  => true
      case Some("false") => false
      case Some("0")     => false
      case Some("1")     => true
      case _             => false
    }
  }

}

class MappingTransformer(
    config: ApplicationConfiguration,
    client: software.amazon.awssdk.services.sqs.SqsClient,
    playerStoreName: String
) extends Transformer[String, Array[Byte], KeyValue[Unit, Unit]]
    with Punctuator {

  import MappingTransformer._
  val Country_BR = "76"
  val Country_GB = "826"
  val Country_NL = "528"

  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  val bootstrap = new AtomicBoolean(true)

  val GenericUser = "GENERIC_USER"

  private final val logger =
    Logger.getLogger(classOf[MappingTransformer])
  private var processorContext: ProcessorContext = _
  private var playerStore: KeyValueStore[String, PlayerStore] = _

  override def init(processorContext: ProcessorContext): Unit = {
    logger.debug(f"${config}")
    this.processorContext = processorContext
    this.playerStore = processorContext.getStateStore(playerStoreName)
    this.processorContext.schedule(
      config.punctuator,
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }

  def getSQS(ps: PlayerStore): String = {
    (
      ps.licenseuid,
      ps.country_id,
      ps.country,
      ps.operator
    ) match {
      case (_, Some(Country_BR), _, _) =>
        config.sqsQueueBR
      case (_, _, Some(country), _)
          if Set("BR").contains(country.toUpperCase()) =>
        config.sqsQueueBR
      case (Some(v), _, _, _) if v.toUpperCase() == "IOM" =>
        config.sqsQueueLicenseuid
      case (_, Some(Country_GB), _, _) =>
        config.sqsQueueUK
      case (_, _, Some(country), _)
          if Set("UK", "GB").contains(country.toUpperCase()) =>
        config.sqsQueueUK
      case (_, Some(Country_NL), _, Some(operator))
          if operator.toUpperCase() == "MGM" =>
        config.sqsQueueOperatorNL
      case (_, _, Some(country), Some(operator))
          if operator.toUpperCase() == "MGM" && Set("NL").contains(
            country.toUpperCase()
          ) =>
        config.sqsQueueOperatorNL

      case (_, _, _, _) => config.sqsQueue
    }
  }

  override def transform(key: String, v: Array[Byte]): KeyValue[Unit, Unit] = {
    val playerFromStore = playerStore.get(key)
    val playerToSaveInStore = processorContext.topic() match {

      case config.topicExtendedPlayersRepartitioned =>
        val inputPlayer = deserialize[PlayerExtended](v)
        Option(playerFromStore) match {
          case Some(storeData) =>
            storeData.copy(
              brand_id = inputPlayer.brand_id.orElse(storeData.brand_id),
              licenseuid = inputPlayer.licenseuid.orElse(storeData.licenseuid),
              operator = inputPlayer.operator.orElse(storeData.licenseuid),
              country = inputPlayer.country.orElse(storeData.country)
            )

          case None =>
            // partially register non null infos
            PlayerStore(
              player_id = inputPlayer.originalId,
              brand_id = inputPlayer.brand_id,
              licenseuid = inputPlayer.licenseuid,
              operator = inputPlayer.operator,
              country = inputPlayer.country,
              betsAndResults = List.empty
            )
        }
      case config.topicPlayersRepartitioned =>
        val inputPlayer = deserialize[Player](v)
        val regDateAsInstant = inputPlayer.reg_datetime.map(v => parseDate(v))
        if (regDateAsInstant.isDefined) {
          Option(playerFromStore) match {
            case Some(storeData) =>
              storeData.copy(
                brand_id = inputPlayer.brand_id.orElse(storeData.brand_id),
                reg_datetime = Some(parseDate(inputPlayer.reg_datetime.get))
                  .orElse(storeData.reg_datetime),
                first_name =
                  inputPlayer.first_name.orElse(storeData.first_name),
                last_name = inputPlayer.last_name.orElse(storeData.last_name),
                test_user = inputPlayer.test_user.orElse(storeData.test_user),
                email = inputPlayer.email.orElse(storeData.email),
                phone_number =
                  inputPlayer.phone_number.orElse(storeData.phone_number),
                is_self_excluded = inputPlayer.is_self_excluded.orElse(
                  storeData.is_self_excluded
                ),
                vip = inputPlayer.vip.orElse(storeData.vip),
                currency_id =
                  inputPlayer.currency_id.orElse(storeData.currency_id),
                country_id =
                  inputPlayer.country_id.orElse(storeData.country_id),
                licenseuid =
                  inputPlayer.licenseuid.orElse(storeData.licenseuid),
                operator = inputPlayer.operator.orElse(storeData.licenseuid),
                country = inputPlayer.country.orElse(storeData.country)
              )

            case None =>
              // partially register non null infos
              PlayerStore(
                player_id = inputPlayer.player_id,
                test_user = inputPlayer.test_user,
                brand_id = inputPlayer.brand_id,
                reg_datetime = Some(parseDate(inputPlayer.reg_datetime.get)),
                first_name = inputPlayer.first_name,
                last_name = inputPlayer.last_name,
                email = inputPlayer.email,
                phone_number = inputPlayer.phone_number,
                is_self_excluded = inputPlayer.is_self_excluded,
                vip = inputPlayer.vip,
                currency_id = inputPlayer.currency_id,
                country_id = inputPlayer.country_id,
                licenseuid = inputPlayer.licenseuid,
                operator = inputPlayer.operator,
                country = inputPlayer.country,
                betsAndResults = List.empty[BetAndResult]
              )
          }
        } else null
      case config.topicPlayersHistory =>
        if (v != null) {
          val inputPlayer = deserialize[Player](v)
          Option(playerFromStore) match {
            case Some(storeData) =>
              storeData.copy(
                brand_id = inputPlayer.brand_id.orElse(storeData.brand_id),
                reg_datetime = Some(parseDate(inputPlayer.reg_datetime.get))
                  .orElse(storeData.reg_datetime),
                first_name =
                  inputPlayer.first_name.orElse(storeData.first_name),
                last_name = inputPlayer.last_name.orElse(storeData.last_name),
                test_user = inputPlayer.test_user.orElse(storeData.test_user),
                email = inputPlayer.email.orElse(storeData.email),
                phone_number =
                  inputPlayer.phone_number.orElse(storeData.phone_number),
                is_self_excluded = inputPlayer.is_self_excluded.orElse(
                  storeData.is_self_excluded
                ),
                vip = inputPlayer.vip.orElse(storeData.vip),
                currency_id =
                  inputPlayer.currency_id.orElse(storeData.currency_id),
                country_id =
                  inputPlayer.country_id.orElse(storeData.country_id),
                operator = inputPlayer.operator.orElse(storeData.operator),
                country = inputPlayer.country.orElse(storeData.country),
                licenseuid = inputPlayer.licenseuid.orElse(storeData.licenseuid)
              )

            case None =>
              // partially register non null infos
              PlayerStore(
                player_id = inputPlayer.player_id,
                country_id = inputPlayer.country_id,
                operator = inputPlayer.operator,
                country = inputPlayer.country,
                licenseuid = inputPlayer.licenseuid,
                test_user = inputPlayer.test_user,
                brand_id = inputPlayer.brand_id,
                reg_datetime = Some(parseDate(inputPlayer.reg_datetime.get)),
                first_name = inputPlayer.first_name,
                last_name = inputPlayer.last_name,
                email = inputPlayer.email,
                phone_number = inputPlayer.phone_number,
                is_self_excluded = inputPlayer.is_self_excluded,
                vip = inputPlayer.vip,
                currency_id = inputPlayer.currency_id,
                betsAndResults = List.empty[BetAndResult]
              )
          }
        } else { null }

      case config.topicWageringsRepartitioned =>
        val wagering = deserialize[Wagering](v)
        if (
          (playerFromStore == null || (playerFromStore != null && !playerFromStore.processed && !playerFromStore.cleaned && playerFromStore.wagering.isEmpty)) &&
          toBoolean(wagering.has_resulted) && wagering.vertical_id.contains(
            "2"
          )
        ) {
          val newPlayer = Option(playerFromStore) match {
            case Some(storeData) =>
              storeData.copy(
                player_id = wagering.player_id.orElse(storeData.player_id),
                brand_id = wagering.brand_id.orElse(storeData.brand_id),
                country_id = wagering.country_id.orElse(storeData.country_id),
                operator = wagering.operator.orElse(storeData.operator),
                country = wagering.country.orElse(storeData.country),
                licenseuid = wagering.licenseuid.orElse(storeData.licenseuid),
                wagering = Some(wagering)
              )

            case None =>
              // partially register non null infos
              PlayerStore(
                player_id = wagering.player_id,
                brand_id = wagering.brand_id,
                country_id = wagering.country_id,
                operator = wagering.operator,
                country = wagering.country,
                licenseuid = wagering.licenseuid,
                wagering = Some(wagering)
              )
          }

          newPlayer
        } else { null }
    }
    if (playerToSaveInStore != null) {
      playerStore.put(key, playerToSaveInStore)
    }
    new KeyValue(key, playerToSaveInStore)
    null
  }

  def deserialize[T: Decoder](data: Array[Byte]): T = {
    parser.decode[T](new String(data)).right.get
  }

  override def punctuate(timestamp: Long): Unit = {
    logger.debug("punctuate")
    import scala.math.Ordered.orderingToOrdered

    val beginningOfTheApp =
      LocalDate.of(2025, 1, 31).atStartOfDay(ZoneId.systemDefault()).toInstant()
    val players = playerStore.all()
    try {
      while (players.hasNext) {

        val keyValue = players.next()
        val p = keyValue.value
        if (p.wagering.isDefined) {
          if (
            !p.processed && p.reg_datetime.isDefined && p.reg_datetime.get
              .isAfter(beginningOfTheApp)
          ) {
            val firstBet = p.wagering.get
            val mp = firstBet.result_id match {
              case Some("0") => "LeoVegas_first_winning_bet"
              case _         => "LeoVegas_first_losing_bet"
            }
            val body =
              s"""
               |{
               |   "type":"GENERIC_USER",
               |   "mappingSelector":"${mp}",
               |   "contactId":"${p.player_id.get}",
               |   "properties":{
               |      "bet_id":"${firstBet.bet_id.getOrElse("")}",
               |      "bet_amount":"${firstBet.ggr_amount.getOrElse("")}",
               |      "result":"${firstBet.result_id.getOrElse("")}",
               |      "vertical":"${firstBet.vertical_id.getOrElse("")}"
               |   }
               |}
               |""".stripMargin

            sendMessageToSQS(p, body)
            playerStore.put(
              keyValue.key,
              keyValue.value
                .copy(
                  processed = true
                )
            )

          }

        } else {
          val firstBet = p.betsAndResults
            .filter(x => x.betDatetime.isDefined)
            .sortWith((x, y) => x.betDatetime.get < y.betDatetime.get)
            .headOption

          if (
            firstBet.isDefined &&
            p.reg_datetime.isDefined &&
            p.player_id.isDefined &&
            p.licenseuid.isDefined &&
            p.operator.isDefined &&
            (p.country_id.isDefined || p.country.isDefined) &&
            !p.processed &&
            firstBet.get.betDatetime.isDefined && firstBet.get.resultDatetime.isDefined && firstBet.get.hasResulted &&
            firstBet.get.resultId.isDefined && (firstBet.get.resultId.get == "1" || firstBet.get.resultId.get == "0") &&
            p.is_self_excluded.getOrElse("false") == "false" &&
            firstBet.get.vertical_id.isDefined && firstBet.get.vertical_id.get == "2" &&
            firstBet.get.resultDatetime.get.toEpochMilli > p.reg_datetime.get.toEpochMilli &&
            Duration
              .between(
                p.reg_datetime.get,
                firstBet.get.resultDatetime.get
              )
              .toMillis
              .abs <= Duration.ofDays(2).toMillis
          ) {

            val mp = firstBet.get.resultId match {
              case Some("0") => "LeoVegas_first_winning_bet"
              case _         => "LeoVegas_first_losing_bet"
            }
            val body =
              s"""
               |{
               |   "type":"GENERIC_USER",
               |   "mappingSelector":"${mp}",
               |   "contactId":"${p.player_id.get}",
               |   "properties":{
               |      "bet_id":"${firstBet.get.betId.getOrElse("")}",
               |      "bet_amount":"${firstBet.get.ggr_amount.getOrElse("")}",
               |      "result":"${firstBet.get.resultId.getOrElse("")}",
               |      "vertical":"${firstBet.get.vertical_id.getOrElse("")}"
               |   }
               |}
               |""".stripMargin

            sendMessageToSQS(p, body)
            playerStore.put(
              keyValue.key,
              keyValue.value
                .copy(
                  processed = true /*,
                betsAndResults = List.empty[BetAndResult] while debugging*/
                )
            )
          }

          // clean store if not already cleaned
          if (!p.cleaned && !p.processed) {
            val firstBetDateTime: Option[Long] = keyValue.value.betsAndResults
              .filter(_.betDatetime.isDefined)
              .map(_.betDatetime.get.toEpochMilli)
              .minOption
            val firstResultDateTime: Option[Long] =
              keyValue.value.betsAndResults
                .filter(_.resultDatetime.isDefined)
                .map(_.resultDatetime.get.toEpochMilli)
                .minOption

            if (
              p.reg_datetime.isDefined && (p.reg_datetime.get < Instant
                .ofEpochMilli(timestamp)) &&
              Duration
                .between(Instant.ofEpochMilli(timestamp), p.reg_datetime.get)
                .toMillis
                .abs > Duration.ofDays(2).toMillis
            ) {
              playerStore.put(
                keyValue.key,
                keyValue.value
                  .copy(
                    betsAndResults = List.empty[BetAndResult],
                    cleaned = true
                  )
              )
            } else if (
              firstBetDateTime.isDefined &&
              Instant
                .ofEpochMilli(firstBetDateTime.get) < Instant.ofEpochMilli(
                timestamp
              ) &&
              Duration
                .between(
                  Instant.ofEpochMilli(timestamp),
                  Instant.ofEpochMilli(firstBetDateTime.get)
                )
                .toMillis
                .abs > Duration.ofDays(2).toMillis
            ) {
              playerStore.put(
                keyValue.key,
                keyValue.value
                  .copy(
                    betsAndResults = List.empty[BetAndResult],
                    cleaned = true
                  )
              )
            } else if (
              firstResultDateTime.isDefined &&
              Instant.ofEpochMilli(firstResultDateTime.get) < Instant
                .ofEpochMilli(
                  timestamp
                ) &&
              Duration
                .between(
                  Instant.ofEpochMilli(timestamp),
                  Instant.ofEpochMilli(firstResultDateTime.get)
                )
                .toMillis
                .abs > Duration.ofDays(2).toMillis
            ) {
              playerStore.put(
                keyValue.key,
                keyValue.value
                  .copy(
                    betsAndResults = List.empty[BetAndResult],
                    cleaned = true
                  )
              )
            }
          }
        }
      }
    } finally {
      players.close()
    }
  }

  val once = new AtomicBoolean(true)
  def clearLicenseuid() = {
    if (once.compareAndSet(true, false)) {
      val players = playerStore.all()
      try {
        while (players.hasNext) {
          val keyValue = players.next()
          val p = keyValue.value
          if (p.licenseuid == Some("22")) {
            playerStore.put(
              keyValue.key,
              keyValue.value
                .copy(licenseuid = None)
            )
          }
        }
      } finally {
        players.close()
      }
    }

  }

  def sendMessageToSQS(
      ps: PlayerStore,
      body: String
  ): Unit = {
    val queue = getSQS(ps)
    val playerId = ps.player_id.get
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
          logger.infov(
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

  override def close(): Unit = {}
}
