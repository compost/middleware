package com.jada.processor

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonFactoryBuilder
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
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
import com.jada.models._
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

class MappingTransformer(
    config: ApplicationConfiguration,
    client: software.amazon.awssdk.services.sqs.SqsClient,
    clientNorth: software.amazon.awssdk.services.sqs.SqsClient,
    playerStoreName: String
) extends Transformer[String, Array[Byte], KeyValue[Unit, Unit]]
    with Punctuator {

  import MappingTransformer._
  val sender = new Sender(config, client, clientNorth)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  val bootstrap = new AtomicBoolean(true)
  val updateCountryCurrency = new AtomicBoolean(true)

  val GenericUser = "GENERIC_USER"

  val ResultIdWageringWin = "won"
  val WalletWithdraw = "withdraw_"
  val WalletDeposit = "deposit_"
  val FailedStatuses = Set(
    "rejected",
    "agentreject"
  )

  val SuccessStatuses = Set(
    "completed"
  )
  private final val logger =
    Logger.getLogger(classOf[MappingTransformer])
  private var processorContext: ProcessorContext = _
  private var playerStore: KeyValueStore[String, PlayerStore] = _

  override def init(processorContext: ProcessorContext): Unit = {
    logger.debug(f"${config}")
    this.processorContext = processorContext
    this.playerStore = processorContext.getStateStore(playerStoreName)
  }

  override def transform(key: String, v: Array[Byte]): KeyValue[Unit, Unit] = {
    if (key == null) { // They created and produced without respect
      return null
    }
    val playerFromStore = playerStore.get(key)

    val playerToSaveInStore = processorContext.topic() match {
      case config.topicHistoryPlayers =>
        val inputPlayer = deserialize[PlayerDatabase](v)
        val previous = Option(playerFromStore)
        PlayerStore(previous, inputPlayer)

      case config.topicPlayers =>
        val inputPlayer = deserialize[Player](v)

        val previous = Option(playerFromStore)
        val newPlayer = PlayerStore(previous, inputPlayer)

        if (playerFromStore == null) {
          val playerSQS = PlayerSQS(newPlayer)
          send(
            newPlayer.player_id.get,
            newPlayer.brand_id.get,
            GenericUser,
            computeMappingSelector(newPlayer.brand_id, "player_registration"),
            playerSQS
          )

        } else {
          val playerSQS =
            PlayerSQS(newPlayer)
          if (
            playerFromStore != null &&
            playerSQS != PlayerSQS(playerFromStore)
          ) {
            send(
              newPlayer.player_id.get,
              newPlayer.brand_id.get,
              GenericUser,
              computeMappingSelector(
                newPlayer.brand_id,
                "player_update"
              ),
              playerSQS
            )
          }

        }
        if (
          (previous.isEmpty || previous.get.first_dep_datetime.isEmpty) && newPlayer.first_dep_datetime.isDefined
        ) {
          val message =
            s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector":"${computeMappingSelector(
                newPlayer.brand_id,
                "first_deposit"
              )}",
               |"contactId": "${newPlayer.player_id.get}",
               |"properties": {
               | "first_dep_datetime": "${newPlayer.first_dep_datetime
                .map(d =>
                  if (d.length() > 10) { d.substring(0, 10) }
                  else { d }
                )
                .get}"
               |}
               |}
               |""".stripMargin

          sender.sendMessageToSQS(
            newPlayer.player_id.get,
            newPlayer.brand_id.get,
            message
          )
        }
        newPlayer

      case config.topicPlayerConsent =>
        val userConsentUpdate = deserialize[PlayerConsent](v)
        if (
          userConsentUpdate.channel.isDefined && userConsentUpdate.consented.isDefined && userConsentUpdate.player_id.isDefined && userConsentUpdate.brand_id.isDefined
        ) {
          val message = s"""
               |{
               |"type": "USER_CONSENT_UPDATE",
               |"mappingSelector": "${computeMappingSelector(
                            userConsentUpdate.brand_id,
                            "consent_change"
                          )}",
               |"contactId": "${userConsentUpdate.player_id.get}",
               |"channel": "${userConsentUpdate.channel.get.toUpperCase()}",
               |"consented": ${userConsentUpdate.consented.get.toLowerCase()}
               |}
               |""".stripMargin

          sender.sendMessageToSQS(
            userConsentUpdate.player_id.get,
            userConsentUpdate.brand_id.get,
            message
          )
        }

        null
      case config.topicLogins =>
        val login = deserialize[Login](v)
        val loginSQS = LoginSQS(login)
        send(
          login.player_id.get,
          login.brand_id.get,
          GenericUser,
          computeMappingSelector(login.brand_id, "player_login"),
          loginSQS
        )
        null
      case config.topicPlayerStatus =>
        val ps = deserialize[PlayerStatus](v)

        if (ps.brand_id.isDefined && ps.player_id.isDefined) {
          ps.blocked_end_date match {
            case None => sendPlayerStatus("player_blocked", ps, Option(true))
            case Some(d) if d.isBlank() =>
              sendPlayerStatus("player_blocked", ps, Option(true))
            case Some(d) if !d.isBlank() =>
              sendPlayerStatus("player_self_exclude", ps, None)
            case _ => logger.debug("shoud never happen")
          }
        }
        null

      case config.topicWallets =>
        val wallet = deserialize[Wallet](v)

        if (
          wallet.transaction_type_id
            .getOrElse(
              ""
            )
            .toLowerCase()
            .startsWith(WalletDeposit) &&
          FailedStatuses.contains(
            wallet.transaction_status_id
              .getOrElse("")
              .toLowerCase()
          )
        ) {
          val sqs = FailedDepositSQS(wallet)
          send(
            wallet.player_id.get,
            wallet.brand_id.get,
            GenericUser,
            computeMappingSelector(wallet.brand_id, "failed_deposit"),
            sqs
          )
        }

        if (
          wallet.transaction_type_id
            .getOrElse(
              ""
            )
            .toLowerCase
            .startsWith(WalletWithdraw) &&
          FailedStatuses.contains(
            wallet.transaction_status_id
              .getOrElse("")
              .toLowerCase()
          )
        ) {
          val sqs = FailedWithdrawalSQS(wallet)
          send(
            wallet.player_id.get,
            wallet.brand_id.get,
            GenericUser,
            computeMappingSelector(wallet.brand_id, "failed_withdrawal"),
            sqs
          )
        }

        if (
          wallet.transaction_type_id
            .getOrElse(
              ""
            )
            .toLowerCase
            .startsWith(WalletWithdraw) &&
          SuccessStatuses.contains(
            wallet.transaction_status_id
              .getOrElse("")
              .toLowerCase()
          )
        ) {
          val sqs = SuccessWithdrawalSQS(wallet)
          send(
            wallet.player_id.get,
            wallet.brand_id.get,
            GenericUser,
            computeMappingSelector(wallet.brand_id, "success_withdrawal"),
            sqs
          )
        }

        if (
          wallet.transaction_type_id
            .getOrElse(
              ""
            )
            .toLowerCase()
            .startsWith(WalletDeposit) &&
          SuccessStatuses.contains(
            wallet.transaction_status_id
              .getOrElse("")
              .toLowerCase()
          )
        ) {
          val sqs = SuccessDepositSQS(wallet)
          send(
            wallet.player_id.get,
            wallet.brand_id.get,
            GenericUser,
            computeMappingSelector(wallet.brand_id, "success_deposit"),
            sqs
          )
        }

        null

      case config.topicWagerings =>
        val wagering = deserialize[Wagering](v)
        if (
          wagering.result_id
            .getOrElse("")
            .toLowerCase() == ResultIdWageringWin &&
          wagering.has_resulted
            .map(_.toLowerCase())
            .map { v => toBoolean(v) }
            .getOrElse(false) &&
          wagering.win_amount.getOrElse("0").toFloat >= 1000
        ) {
          // big_win
          val bigWinMessage =
            s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector":"${computeMappingSelector(
                wagering.brand_id,
                "big_win"
              )}",
               |"contactId": "${wagering.player_id.get}",
               |"properties": {
               | "big_win": "${wagering.win_amount.get}"
               |}
               |}
               |""".stripMargin
          sender.sendMessageToSQS(
            wagering.player_id.get,
            wagering.brand_id.get,
            bigWinMessage
          )
        }
        null
    }

    if (playerToSaveInStore != null) {
      playerStore.put(key, playerToSaveInStore)

    }

    null
  }

  def deserialize[T: Decoder](data: Array[Byte]): T = {
    parser.decode[T](new String(data)).right.get
  }

  def sendPlayerStatus(
      mp: String,
      ps: PlayerStatus,
      blocked: Option[Boolean]
  ) = {
    val psBody = PlayerStatusBody(
      ps,
      computeMappingSelector(ps.brand_id, mp),
      blocked
    )
    sender.sendMessageToSQS(
      ps.player_id.get,
      ps.brand_id.get,
      printer.print(
        PlayerStatusBody.playerStatusBodyEncoder.apply(psBody)
      )
    )
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
      mappingSelector,
      value
    )
    sender.sendMessageToSQS(
      playerId,
      brandId,
      printer.print(Body.bodyEncoder[T].apply(body))
    )
  }

  override def punctuate(timestamp: Long): Unit = {
    logger.debug("punctuate")

    if (config.bootstrapPlayers) {
      if (bootstrap.compareAndSet(true, false)) {
        val players = playerStore.all()
        try {
          val now = UUID.randomUUID.toString
          logger.debug(s"start bootstrap players ${now}")
          val brandFile = writeFile(now, players)
          brandFile.foreach {
            case (key, value) => {
              logger.debug(s"expose ${key}")
              expose(key, now, value)
            }
          }
          logger.debug(s"end bootstrap players ${now}")
        } finally {
          players.close()
        }
      }
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

          val p = PlayerSQS(keyAndValue.value)
          filewrite._2.write(
            s"""{"id":"${keyAndValue.value.player_id.get}", "properties":${printer
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
    val bsclient = new BlobServiceClientBuilder()
      .connectionString(config.connectionString)
      .buildClient()
    val blobClient = bsclient
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
         |  "mappingSelector":"${computeMappingSelector(
          Option(brandId),
          "batch_update"
        )}",
         |  "downloadUri": "$url"
         |}
         |""".stripMargin

    logger.debug(body)
    sender.sendMessageToSQS("batch-import", brandId, body)
  }

  def computeMappingSelector(brandId: Option[String], mp: String): String = {
    if (brandId.exists(v => config.mappingSelectorDevPrefix.contains(v))) {
      s"DEV_${mp}"
    } else {
      mp
    }
  }
  override def close(): Unit = {}
}

object MappingTransformer {

  val DateTimeformatter = DateTimeFormatter
    .ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    .withZone(ZoneId.systemDefault())

  val DateTimeformatterWithoutT = DateTimeFormatter
    .ofPattern("yyyy-MM-dd' 'HH:mm:ss")
    .withZone(ZoneId.systemDefault())

  def parseDate(date: String): Instant = {
    if (date.length >= 19) {
      parseDateWithFallback(
        date.substring(0, 19),
        DateTimeformatter,
        DateTimeformatterWithoutT
      )
    } else {
      Instant.from(
        DateTimeformatter.parse(s"${date.substring(0, 10)}T00:00:00")
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
