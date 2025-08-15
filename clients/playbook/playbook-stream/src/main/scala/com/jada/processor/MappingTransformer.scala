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
import com.jada.models.BlockedBody
import com.jada.models.PlayerDB
import com.jada.models.PlayerStatusDB
import com.jada.models.StakeFactor

class MappingTransformer(
    config: ApplicationConfiguration,
    client: software.amazon.awssdk.services.sqs.SqsClient,
    clientNorth: software.amazon.awssdk.services.sqs.SqsClient,
    playerStoreName: String,
    currencyStoreName: String,
    countryStoreName: String,
    ci1359StoreName: String
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
  val UserBlockedToggle = "USER_BLOCKED_TOGGLE"

  val ResultIdWageringWin = "won"
  val WalletWithdraw = "withdrawal"
  val WalletDeposit = "deposit"
  val FailedStatuses = Set(
    "rejected",
    "error",
    "cancelled",
    "voided",
    "failed",
    "cancel",
    "decline"
  )

  private final val logger =
    Logger.getLogger(classOf[MappingTransformer])
  private var processorContext: ProcessorContext = _
  private var playerStore: KeyValueStore[String, PlayerStore] = _
  private var currencyStore: KeyValueStore[String, Currency] = _
  private var countryStore: KeyValueStore[String, Country] = _
  private var blockedReasonStore: KeyValueStore[String, CI1359BlockedReason] = _

  override def init(processorContext: ProcessorContext): Unit = {
    logger.debug(f"${config}")
    this.processorContext = processorContext
    this.playerStore = processorContext.getStateStore(playerStoreName)
    this.currencyStore = processorContext.getStateStore(currencyStoreName)
    this.countryStore = processorContext.getStateStore(countryStoreName)
    this.blockedReasonStore = processorContext.getStateStore(ci1359StoreName)

    // this.processorContext.schedule(
    //  config.punctuator,
    //  PunctuationType.WALL_CLOCK_TIME,
    //  this
    // )
  }

  override def transform(key: String, v: Array[Byte]): KeyValue[Unit, Unit] = {
    val playerFromStore = playerStore.get(key)

    val thirtyDaysAgo = Instant.now.minus(30, ChronoUnit.DAYS)

    val playerToSaveInStore = processorContext.topic() match {
      case config.topicCI1359BlockedReason => {
        val input = deserialize[CI1359BlockedReason](v)
        if (
          playerFromStore != null && playerFromStore.status == Some("blocked")
        ) {
          blockedReasonStore.put(key, input)
        }
        null
      }
      case config.topicRaw => {
        val input = deserialize[PlayerDB](v)
        val newPlayer = PlayerStore(Option(playerFromStore), input)
        playerStore.put(key, newPlayer)
        null
      }
      case config.topicPlayerStatusRepartitioned => {
        val input = deserialize[PlayerStatus](v)
        processPlayerStatus(Option(playerFromStore), input).orNull
      }
      case config.topicStakeFactorRepartitioned => {
        val input = deserialize[StakeFactor](v)
        processStakeFactor(Option(playerFromStore), input)
      }
      case config.topicPlayerConsentMultiRepartitioned => {
        val input = deserialize[PlayerConsentMulti](v)
        processPlayerConsentMulti(Option(playerFromStore), input)
      }
      case config.topicPlayersRepartitioned =>
        val inputPlayer = deserialize[Player](v)

        val newPlayer = PlayerStore(Option(playerFromStore), inputPlayer)

        val pp =
          if (
            (playerFromStore == null || (playerFromStore != null && playerFromStore.reg_datetime.isEmpty)) && inputPlayer.reg_datetime.isDefined &&
            parseDate(inputPlayer.reg_datetime.get)
              .isAfter(Instant.now().minusSeconds(86400))
          ) {

            val player =
              (newPlayer.brand_id, newPlayer.is_deposited_restricted) match {
                case (Some("238" | "239"), None) =>
                  newPlayer.copy(is_deposited_restricted = Some("false"))
                case _ => newPlayer
              }

            val playerSQS = PlayerSQS(player, currencyStore, countryStore)
            send(
              newPlayer.player_id.get,
              newPlayer.brand_id.get,
              GenericUser,
              computeMappingSelector(newPlayer.brand_id, "player_registration"),
              playerSQS
            )

            newPlayer.sms_consented.foreach(v =>
              sendConsent(
                newPlayer.brand_id,
                newPlayer.player_id,
                Some("SMS"),
                v
              )
            )
            newPlayer.email_consented.foreach(v =>
              sendConsent(
                newPlayer.brand_id,
                newPlayer.player_id,
                Some("EMAIL"),
                v
              )
            )
            player
          } else {
            newPlayer
          }
        pp

      case config.topicUserConsentUpdatePlaybookRepartitioned =>
        val userConsentUpdate = deserialize[UserConsentUpdate](v)

        if (
          userConsentUpdate.player_id.isDefined &&
          userConsentUpdate.brand_id.isDefined &&
          userConsentUpdate.channel.isDefined && userConsentUpdate.consented.isDefined &&
          userConsentUpdate.`type`.isDefined && userConsentUpdate.`type`.get
            .toUpperCase() == "USER_CONSENT_UPDATE"
        ) {
          logger.debug(s"user_consent_update ${userConsentUpdate}")

          val newPlayer =
            PlayerStore(Option(playerFromStore), userConsentUpdate)

          if (
            ((playerFromStore != null && playerFromStore.players_data_received
              .contains(
                "true"
              )) && (playerFromStore.sms_consented != newPlayer.sms_consented ||
              playerFromStore.email_consented != newPlayer.email_consented)) &&
            userConsentUpdate.channel.getOrElse("").toLowerCase() != "phone"
          ) {
            sendConsent(
              userConsentUpdate.brand_id,
              userConsentUpdate.player_id,
              userConsentUpdate.channel,
              userConsentUpdate.consented.get
            )
          }
          newPlayer
        } else {
          null
        }
      case config.topicLoginsRepartitioned =>
        val login = deserialize[Login](v)

        val loginDTAsDate = login.login_datetime.map(x => parseDate(x))

        if (
          loginDTAsDate.isDefined && loginDTAsDate.get.isAfter(thirtyDaysAgo)
        ) {
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
            computeMappingSelector(login.brand_id, "player_login"),
            loginSQS
          )
          newPlayerWithLogin
        } else { null }

      case config.topicWalletsRepartitioned =>
        val wallet = deserialize[Wallet](v)
        val newActivityDate = wallet.transaction_datetime.map(x => parseDate(x))

        if (
          newActivityDate.isDefined && newActivityDate.get.isAfter(
            thirtyDaysAgo
          )
        ) {
          val newPlayerWithWallet = Option(playerFromStore) match {
            case Some(player) =>
              player.copy(
                player_id = wallet.player_id.orElse(player.player_id),
                wallet_transaction_type_id =
                  wallet.transaction_type_id.orElse(wallet.transaction_type_id),
                wallet_transaction_status_id = wallet.transaction_status_id
                  .orElse(wallet.transaction_status_id),
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
            FailedStatuses.contains(
              newPlayerWithWallet.wallet_transaction_status_id
                .getOrElse("")
                .toLowerCase()
            )
          ) {
            // failed_deposit
            val failedDepositMessage =
              s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector":"${computeMappingSelector(
                  wallet.brand_id,
                  "failed_deposit"
                )}",
               |"contactId": "${wallet.player_id.get}",
               |"properties": {
               | "failed_deposit_date": "${wallet.transaction_datetime
                  .map(d => d.substring(0, 10))
                  .get}"
               |}
               |}
               |""".stripMargin
            sender.sendMessageToSQS(
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
            FailedStatuses.contains(
              newPlayerWithWallet.wallet_transaction_status_id
                .getOrElse("")
                .toLowerCase()
            )
          ) {
            // failed_withdrawal
            val failedWithdrawalMessage =
              s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector":"${computeMappingSelector(
                  wallet.brand_id,
                  "failed_withdrawal"
                )}",
               |"contactId": "${wallet.player_id.get}",
               |"properties": {
               | "failed_withdrawal_date": "${wallet.transaction_datetime
                  .map(d => d.substring(0, 10))
                  .get}"
               |}
               |}
               |""".stripMargin
            sender.sendMessageToSQS(
              wallet.player_id.get,
              wallet.brand_id.get,
              failedWithdrawalMessage
            )
          }

          newPlayerWithWallet
        } else { null }

      case config.topicWageringsRepartitioned =>
        val wagering = deserialize[Wagering](v)
        val newActivityDate =
          wagering.bet_datetime.map(x => parseDate(x))

        if (
          newActivityDate.isDefined && newActivityDate.get.isAfter(
            thirtyDaysAgo
          )
        ) {
          val newPlayerWithWagering = Option(playerFromStore) match {
            case Some(player) =>
              player.copy(
                player_id = wagering.player_id.orElse(player.player_id),
                wagering_transaction_type_id = wagering.transaction_type_id
                  .orElse(player.wagering_transaction_type_id),
                wagering_ggr_amount =
                  wagering.ggr_amount.orElse(player.wagering_ggr_amount),
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
                wagering_has_resulted = wagering.has_resulted,
                last_activity_date =
                  Some(newActivityDate.getOrElse(Instant.now())),
                brand_id = wagering.brand_id
              )
          }

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
          newPlayerWithWagering
        } else { null }

    }

    if (playerToSaveInStore != null) {
      playerStore.put(key, playerToSaveInStore)

      val playerSQS =
        PlayerSQS(playerToSaveInStore, currencyStore, countryStore)

      if (
        (playerFromStore != null && playerFromStore.players_data_received
          .contains("true")) &&
        playerSQS != PlayerSQS(playerFromStore, currencyStore, countryStore)
      ) {
        send(
          playerToSaveInStore.player_id.get,
          playerToSaveInStore.brand_id.get,
          GenericUser,
          computeMappingSelector(playerToSaveInStore.brand_id, "player_update"),
          playerSQS
        )
      }

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
               |"mappingSelector": "${computeMappingSelector(
                      brandId,
                      "consent_change"
                    )}",
               |"contactId": "${playerId.get}",
               |"channel": "${channel.get.toUpperCase()}",
               |"consented": ${consented.toLowerCase()}
               |}
               |""".stripMargin

    sender.sendMessageToSQS(
      playerId.get,
      brandId.get,
      message
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

  def processPlayerConsentMulti(
      stored: Option[PlayerStore],
      ps: PlayerConsentMulti
  ): PlayerStore = {
    val mp = computeMappingSelector(ps.brand_id, "consent_change_multi")
    val playerConsentMultiSQSs = PlayerConsentMultiSQS(ps, mp)
    playerConsentMultiSQSs.foreach(v => {
      sender.sendMessageToSQS(
        v.contactId,
        v.properties.brand_id.get,
        printer.print(PlayerConsentMultiSQS.encoder.apply(v))
      )
    })
    stored match {
      case Some(previous) =>
        previous.copy(
          player_consent_multi = Option(ps)
        )
      case None =>
        PlayerStore(
          player_id = ps.player_id,
          brand_id = ps.brand_id,
          player_consent_multi = Option(ps)
        )
    }

  }
  def processStakeFactor(
      stored: Option[PlayerStore],
      ps: StakeFactor
  ): PlayerStore = {

    val mp = computeMappingSelector(ps.brand_id, "stake_factor")
    val body = StakeFactorBody(ps, mp)
    sender.sendMessageToSQS(
      ps.player_id.get,
      ps.brand_id.get,
      printer.print(StakeFactorBody.stakeFactorBodyEncoder.apply(body))
    )
    stored match {
      case Some(previous) =>
        previous.copy(
          sf_inplay = ps.sf_inplay.orElse(previous.sf_inplay),
          sf_inplay_casino =
            ps.sf_inplay_casino.orElse(previous.sf_inplay_casino),
          sf_inplay_livecasino =
            ps.sf_inplay_livecasino.orElse(previous.sf_inplay_livecasino),
          sf_inplay_lottery =
            ps.sf_inplay_lottery.orElse(previous.sf_inplay_lottery),
          sf_inplay_virtualsports =
            ps.sf_inplay_virtualsports.orElse(previous.sf_inplay_virtualsports),
          sf_prematch = ps.sf_prematch.orElse(previous.sf_prematch),
          sf_prematch_casino =
            ps.sf_prematch_casino.orElse(previous.sf_prematch_casino),
          sf_prematch_livecasino =
            ps.sf_prematch_livecasino.orElse(previous.sf_prematch_livecasino),
          sf_prematch_lottery =
            ps.sf_prematch_lottery.orElse(previous.sf_prematch_lottery),
          sf_prematch_virtualsports = ps.sf_prematch_virtualsports.orElse(
            previous.sf_prematch_virtualsports
          )
        )
      case None =>
        PlayerStore(
          player_id = ps.player_id,
          brand_id = ps.brand_id,
          sf_inplay = ps.sf_inplay,
          sf_inplay_casino = ps.sf_inplay_casino,
          sf_inplay_livecasino = ps.sf_inplay_livecasino,
          sf_inplay_lottery = ps.sf_inplay_lottery,
          sf_inplay_virtualsports = ps.sf_inplay_virtualsports,
          sf_prematch = ps.sf_prematch,
          sf_prematch_casino = ps.sf_prematch_casino,
          sf_prematch_livecasino = ps.sf_prematch_livecasino,
          sf_prematch_lottery = ps.sf_prematch_lottery,
          sf_prematch_virtualsports = ps.sf_prematch_virtualsports
        )
    }
  }

  def processPlayerStatus(
      stored: Option[PlayerStore],
      ps: PlayerStatus
  ): Option[PlayerStore] = {

    ps match {
      case PlayerStatus(
            Some(player_id),
            Some(brand_id),
            Some("1"),
            _,
            _,
            _
          ) => {
        stored match {
          case Some(player) => // if true || !player.status.contains("1") =>
            val body = BlockedBody(
              ps,
              false,
              computeMappingSelector(ps.brand_id, "player_blocked")
            )
            sender.sendMessageToSQS(
              ps.player_id.get,
              ps.brand_id.get,
              printer.print(BlockedBody.blockedBodyEncoder.apply(body))
            )
            Some(player.copy(status = Some("1")))
          case None =>
            val body = BlockedBody(
              ps,
              false,
              computeMappingSelector(ps.brand_id, "player_blocked")
            )
            sender.sendMessageToSQS(
              ps.player_id.get,
              ps.brand_id.get,
              printer.print(BlockedBody.blockedBodyEncoder.apply(body))
            )
            Some(
              PlayerStore(
                player_id = Some(player_id),
                brand_id = Some(brand_id),
                status = Some("1")
              )
            )
          case _ => stored
        }

      }

      case PlayerStatus(
            Some(player_id),
            Some(brand_id),
            Some("2" | "3" | "4" | "5"),
            _,
            _,
            _
          ) => {
        stored match {
          case Some(
                player
              ) => // if true || !player.status.contains("blocked") =>
            val body = BlockedBody(
              ps,
              true,
              computeMappingSelector(ps.brand_id, "player_blocked")
            )
            sender.sendMessageToSQS(
              ps.player_id.get,
              ps.brand_id.get,
              printer.print(BlockedBody.blockedBodyEncoder.apply(body))
            )

            Some(player.copy(status = Some("blocked")))
          case None =>
            val body = BlockedBody(
              ps,
              true,
              computeMappingSelector(ps.brand_id, "player_blocked")
            )
            sender.sendMessageToSQS(
              ps.player_id.get,
              ps.brand_id.get,
              printer.print(BlockedBody.blockedBodyEncoder.apply(body))
            )

            Some(
              PlayerStore(
                player_id = Some(player_id),
                brand_id = Some(brand_id),
                status = Some("blocked")
              )
            )
          case _ => stored
        }
      }
      case _ => {
        logger.debug(s"processPlayerStatus ${ps} ${stored}")
        stored
      }

    }
  }

  override def punctuate(timestamp: Long): Unit = {
    logger.debug("punctuate")

    val players = blockedReasonStore.all()
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
      if (brandFile.isEmpty) {
        logger.info(s"punctuate done ${now}")
      }
      logger.debug(s"end bootstrap players ${now}")
    } finally {
      players.close()
    }
    cleanStore()
  }

  def cleanStore(): Unit = {
    val playersBalance = blockedReasonStore.all
    try {
      while (playersBalance.hasNext) {
        blockedReasonStore.delete(playersBalance.next().key)
      }
    } finally {
      playersBalance.close()
    }
  }

  def writeFile(
      now: String,
      players: KeyValueIterator[String, CI1359BlockedReason]
  ): Map[String, Path] = {

    import io.circe._, io.circe.generic.auto._, io.circe.parser._,
    io.circe.syntax._
    val brandFile = scala.collection.mutable.Map[String, (Path, FileWriter)]()

    try {

      while (players.hasNext()) {
        val keyAndValue = players.next()
        if (keyAndValue.value.BRAND_ID.isDefined) {
          val filewrite = brandFile.getOrElseUpdate(
            keyAndValue.value.BRAND_ID.get.toString, {
              val tmp = Files.createTempFile(
                s"players-${now}-${keyAndValue.value.BRAND_ID}",
                ".json"
              )
              logger.debug(s"file: ${tmp}")
              val writer = new FileWriter(tmp.toFile())
              (tmp, writer)
            }
          )

          filewrite._2.write(
            s"""{"id":"${keyAndValue.value.PLAYER_ID.get}", "properties":{"player_blocked_reason":"${keyAndValue.value.PLAYER_BLOCKED_REASON_DESCRIPTION.get}"}}\n"""
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
          "batch-import"
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
