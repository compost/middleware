package com.jada.processor

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonFactoryBuilder
import com.fasterxml.jackson.databind.ObjectMapper
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
import com.jada.models.{
  Action,
  Body,
  Login,
  LoginSQS,
  Player,
  PlayerSQS,
  PlayerStatus,
  PlayerStatusBody,
  PlayerStore,
  UserConsentUpdate,
  Wagering,
  Wallet
}
import com.jada.Referentials
import io.circe.Printer

import java.util.UUID
import io.circe.Encoder
import io.circe.Decoder
import com.jada.serdes.CirceSerdes
import io.circe._
import io.circe.generic.auto._

import java.time.temporal.{ChronoUnit, TemporalUnit}
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
    referentials: com.jada.Referentials,
    client: software.amazon.awssdk.services.sqs.SqsClient,
    startStopStoreName: String,
    playerStoreName: String
) extends Transformer[String, Array[Byte], KeyValue[Unit, Unit]]
    with Punctuator {

  import MappingTransformer._
  val sender = new Sender
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  val bootstrap = new AtomicBoolean(true)
  val updateCountryCurrency = new AtomicBoolean(true)

  val GenericUser = "GENERIC_USER"
  val UserBlockedToggle = "USER_BLOCKED_TOGGLE"

  val WageringWin = "1"
  val WalletWithdraw = "withdraw"
  val WalletDeposits = Set(
    "lucky_egg",
    "deposit",
    "day7pack_deposit",
    "three_deposit",
    // new
    "three_deposit_1",
    "three_deposit_2",
    "three_deposit_3",
    "safe_box_deposit",
    "day7pack_deposit_1",
    "day7pack_deposit_2",
    "day7pack_deposit_3",
    "day7pack_deposit_4",
    "day7pack_deposit_5",
    "day7pack_deposit_6",
    "miss_deposit",
    "lucky_egg_1",
    "lucky_egg_2",
    "starter_deposit",
    "promotion_deposit",
    "shop_charge",
    "promotion_deposit",
    "unlock_deposit_2",
    "balance_not_enough_1",
    "balance_not_enough_2",
    "vote_charge_1",
    "vote_charge_2",
    "repeat_charge",
    "double_reward",
    "double_room",
    "lucky_bonus",
    "activity_charge",
    "progression_recharge_1",
    "progression_recharge_2",
    "progression_recharge_3",
    "progression_recharge_4",
    "progression_recharge_5",
    "progression_recharge_6",
    "progression_recharge_7",
    "new_unlock"
  )
  val FailedWallets =
    Set("rejected", "error", "cancelled", "voided", "cancel", "failed")

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

  override def transform(key: String, v: Array[Byte]): KeyValue[Unit, Unit] = {
    val playerFromStore = playerStore.get(key)

    val thirtyDaysAgo = Instant.now.minus(30, ChronoUnit.DAYS)

    val playerToSaveInStore = processorContext.topic() match {

      case config.topicPlayerStatusRepartitioned => {
        val input = deserialize[PlayerStatus](v)
        // processPlayerStatus(input) we need to know first what is being sent as value so we can filter
        null
      }
      case config.topicPlayersRepartitioned =>
        val inputPlayer = deserialize[Player](v)

        val newPlayer = Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              player_id = inputPlayer.player_id,
              brand_id = inputPlayer.brand_id.orElse(player.brand_id),
              test_user = inputPlayer.test_user.orElse(player.test_user),
              timezone = inputPlayer.timezone.orElse(player.timezone),
              reg_datetime =
                inputPlayer.reg_datetime.orElse(player.reg_datetime),
              first_name = inputPlayer.first_name.orElse(player.first_name),
              last_name = inputPlayer.last_name.orElse(player.last_name),
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
              dob = inputPlayer.dob
                .orElse(player.dob)
                .map(d => d.substring(0, 10)),
              vip = inputPlayer.vip.orElse(player.vip),
              currency_id = inputPlayer.currency_id.orElse(player.currency_id),
              country_id = inputPlayer.country_id.orElse(player.country_id),
              platform = inputPlayer.platform.orElse(player.platform)
            )

          case None =>
            // partially register non null infos
            PlayerStore(
              player_id = inputPlayer.player_id,
              brand_id = inputPlayer.brand_id,
              reg_datetime = inputPlayer.reg_datetime,
              test_user = inputPlayer.test_user,
              timezone = inputPlayer.timezone,
              first_name = inputPlayer.first_name,
              last_name = inputPlayer.last_name,
              email = inputPlayer.email,
              phone_number = inputPlayer.phone_number,
              language = inputPlayer.language,
              affiliate_id = inputPlayer.affiliate_id,
              is_self_excluded = inputPlayer.is_self_excluded,
              first_dep_datetime = inputPlayer.first_dep_datetime,
              dob = inputPlayer.dob.map(d => d.substring(0, 10)),
              vip = inputPlayer.vip,
              currency_id = inputPlayer.currency_id,
              country_id = inputPlayer.country_id,
              platform = inputPlayer.platform
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
            computeMappingSelector(newPlayer.brand_id, "player_registration"),
            playerSQS
          )
        }

        newPlayer

      case config.topicUserConsentUpdateWinnerStudioRepartitioned =>
        val userConsentUpdate = deserialize[UserConsentUpdate](v)

        if (
          userConsentUpdate.player_id.isDefined &&
          userConsentUpdate.brand_id.isDefined &&
          userConsentUpdate.channel.isDefined && userConsentUpdate.consented.isDefined &&
          userConsentUpdate.`type`.isDefined && userConsentUpdate.`type`.get.toUpperCase == "USER_CONSENT_UPDATE"
        ) {
          logger.debug(s"user_consent_update ${userConsentUpdate}")

          val newPlayer = Option(playerFromStore) match {
            case Some(player) =>
              player.copy(
                email_consented = userConsented(userConsentUpdate, "EMAIL")
                  .orElse(player.email_consented),
                sms_consented = userConsented(userConsentUpdate, "SMS")
                  .orElse(player.sms_consented), // will not be provided
                app_push_consented =
                  userConsented(userConsentUpdate, "PERMISSION_PUSH")
                    .orElse(player.app_push_consented)
              )
            case None =>
              PlayerStore(
                player_id = userConsentUpdate.player_id,
                brand_id = userConsentUpdate.brand_id,
                email_consented = userConsented(userConsentUpdate, "EMAIL"),
                sms_consented = userConsented(userConsentUpdate, "SMS"),
                app_push_consented =
                  userConsented(userConsentUpdate, "PERMISSION_PUSH")
              )
          }

          if (
            playerFromStore == null ||
            playerFromStore.email_consented != newPlayer.email_consented ||
            playerFromStore.sms_consented != newPlayer.sms_consented ||
            playerFromStore.app_push_consented != newPlayer.app_push_consented
          ) {
            val consented =
              if (userConsentUpdate.consented.get == "0") "false" else "true"

            sendConsent(
              userConsentUpdate.brand_id,
              userConsentUpdate.player_id,
              userConsentUpdate.channel,
              consented
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
        } else null

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
            Wallet.TriggerOnTransactionTypeIds.contains(
              wallet.transaction_type_id
                .getOrElse("")
                .toLowerCase
            ) &&
            wallet.transaction_status_id
              .getOrElse("")
              .toLowerCase()
              .equals(Wallet.Successful)
          ) {
            val message =
              s"""
                 |{"type": "GENERIC_USER",
                 |"mappingSelector":"${computeMappingSelector(
                  wallet.brand_id,
                  wallet.transaction_type_id.get
                )}",
                 |"contactId": "${wallet.player_id.get}",
                 |"properties": {
                 | "last_transaction_amount": "${wallet.amount.getOrElse("")}",
                 | "brand_id": "${wallet.brand_id.getOrElse("")}"
                 |}
                 |}
                 |""".stripMargin
            sender.sendMessageToSQS(
              config,
              client,
              wallet.player_id.get,
              wallet.brand_id.get,
              message
            )

          }
          if (
            WalletDeposits.contains(
              newPlayerWithWallet.wallet_transaction_type_id
                .getOrElse("")
                .toLowerCase
            ) &&
            FailedWallets.contains(
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
              config,
              client,
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
              config,
              client,
              wallet.player_id.get,
              wallet.brand_id.get,
              failedWithdrawalMessage
            )
          }

          newPlayerWithWallet
        } else null

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
              .toLowerCase() == WageringWin &&
            wagering.has_resulted
              .map(_.toLowerCase())
              .map { v => toBoolean(v) }
              .getOrElse(false) &&
            wagering.ggr_amount.getOrElse("0").toFloat >= 1000
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
                 | "ggr_amount": "${wagering.ggr_amount.get}"
                 |}
                 |}
                 |""".stripMargin
            sender.sendMessageToSQS(
              config,
              client,
              wagering.player_id.get,
              wagering.brand_id.get,
              bigWinMessage
            )
          }
          newPlayerWithWagering
        } else null

    }

    if (playerToSaveInStore != null) {
      playerStore.put(key, playerToSaveInStore)

      val playerSQS = PlayerSQS(playerToSaveInStore, referentials)

      if (
        playerFromStore == null ||
        PlayerSQS(playerFromStore, referentials) != playerSQS
      ) {
        send(
          playerToSaveInStore.player_id.get,
          playerToSaveInStore.brand_id.get,
          GenericUser,
          computeMappingSelector(
            playerToSaveInStore.brand_id,
            "player_updated"
          ),
          playerSQS
        )
      }

    }

    if (
      (playerFromStore == null || (playerFromStore != null && playerFromStore.first_dep_datetime.isEmpty)) && playerToSaveInStore != null && playerToSaveInStore.first_dep_datetime.isDefined &&
      parseDate(playerToSaveInStore.first_dep_datetime.get)
        .isAfter(Instant.now().minusSeconds(86400))
    ) {
      val firstDepositMessage =
        s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector":"${computeMappingSelector(
            playerToSaveInStore.brand_id,
            "first_deposit"
          )}",
               |"contactId": "${playerToSaveInStore.player_id.get}",
               |"properties": {
               | "first_dep_datetime": "${playerToSaveInStore.first_dep_datetime
            .map(d => d.substring(0, 10))
            .get}"
               |}
               |}
               |""".stripMargin
      sender.sendMessageToSQS(
        config,
        client,
        playerToSaveInStore.player_id.get,
        playerToSaveInStore.brand_id.get,
        firstDepositMessage
      )
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
               |"mappingSelector": "${computeMappingSelector(
                      brandId,
                      "consent_change"
                    )}",
               |"contactId": "${playerId.get}",
               |"channel": "${channel.get.toUpperCase}",
               |"consented": ${consented}
               |}
               |""".stripMargin

    sender.sendMessageToSQS(
      config,
      client,
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
    sender.sendMessageToSQS(
      config,
      client,
      playerId,
      brandId,
      s"""{"type":"${UserBlockedToggle}","mappingSelector":"${computeMappingSelector(
          Option(brandId),
          "self_exclusion"
        )}","contactId":"${playerId}","blocked":$isBlocked}"""
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
    try {
      sender.sendMessageToSQS(
        config,
        client,
        playerId,
        brandId,
        printer.print(Body.bodyEncoder[T].apply(body))
      )
    } catch {
      case e: Exception =>
        logger.errorv(
          "unable send body",
          Array(
            kv("playerId", playerId),
            kv("brandId", brandId),
            kv("body", body),
            kv("error", e)
          ): _*
        )
        throw e
    }
  }

  def processPlayerStatus(ps: PlayerStatus): Option[PlayerStore] = {

    if (ps.player_id.isDefined && ps.brand_id.isDefined) {
      val body = PlayerStatusBody(
        ps,
        computeMappingSelector(ps.brand_id, "player_blocked")
      )
      sender.sendMessageToSQS(
        config,
        client,
        ps.player_id.get,
        ps.brand_id.get,
        printer.print(PlayerStatusBody.playerStatusBodyEncoder.apply(body))
      )
    }
    None
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
        if (
          keyAndValue.value.is_self_excluded
            .map(toBoolean(_))
            .getOrElse(false) == true
        ) {
          val isBlocked = true
          sendUserBlockedToggle(
            keyAndValue.value.player_id.get,
            keyAndValue.value.brand_id.get,
            isBlocked
          )
        }
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

          val p = PlayerSQS(keyAndValue.value, referentials)
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
    sender.sendMessageToSQS(config, client, "batch-import", brandId, body)
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

  val DateTimeformatter6DigitsZone = DateTimeFormatter
    .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSX")
    .withZone(ZoneId.systemDefault())

  val DateTimeformatter6DigitsZoneWithoutT = DateTimeFormatter
    .ofPattern("yyyy-MM-dd' 'HH:mm:ss.SSSSSSX")
    .withZone(ZoneId.systemDefault())

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
    if (date.length == 27) {
      parseDateWithFallback(
        date,
        DateTimeformatter6DigitsZone,
        DateTimeformatter6DigitsZoneWithoutT
      )
    } else if (date.length == 23) {
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

  def userConsented(
      userConsentUpdate: UserConsentUpdate,
      kind: String
  ): Option[String] = {
    userConsentUpdate.channel
      .filter(_.toUpperCase.equals(kind))
      .map(_ => userConsentUpdate.consented)
      .flatten
  }

  def toBoolean(v: String): Boolean = {
    v.toLowerCase match {
      case "true"  => true
      case "yes"   => true
      case "false" => false
      case "no"    => false
      case "0"     => false
      case "1"     => true
      case _       => false
    }
  }

}
