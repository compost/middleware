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
  Wagering,
  Wallet
}
import io.circe.Printer
import com.azure.storage.blob.sas.BlobSasPermission
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues

import java.time.OffsetDateTime
import org.apache.kafka.streams.state.KeyValueIterator
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Path
import com.azure.storage.blob.BlobServiceClientBuilder

import java.util.UUID
import io.circe.Encoder
import io.circe.Decoder
import com.jada.serdes.CirceSerdes
import io.circe._
import io.circe.generic.auto._

import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.time.LocalDateTime

case class StatePlayer(stored: Option[PlayerStore], current: PlayerStore)

class MappingTransformer(
    config: ApplicationConfiguration,
    referentials: com.jada.Referentials,
    client: software.amazon.awssdk.services.sqs.SqsClient,
    startStopStoreName: String,
    playerBrandStoreName: String
) extends Transformer[String, Array[Byte], KeyValue[Unit, Unit]]
    with Punctuator {

  val bootstrap = new AtomicBoolean(true)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  val GenericUser = "GENERIC_USER"
  val UserBlockedToggle = "USER_BLOCKED_TOGGLE"
  private final val logger =
    Logger.getLogger(classOf[MappingTransformer])
  private var processorContext: ProcessorContext = _
  private var startStopStore: KeyValueStore[String, Action] = _
  private var playerBrandStore: KeyValueStore[String, PlayerStore] = _

  override def init(processorContext: ProcessorContext): Unit = {
    logger.debug(f"${config}")
    this.processorContext = processorContext
    this.startStopStore = processorContext.getStateStore(startStopStoreName)
    this.playerBrandStore = processorContext.getStateStore(playerBrandStoreName)
    this.processorContext.schedule(
      config.punctuator,
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }

  override def transform(k: String, v: Array[Byte]): KeyValue[Unit, Unit] = {

    val action: Option[Action] = Option(
      startStopStore.get(processorContext.applicationId())
    )

    val statePlayer = processorContext.topic() match {

      case config.topicCoordinator =>
        startStopStore.put(
          processorContext.applicationId(),
          deserialize[Action](v)
        )
        StatePlayer(None, null)

      case _ if action.exists(f => f.action == Common.stop) =>
        logger.debug("nothing to do")
        StatePlayer(None, null)

      case config.topicPlayersRepartitioned =>
        val inputPlayer = deserialize[Player](v)

        val playerFromStore = playerBrandStore.get(generateKey(inputPlayer))
        val newPlayer = Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              player_id = Some(k),
              brand_id = inputPlayer.brand_id.orElse(player.brand_id),
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
              dob = inputPlayer.dob.orElse(player.dob),
              vip = inputPlayer.vip.orElse(player.vip),
              currency_description = inputPlayer.currency_id
                .map(v =>
                  referentials.currencies
                    .get(
                      inputPlayer.brand_id.orElse(player.brand_id).getOrElse("")
                    )
                    .getOrElse(Map.empty[String, String])
                    .get(v)
                    .getOrElse("")
                )
                .orElse(player.currency_description),
              country_id = inputPlayer.country_id.orElse(player.country_id)
            )

          case None =>
            // partially register non null infos
            PlayerStore(
              player_id = Some(k),
              brand_id = inputPlayer.brand_id,
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
              currency_id = inputPlayer.currency_id,
              country_id = inputPlayer.country_id,
              currency_description = inputPlayer.currency_id.map(v =>
                referentials.currencies
                  .get(inputPlayer.brand_id.getOrElse(""))
                  .getOrElse(Map.empty[String, String])
                  .get(v)
                  .getOrElse("")
              )
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
            newPlayer.brand_id.getOrElse(""),
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
               |"contactId": "$k",
               |"properties": {
               | "first_deposit_datetime": "${inputPlayer.first_dep_datetime
                .map(PlayerSQS.keepYearMonthDay(_))
                .get}"
               |}
               |}
               |""".stripMargin
          sendMessageToSQS(
            k,
            inputPlayer.brand_id.getOrElse(""),
            firstDepositMessage
          )
        }

        StatePlayer(Option(playerFromStore), newPlayer)

      case config.topicLoginsRepartitioned =>
        val login = deserialize[Login](v)
        val playerFromStore = playerBrandStore.get(generateKey(login))

        val newPlayerWithLogin = Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              player_id = Some(k),
              brand_id = login.brand_id.orElse(playerFromStore.brand_id),
              last_login_datetime =
                login.login_datetime.orElse(player.last_login_datetime),
              last_login_success =
                login.login_success.orElse(player.last_login_success)
            )

          case None =>
            PlayerStore(
              player_id = Some(k),
              brand_id = login.brand_id,
              last_login_datetime = login.login_datetime,
              last_login_success = login.login_success
            )
        }
        val loginSQS = LoginSQS(login)
        send(
          k,
          login.brand_id.getOrElse(""),
          GenericUser,
          config.mappingSelectorLogins,
          loginSQS
        )
        StatePlayer(Option(playerFromStore), newPlayerWithLogin)

      case config.topicWalletRepartitioned =>
        val wallet = deserialize[Wallet](v)
        val newActivityDate = wallet.transaction_datetime.map(x => parseDate(x))

        val playerFromStore = playerBrandStore.get(generateKey(wallet))
        val newPlayerWithWallet = Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              player_id = Some(k),
              brand_id = wallet.brand_id.orElse(player.brand_id),
              wallet_transaction_type_id = wallet.transaction_type_id.orElse(
                player.wallet_transaction_type_id
              ),
              wallet_transaction_status_id = wallet.transaction_status_id
                .orElse(player.wallet_transaction_status_id),
              last_activity_date =
                newActivityDate.orElse(player.last_activity_date)
            )

          case None =>
            PlayerStore(
              player_id = Some(k),
              brand_id = wallet.brand_id,
              wallet_transaction_type_id = wallet.transaction_type_id,
              wallet_transaction_status_id = wallet.transaction_status_id,
              last_activity_date =
                Some(newActivityDate.getOrElse(Instant.now()))
            )
        }

        if (
          newPlayerWithWallet.wallet_transaction_type_id.getOrElse(
            ""
          ) == "Deposit" &&
          newPlayerWithWallet.wallet_transaction_status_id
            .getOrElse("")
            .toUpperCase() != "ACCEPTED"
        ) {
          // failed_deposit
          val failedDepositMessage =
            s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector": "failed_deposit",
               |"contactId": "$k",
               |"properties": {
               | "failed_deposit_date": "${wallet.transaction_datetime
                .map(PlayerSQS.keepYearMonthDay(_))
                .get}"
               |}
               |}
               |""".stripMargin
          sendMessageToSQS(
            k,
            wallet.brand_id.getOrElse(""),
            failedDepositMessage
          )
        }

        if (
          newPlayerWithWallet.wallet_transaction_type_id.getOrElse(
            ""
          ) == "Withdraw" &&
          newPlayerWithWallet.wallet_transaction_status_id
            .getOrElse("")
            .toUpperCase() != "ACCEPTED"
        ) {
          // failed_withdrawal
          val failedWithdrawalMessage =
            s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector": "failed_withdrawal",
               |"contactId": "$k",
               |"properties": {
               | "failed_withdrawal_date": "${wallet.transaction_datetime
                .map(PlayerSQS.keepYearMonthDay(_))
                .get}"
               |}
               |}
               |""".stripMargin
          sendMessageToSQS(
            k,
            wallet.brand_id.getOrElse(""),
            failedWithdrawalMessage
          )
        }

        StatePlayer(Option(playerFromStore), newPlayerWithWallet)

      case config.topicWageringRepartitioned =>
        val wagering = deserialize[Wagering](v)
        val newActivityDate =
          wagering.bet_datetime.map(x => parseDate(x))

        val playerFromStore = playerBrandStore.get(generateKey(wagering))
        val newPlayerWithWagering = Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              player_id = Some(k),
              brand_id = wagering.brand_id
                .orElse(player.brand_id),
              wagering_transaction_type_id = wagering.transaction_type_id
                .orElse(player.wagering_transaction_type_id),
              wagering_ggr_amount =
                wagering.ggr_amount.orElse(player.wagering_ggr_amount),
              wagering_has_resulted =
                wagering.has_resulted.orElse(player.wagering_has_resulted),
              last_activity_date =
                newActivityDate.orElse(player.last_activity_date)
            )

          case None =>
            PlayerStore(
              player_id = Some(k),
              brand_id = wagering.brand_id,
              wagering_transaction_type_id = wagering.transaction_type_id,
              wagering_ggr_amount = wagering.ggr_amount,
              wagering_has_resulted = wagering.has_resulted,
              last_activity_date =
                Some(newActivityDate.getOrElse(Instant.now()))
            )
        }

        if (
          wagering.transaction_type_id.getOrElse("").toUpperCase() == "WIN" &&
          wagering.has_resulted
            .map(_.toLowerCase())
            .map {
              {
                case "true"  => true
                case "false" => false
                case "0"     => false
                case "1"     => true
                case _       => false
              }
            }
            .getOrElse(false) &&
          wagering.ggr_amount.getOrElse("0").toFloat >= 1000
        ) {
          // big_win
          val bigWinMessage =
            s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector": "big_win",
               |"contactId": "$k",
               |"properties": {
               | "ggr_amount": "${wagering.ggr_amount.get}"
               |}
               |}
               |""".stripMargin
          sendMessageToSQS(
            k,
            wagering.brand_id.getOrElse(""),
            bigWinMessage
          )
        }
        StatePlayer(Option(playerFromStore), newPlayerWithWagering)

    }

    val playerToSaveInStore = statePlayer.current
    val storedPlayer = statePlayer.stored

    if (playerToSaveInStore != null) {
      playerBrandStore.put(
        generateKey(playerToSaveInStore),
        playerToSaveInStore
      )

      val playerSQS = PlayerSQS(playerToSaveInStore, referentials)

      if (
        (storedPlayer.isEmpty || !(
          playerToSaveInStore.reg_datetime == storedPlayer.get.reg_datetime &&
            playerToSaveInStore.first_name == storedPlayer.get.first_name &&
            playerToSaveInStore.last_name == storedPlayer.get.last_name &&
            playerToSaveInStore.email == storedPlayer.get.email &&
            playerToSaveInStore.phone_number == storedPlayer.get.phone_number &&
            playerToSaveInStore.language == storedPlayer.get.language &&
            playerToSaveInStore.affiliate_id == storedPlayer.get.affiliate_id &&
            playerToSaveInStore.is_self_excluded == storedPlayer.get.is_self_excluded &&
            playerToSaveInStore.first_dep_datetime == storedPlayer.get.first_dep_datetime &&
            playerToSaveInStore.dob == storedPlayer.get.dob &&
            playerToSaveInStore.country_id == storedPlayer.get.country_id &&
            playerToSaveInStore.vip == storedPlayer.get.vip &&
            playerToSaveInStore.currency_id == storedPlayer.get.currency_id &&
            playerToSaveInStore.currency_description == storedPlayer.get.currency_description
        )) && playerToSaveInStore.brand_id.isDefined
      ) {

        send(
          k,
          playerSQS.brand_id.get,
          GenericUser,
          config.mappingSelectorPlayers,
          playerSQS
        )

      }
    }

    if (
      playerToSaveInStore != null && (storedPlayer.isEmpty || (playerToSaveInStore.is_self_excluded != storedPlayer.get.is_self_excluded))
    ) {
      val isBlocked =
        playerToSaveInStore.is_self_excluded.getOrElse("false") match {
          case "true" => true
          case _      => false
        }
      sendUserBlockedToggle(
        k,
        playerToSaveInStore.brand_id.getOrElse(""),
        isBlocked
      )
    }
    null
  }

  def sendUserBlockedToggle(
      player_id: String,
      brand_id: String,
      isBlocked: Boolean
  ): Unit = {
    sendMessageToSQS(
      player_id,
      brand_id,
      s"""{"type":"${UserBlockedToggle}","mappingSelector":"self_exclusion","contactId":"${player_id}","blocked":$isBlocked}"""
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
    // TODO populate with brandID
    val queue = brandId match {
      case "164" => config.sqsQueueSoftMayaBetShift
      case "72"  => config.sqsQueueSoftMayaPokies
      case _     => config.sqsQueueSoftMaya
    }

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

  override def punctuate(timestamp: Long): Unit = {
    logger.debug("punctuate")

    // churn
    val players = playerBrandStore.all()
    while (players.hasNext) {
      val p = players.next().value
      logger.debug(s"last_activity_date ${p.last_activity_date}")
      if (
        p.player_id.isDefined && p.last_activity_date.isDefined &&
        ChronoUnit.DAYS.between(
          LocalDate.ofInstant(p.last_activity_date.get, ZoneId.of("UTC")),
          LocalDate.now()
        ) > 30 &&
        !p.sent_customer_churn
      ) {
        val bigWinMessage =
          s"""
             |{"type": "GENERIC_USER",
             |"mappingSelector": "customer_churn",
             |"contactId": "${p.player_id.get}",
             |"properties": {
             | "last_activity_date": "${PlayerSQS.keepYearMonthDay(
              // DateTimeFormatter.ISO_INSTANT.format behind the string. The date is printed with T.
              p.last_activity_date.get
                .toString()
            )}"
             |}
             |}
             |""".stripMargin
        sendMessageToSQS(
          p.player_id.get,
          p.brand_id.getOrElse(""),
          bigWinMessage
        )

        // mark customer churn as already sent
        val newPlayerStore = p.copy(sent_customer_churn = true)
        playerBrandStore.put(generateKey(p), newPlayerStore)
      }
    }
    logger.debug("punctuator done")
    players.close()

    if (config.bootstrap) {
      if (bootstrap.compareAndSet(true, false)) {
        val players = playerBrandStore.all()
        try {
          val now = UUID.randomUUID.toString
          logger.info(s"start bootstrap players ${now}")
          val brandFile = writeFile(now, players)
          brandFile.foreach {
            case (key, value) => {
              logger.debug(s"expose ${key}")
              expose(key, now, value)
            }
          }
          logger.info(s"end bootstrap players ${now}")
        } finally {
          players.close()
        }
      }
    }

  }

  def generateKey(p: PlayerStore): String = {
    s"${p.brand_id.get}-${p.player_id.get}"
  }

  def generateKey(p: Login): String = {
    s"${p.brand_id.get}-${p.player_id.get}"
  }
  def generateKey(p: Player): String = {
    s"${p.brand_id.get}-${p.player_id.get}"
  }
  def generateKey(p: Wallet): String = {
    s"${p.brand_id.get}-${p.player_id.get}"
  }
  def generateKey(p: Wagering): String = {
    s"${p.brand_id.get}-${p.player_id.get}"
  }
  def parseDate(date: String): Instant = {
    val DateTimeformatter3Digits = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
      .withZone(ZoneId.systemDefault())

    val DateTimeformatter2Digits = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SS")
      .withZone(ZoneId.systemDefault())

    val DateTimeformatter1Digit = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.S")
      .withZone(ZoneId.systemDefault())

    if (date.length == 23) {
      Instant.from(DateTimeformatter3Digits.parse(date))
    } else if (date.length == 22) {
      Instant.from(DateTimeformatter2Digits.parse(date))
    } else if (date.length == 21) {
      Instant.from(DateTimeformatter1Digit.parse(date))
    } else {
      Instant.from(
        DateTimeformatter3Digits.parse(s"${date.substring(0, 10)}T00:00:00.000")
      )
    }
  }

  override def close(): Unit = {}

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
         |  "mappingSelector":"batch-import",
         |  "downloadUri": "$url"
         |}
         |""".stripMargin

    logger.debug(body)
    sendMessageToSQS("batch-import", brandId, body)
  }

}
