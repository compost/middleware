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
import com.jada.models.{Body, CustomerDetail}
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
import com.jada.models.CustomerCreated
import com.jada.models.CustomerVerified
import com.jada.models.CustomerUpdated
import com.jada.models.CustomerTag
import com.jada.models.BodyBlocks
import com.jada.models.CustomerBlocks
import com.jada.models.CustomerSession
import com.jada.models.CustomerConsent
import com.jada.models.CustomerBalance
import com.jada.models.History
import com.jada.models.Wallet
import com.jada.models.FirstDeposit
import com.jada.models.FirstWithdrawal
import com.jada.models.LastDeposit
import com.jada.models.LastWithdrawal
import com.jada.models.CustomerFPP
import com.jada.models.HistoryConsent
import com.jada.models.HistoryBlocked

class MappingTransformer(
    config: ApplicationConfiguration,
    client: software.amazon.awssdk.services.sqs.SqsClient,
    customerDetailStoreName: String,
    customerBalanceStoreName: String
) extends Transformer[String, Array[Byte], KeyValue[Unit, Unit]]
    with Punctuator {

  val sender = new Sender
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )

  private final val logger =
    Logger.getLogger(classOf[MappingTransformer])
  private var processorContext: ProcessorContext = _
  private var customerDetailStore: KeyValueStore[String, CustomerDetail] = _
  private var customerBalanceStore: KeyValueStore[String, CustomerBalance] = _

  override def init(processorContext: ProcessorContext): Unit = {
    logger.debug(f"${config}")
    this.processorContext = processorContext
    this.customerDetailStore =
      processorContext.getStateStore(customerDetailStoreName)
    this.customerBalanceStore =
      processorContext.getStateStore(customerBalanceStoreName)
    processorContext.schedule(
      config.punctuator,
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }

  override def punctuate(timestamp: Long): Unit = {
    val now = UUID.randomUUID.toString
    val playersBalance = customerBalanceStore.all()
    try {
      logger.debug(s"start ${now}")
      val brandFile = writeFile(now, playersBalance)
      brandFile.foreach {
        case (key, value) => {
          logger.debug(s"expose ${key}")
          expose(key, now, value)
        }
      }
    } finally {
      playersBalance.close()
    }
    cleanStore()
    logger.debug(s"end balance players ${now}")
  }

  def cleanStore(): Unit = {
    val playersBalance = customerBalanceStore.all
    try {
      while (playersBalance.hasNext) {
        customerBalanceStore.delete(playersBalance.next().key)
      }
    } finally {
      playersBalance.close()
    }
  }

  def writeFile(
      now: String,
      playersBalance: KeyValueIterator[String, CustomerBalance]
  ): Map[String, Path] = {

    import io.circe._, io.circe.generic.auto._, io.circe.parser._,
    io.circe.syntax._
    val brandFile = scala.collection.mutable.Map[String, (Path, FileWriter)]()

    try {

      while (playersBalance.hasNext()) {
        val keyAndValue = playersBalance.next()

        if (keyAndValue.value.brand_id.isDefined) {
          val filewrite = brandFile.getOrElseUpdate(
            keyAndValue.value.brand_id.get, {
              val tmp = Files.createTempFile(
                s"players-balance-${now}-${keyAndValue.value.brand_id}",
                ".json"
              )
              logger.debug(s"file: ${tmp}")
              val writer = new FileWriter(tmp.toFile())
              (tmp, writer)
            }
          )

          filewrite._2.write(
            s"""{"id":"${keyAndValue.value.original_id.get}", "properties":${printer
                .print(keyAndValue.value.asJson)}}\n"""
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
        s"data/${brandId}/players-balance/data-${timestamp}.json"
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
         |  "mappingSelector": "${computeMappingSelector(
          Some(brandId),
          "CUSTOMER_BALANCE"
        )}",
         |  "downloadUri": "$url"
         |}
         |""".stripMargin

    logger.debug(body)
    sender.sendMessageToSQS(
      config,
      client,
      brandId,
      brandId,
      body
    )
  }

  override def transform(key: String, v: Array[Byte]): KeyValue[Unit, Unit] = {
    val customerDetailFromStore = Option(customerDetailStore.get(key))

    val customerDetailToSaveInStore: Option[CustomerDetail] =
      processorContext.topic() match {
        case config.topicCustomerDetailRepartitioned =>
          customerDetailProcess(
            deserialize[CustomerDetail](v),
            customerDetailFromStore
          )

        case config.topicHistory =>
          historyProcess(
            deserialize[History](v),
            customerDetailFromStore
          )
        case config.topicHistoryConsent =>
          val hc = deserialize[HistoryConsent](v)
          send(
            hc.PLAYER_ID,
            hc.BRAND_ID,
            "USER_CONSENT_UPDATE",
            "CUSTOMER_CONSENT",
            CustomerConsent(hc)
          )
          None
        case config.topicHistoryBlocked =>
          val hc = deserialize[HistoryBlocked](v)
          val body = BodyBlocks(
            "USER_BLOCKED_TOGGLE",
            hc.PLAYER_ID.get,
            computeMappingSelector(hc.BRAND_ID, "CUSTOMER_BLOCKS"),
            CustomerBlocks(hc),
            hc.BLOCKED.map(_.toLowerCase()).contains("true")
          )
          sender.sendMessageToSQS(
            config,
            client,
            hc.PLAYER_ID.get,
            hc.BRAND_ID.get,
            printer.print(BodyBlocks.bodyBlocksEncoder.apply(body))
          )
          None
        case config.topicWalletRepartitioned =>
          walletProcess(
            deserialize[Wallet](v),
            customerDetailFromStore
          )

        case _ => None
      }
    customerDetailToSaveInStore.foreach(
      customerDetailStore.put(key, _)
    )
    null
  }
  def walletProcess(
      w: Wallet,
      stored: Option[CustomerDetail]
  ): Option[CustomerDetail] = {
    val newCustomerDetail = stored match {
      case None =>
        Some(
          CustomerDetail(
            player_id = w.player_id,
            brand_id = w.brand_id,
            first_deposit_datetime = w.transaction_datetime.filter(_ =>
              w.transaction_type_id.map(_.toLowerCase).contains(Wallet.Deposit)
            ),
            first_withdrawal_datetime = w.transaction_datetime.filter(_ =>
              w.transaction_type_id
                .map(_.toLowerCase)
                .contains(Wallet.Withdrawal)
            )
          )
        )
      case Some(previous) =>
        Some(
          previous.copy(
            first_deposit_datetime = w.transaction_datetime
              .filter(_ =>
                w.transaction_type_id
                  .map(_.toLowerCase)
                  .contains(Wallet.Deposit)
              )
              .orElse(previous.first_deposit_datetime),
            first_withdrawal_datetime = w.transaction_datetime
              .filter(_ =>
                w.transaction_type_id
                  .map(_.toLowerCase)
                  .contains(Wallet.Withdrawal)
              )
              .orElse(previous.first_withdrawal_datetime)
          )
        )
    }

    val firstDeposit = FirstDeposit(w)
    if (
      (stored.isEmpty || stored.get.first_deposit_datetime.isEmpty) && firstDeposit.isDefined
    ) {
      send(
        newCustomerDetail.get.player_id,
        newCustomerDetail.get.brand_id,
        "GENERIC_USER",
        "FIRST_DEPOSIT",
        firstDeposit.get
      )
    }

    val firstWithdrawal = FirstWithdrawal(w)
    if (
      (stored.isEmpty || stored.get.first_withdrawal_datetime.isEmpty) && firstWithdrawal.isDefined
    ) {
      send(
        newCustomerDetail.get.player_id,
        newCustomerDetail.get.brand_id,
        "GENERIC_USER",
        "FIRST_WITHDRAWAL",
        firstWithdrawal.get
      )
    }

    LastDeposit(w).foreach(v =>
      send(
        newCustomerDetail.get.player_id,
        newCustomerDetail.get.brand_id,
        "GENERIC_USER",
        "LAST_DEPOSIT",
        v
      )
    )
    LastWithdrawal(w).foreach(v =>
      send(
        newCustomerDetail.get.player_id,
        newCustomerDetail.get.brand_id,
        "GENERIC_USER",
        "LAST_WITHDRAWAL",
        v
      )
    )
    newCustomerDetail
  }

  def historyProcess(
      h: History,
      stored: Option[CustomerDetail]
  ): Option[CustomerDetail] = {
    stored match {
      case None =>
        Some(
          CustomerDetail(
            player_id = h.ORIGINALID,
            brand_id = h.BRAND_ID,
            first_deposit_datetime =
              h.FIRST_DEPOSIT_DATE.filterNot(_.trim.isEmpty),
            first_withdrawal_datetime =
              h.FIRST_WITHDRAWAL_DATE.filterNot(_.trim.isEmpty)
          )
        )
      case Some(previous) =>
        Some(
          previous.copy(
            first_deposit_datetime =
              h.FIRST_DEPOSIT_DATE.filterNot(_.trim.isEmpty),
            first_withdrawal_datetime =
              h.FIRST_WITHDRAWAL_DATE.filterNot(_.trim.isEmpty)
          )
        )
    }
  }

  def customerDetailProcess(
      cd: CustomerDetail,
      stored: Option[CustomerDetail]
  ): Option[CustomerDetail] = {
    val current = stored match {
      case Some(previous) =>
        Some(
          previous.copy(
            player_id = cd.player_id.orElse(previous.player_id),
            brand_id = cd.brand_id.orElse(previous.brand_id),
            last_name = cd.last_name.orElse(previous.last_name),
            current_country_name =
              cd.current_country_name.orElse(previous.current_country_name),
            acquisition_source_code = cd.acquisition_source_code.orElse(
              previous.acquisition_source_code
            ),
            city = cd.city.orElse(previous.city),
            mobile = cd.mobile.orElse(previous.mobile),
            sex = cd.sex.orElse(previous.sex),
            birth_date = cd.birth_date.orElse(previous.birth_date),
            signup_language =
              cd.signup_language.orElse(previous.signup_language),
            postal_code = cd.postal_code.orElse(previous.postal_code),
            first_name = cd.first_name.orElse(previous.first_name),
            email = cd.email.orElse(previous.email),
            account_creation_datetime_local =
              cd.account_creation_datetime_local.orElse(
                previous.account_creation_datetime_local
              ),
            phone = cd.phone.orElse(previous.phone),
            customer_currency_code =
              cd.customer_currency_code.orElse(previous.customer_currency_code),
            username = cd.username.orElse(previous.username),
            role_name = cd.role_name.orElse(previous.role_name),

            // CUSTOMER_VERIFIED
            account_verification_datetime_local =
              cd.account_verification_datetime_local
                .orElse(previous.account_verification_datetime_local),
            reference_btag = cd.reference_btag.orElse(previous.reference_btag),
            click_id = cd.click_id.orElse(previous.click_id),

            // CUSTOMER_TAG
            tag_name = cd.tag_name.orElse(previous.tag_name),
            is_abuser = cd.is_abuser.orElse(previous.is_abuser),
            // CUSTOMER_SESSION
            last_login_datetime_local = cd.last_login_datetime_local.orElse(
              previous.last_login_datetime_local
            ),
            last_deposit_datetime_local = cd.last_deposit_datetime_local.orElse(
              previous.last_deposit_datetime_local
            ),
            // CUSTOMER_BLOCKS
            is_blocked = cd.is_blocked.orElse(previous.is_blocked),
            is_self_excluded =
              cd.is_self_excluded.orElse(previous.is_self_excluded),
            fpp_customer_level =
              cd.fpp_customer_level.orElse(previous.fpp_customer_level),
            fpp_reward_level_id =
              cd.fpp_reward_level_id.orElse(previous.fpp_reward_level_id),
            fpp_update_datetime_local = cd.fpp_update_datetime_local.orElse(
              previous.fpp_update_datetime_local
            ),
            consent_marketing_email = cd.consent_marketing_email.orElse(
              previous.consent_marketing_email
            ),
            consent_marketing_text_message = cd.consent_marketing_text_message
              .orElse(previous.consent_marketing_text_message),
            consent_marketing_direct_mail = cd.consent_marketing_direct_mail
              .orElse(previous.consent_marketing_direct_mail),
            consent_marketing_telephone = cd.consent_marketing_telephone.orElse(
              previous.consent_marketing_telephone
            ),
            consent_marketing_oms =
              cd.consent_marketing_oms.orElse(previous.consent_marketing_oms)
          )
        )
      case None => Some(cd)
    }

    dispatch(cd)
    current
  }

  def dispatch(cd: CustomerDetail): Unit = {
    cd.activity_field match {
      case Some(CustomerDetail.CustomerCreated) =>
        send(
          cd.player_id,
          cd.brand_id,
          "GENERIC_USER",
          "CUSTOMER_CREATED",
          CustomerCreated(cd)
        )
      case Some(CustomerDetail.CustomerVerified) =>
        None
        send(
          cd.player_id,
          cd.brand_id,
          "GENERIC_USER",
          "CUSTOMER_VERIFIED",
          CustomerVerified(cd)
        )
      case Some(CustomerDetail.CustomerFPP) =>
        None
        send(
          cd.player_id,
          cd.brand_id,
          "GENERIC_USER",
          "CUSTOMER_FPP",
          CustomerFPP(cd)
        )

      case Some(CustomerDetail.CustomerUpdated) =>
        send(
          cd.player_id,
          cd.brand_id,
          "GENERIC_USER",
          "CUSTOMER_UPDATED",
          CustomerUpdated(cd)
        )
      case Some(CustomerDetail.CustomerTag) =>
        send(
          cd.player_id,
          cd.brand_id,
          "GENERIC_USER",
          "CUSTOMER_TAG",
          CustomerTag(cd)
        )
      case Some(CustomerDetail.CustomerBlocks) =>
        val body = BodyBlocks(
          "USER_BLOCKED_TOGGLE",
          cd.player_id.get,
          computeMappingSelector(cd.brand_id, "CUSTOMER_BLOCKS"),
          CustomerBlocks(cd),
          cd.is_blocked.map(_.toLowerCase()).contains("true")
        )
        sender.sendMessageToSQS(
          config,
          client,
          cd.player_id.get,
          cd.brand_id.get,
          printer.print(BodyBlocks.bodyBlocksEncoder.apply(body))
        )
      case Some(CustomerDetail.CustomerSession) =>
        send(
          cd.player_id,
          cd.brand_id,
          "GENERIC_USER",
          "CUSTOMER_SESSION",
          CustomerSession(cd)
        )
      case Some(CustomerDetail.CustomerConsent) =>
        send(
          cd.player_id,
          cd.brand_id,
          "USER_CONSENT_UPDATE",
          "CUSTOMER_CONSENT",
          CustomerConsent(cd)
        )
      case Some(CustomerDetail.CustomerBalance) =>
        customerBalanceStore.put(
          s"${cd.brand_id.get}-${cd.player_id.get}",
          CustomerBalance(cd)
        )

      case _ =>
        logger.warnv("not handled", kv("activity_field", cd.activity_field))
    }

  }

  def deserialize[T: Decoder](data: Array[Byte]): T = {
    parser.decode[T](new String(data)).right.get
  }

  def send[T: Encoder](
      playerId: Option[String],
      brandId: Option[String],
      `type`: String,
      mappingSelector: String,
      value: T
  ): Unit = {
    val body = Body[T](
      `type`,
      playerId.get,
      computeMappingSelector(brandId, mappingSelector),
      value
    )
    sender.sendMessageToSQS(
      config,
      client,
      playerId.get,
      brandId.get,
      printer.print(Body.bodyEncoder[T].apply(body))
    )
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
