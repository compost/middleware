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
import com.jada.sqs.Body
import io.circe.Printer
import com.soft2bet.model.BonusUpdatedSQS
import com.soft2bet.model.Wagering
import com.soft2bet.model.WageringJSON
import org.apache.kafka.streams.processor.Punctuator
import java.time.Duration
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.common.serialization.StringSerializer
import java.io.FileWriter
import java.nio.file.Files
import org.apache.kafka.streams.state.KeyValueIterator
import java.nio.file.Path
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.sas.BlobSasPermission
import java.time.OffsetDateTime
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues
import java.time.Instant
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp

class PlayerPunctuatorTransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    playerStore: String
) extends Transformer[String, Player, KeyValue[String, Player]]
    with Punctuator {

  private var processorContext: ProcessorContext = _
  private var storePlayers: KeyValueStore[String, Player] = _
  private val sender = new Sender(config, sqs, ueNorthSQS, false, false)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[PlayerPunctuatorTransformer])

  override def transform(
      key: String,
      value: Player
  ): KeyValue[String, Player] = {
    val stored = storePlayers.get(key)
    val playerToSave = Option(stored) match {
      case None => value
      case Some(inStore) => {
        inStore.copy(
          first_name = value.first_name.orElse(inStore.first_name),
          last_name = value.last_name.orElse(inStore.last_name),
          email = value.email.orElse(inStore.email),
          phone_number = value.phone_number.orElse(inStore.phone_number),
          is_self_excluded =
            value.is_self_excluded.orElse(inStore.is_self_excluded),
          vip = value.vip.orElse(inStore.vip),
          reg_datetime = value.reg_datetime.orElse(inStore.reg_datetime),
          language = value.language.orElse(inStore.language),
          affiliate_id = value.affiliate_id.orElse(inStore.affiliate_id),
          dob = value.dob.orElse(inStore.dob),
          country_id = value.country_id.orElse(inStore.country_id),
          is_suppressed = value.is_suppressed.orElse(inStore.is_suppressed),
          brand_id = value.brand_id.orElse(inStore.brand_id),
          brand_name = PlayerStoreSQS.fixBrandName(
            value.brand_id.orElse(inStore.brand_id),
            value.brand_name.orElse(inStore.brand_name)
          ),
          Postcode = value.Postcode.orElse(inStore.Postcode),
          Country = value.Country.orElse(inStore.Country),
          IsSelfExcludedOtherBrand = value.IsSelfExcludedOtherBrand.orElse(
            inStore.IsSelfExcludedOtherBrand
          ),
          AccountType = value.AccountType.orElse(inStore.AccountType),
          BonusProgram = value.BonusProgram.orElse(inStore.BonusProgram),
          Currency = value.Currency.orElse(inStore.Currency),
          Duplicate = value.Duplicate.orElse(inStore.Duplicate),
          GDPR = value.GDPR.orElse(inStore.GDPR),
          Gender = value.Gender.orElse(inStore.Gender),
          ReferralType = value.ReferralType.orElse(inStore.ReferralType),
          RegistrationPlatform =
            value.RegistrationPlatform.orElse(inStore.RegistrationPlatform),
          Username = value.Username.orElse(inStore.Username),
          IsEmailValid = value.IsEmailValid.orElse(inStore.IsEmailValid),
          IsVerified = value.IsVerified.orElse(inStore.IsVerified),
          Avatar = value.Avatar.orElse(inStore.Avatar),
          BonusChosen = value.BonusChosen.orElse(inStore.BonusChosen),
          ZeroBounce = value.ZeroBounce.orElse(inStore.ZeroBounce),
          License = value.License.orElse(inStore.License),
          isZeroBalance = value.isZeroBalance.orElse(inStore.isZeroBalance),
          GlobalProductClassification = value.GlobalProductClassification
            .orElse(inStore.GlobalProductClassification),
          ProductClassification =
            value.ProductClassification.orElse(inStore.ProductClassification),
          GGRAmount = value.GGRAmount.orElse(inStore.GGRAmount),
          GGRAmountEUR = value.GGRAmountEUR.orElse(inStore.GGRAmountEUR),
          NGRAmount = value.NGRAmount.orElse(inStore.NGRAmount),
          NGRAmountEUR = value.NGRAmountEUR.orElse(inStore.NGRAmountEUR),
          Balance = value.Balance.orElse(inStore.Balance),
          BalanceEUR = value.BalanceEUR.orElse(inStore.BalanceEUR),
          BonusGGRRatio = value.BonusGGRRatio.orElse(inStore.BonusGGRRatio),
          BonusDepositRatio =
            value.BonusDepositRatio.orElse(inStore.BonusDepositRatio),
          NGRDepositRatio =
            value.NGRDepositRatio.orElse(inStore.NGRDepositRatio),
          TotalDeposit = value.TotalDeposit.orElse(inStore.TotalDeposit),
          TotalDepositEUR =
            value.TotalDepositEUR.orElse(inStore.TotalDepositEUR),
          BonusAbusing = value.BonusAbusing.orElse(inStore.BonusAbusing),
          BlockedCasino = value.BlockedCasino.orElse(inStore.BlockedCasino),
          BlockedSport = value.BlockedSport.orElse(inStore.BlockedSport),
          CanBeContactedBySMS =
            value.CanBeContactedBySMS.orElse(inStore.CanBeContactedBySMS),
          CanBeContactedByEmail =
            value.CanBeContactedByEmail.orElse(inStore.CanBeContactedByEmail),
          CanBeContactedByPhoneOrDynamicMessage =
            value.CanBeContactedByPhoneOrDynamicMessage.orElse(
              inStore.CanBeContactedByPhoneOrDynamicMessage
            ),
          CanReceivePromotions =
            value.CanReceivePromotions.orElse(inStore.CanReceivePromotions)
        )

      }
    }
    storePlayers.put(key, playerToSave)
    new KeyValue(key, value)
  }

  override def close(): Unit = {}

  override def punctuate(timestamp: Long): Unit = {

    if (Utils.canBeExecuted(timestamp)) {
      Sender.Brands.foreach(prefix => {
        pushFile(prefix, timestamp)
      })
    }
  }

  def pushFile(prefix: String, timestamp: Long): Unit = {
    logger.debugv(
      s"push file",
      Array(
        kv("prefix", prefix)
      ): _*
    )
    val players = storePlayers.prefixScan(s"${prefix}-", new StringSerializer())
    try {
      if (players.hasNext()) {
        val file = writeFile(prefix, players)
        expose(prefix, timestamp, file)
        file.toFile().delete()
      }
    } finally {
      players.close()
    }
    cleanStore(prefix)
  }

  def writeFile(
      prefix: String,
      players: KeyValueIterator[String, Player]
  ): Path = {
    val tmp = Files.createTempFile(s"p-${prefix}", ".json")
    val writer = new FileWriter(tmp.toFile())
    logger.debugv(
      s"start - write file",
      Array(
        kv("prefix", prefix),
        kv("tmp", tmp)
      ): _*
    )
    try {

      while (players.hasNext()) {
        val keyAndValue = players.next()
        val player = keyAndValue.value
        if (
          player.GGRAmount.isDefined ||
          player.GGRAmountEUR.isDefined ||
          player.NGRAmount.isDefined ||
          player.NGRAmountEUR.isDefined ||
          player.BonusGGRRatio.isDefined ||
          player.BonusDepositRatio.isDefined ||
          player.NGRDepositRatio.isDefined ||
          player.TotalDeposit.isDefined ||
          player.TotalDepositEUR.isDefined
        ) {
          val ld = PlayerData(
            GGRAmount = player.GGRAmount,
            GGRAmountEUR = player.GGRAmountEUR,
            NGRAmount = player.NGRAmount,
            NGRAmountEUR = player.NGRAmountEUR,
            BonusGGRRatio = player.BonusGGRRatio,
            BonusDepositRatio = player.BonusDepositRatio,
            NGRDepositRatio = player.NGRDepositRatio,
            TotalDeposit = player.TotalDeposit,
            TotalDepositEUR = player.TotalDepositEUR
          )

          val ldJson = printer.print(
            PlayerData.lifetimeDepositSQSEncoder.apply(ld)
          )

          val line =
            s"""{"id":"${player.player_id.get}","properties":${ldJson}}\n"""
          writer.write(line)
        }
        storePlayers.delete(keyAndValue.key)
      }
    } finally {
      logger.debugv(
        s"end - write file",
        Array(
          kv("prefix", prefix),
          kv("tmp", tmp)
        ): _*
      )
      writer.close()
    }

    tmp
  }

  def cleanStore(prefix: String): Unit = {
    val players = storePlayers.prefixScan(s"${prefix}-", new StringSerializer())
    try {
      while (players.hasNext()) {
        storePlayers.delete(players.next().key)
      }
    } finally {
      players.close()
    }
  }

  def expose(prefix: String, timestamp: Long, file: Path): Unit = {
    val client = new BlobServiceClientBuilder()
      .connectionString(config.connectionString)
      .buildClient()
    val blobClient = client
      .getBlobContainerClient(config.outputContainerName)
      .getBlobClient(
        s"data/${prefix}/players/aggregations-${timestamp}.json"
      )
    blobClient.uploadFromFile(file.toString(), true)
    val blobSasPermission = new BlobSasPermission().setReadPermission(true)
    val expiryTime = OffsetDateTime.now().plusDays(7)
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

    sender.sendToSQSByPrefix(
      body,
      url,
      prefix
    )
  }

  override def init(processorContext: ProcessorContext): Unit = {
    this.storePlayers = processorContext.getStateStore(playerStore)
    processorContext.schedule(
      config.punctuator,
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }
}

case class PlayerData(
    GGRAmount: Option[String] = None,
    GGRAmountEUR: Option[String] = None,
    NGRAmount: Option[String] = None,
    NGRAmountEUR: Option[String] = None,
    BonusGGRRatio: Option[String] = None,
    BonusDepositRatio: Option[String] = None,
    NGRDepositRatio: Option[String] = None,
    TotalDeposit: Option[String] = None,
    TotalDepositEUR: Option[String] = None
)

object PlayerData {

  import io.circe.Encoder
  import io.circe.generic.semiauto._
  implicit val lifetimeDepositSQSEncoder: Encoder[PlayerData] = deriveEncoder
}
