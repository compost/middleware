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
import com.soft2bet.model.keepYYYYMMDD
import java.util.UUID

class LifetimeDepositCountTransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    walletStore: String
) extends Transformer[String, Wallet, KeyValue[String, Wallet]]
    with Punctuator {

  private var processorContext: ProcessorContext = _
  private var storeWallet: KeyValueStore[String, Wallet] = _
  private val sender = new Sender(config, sqs, ueNorthSQS, both = true, false)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[LifetimeDepositCountTransformer])

  override def transform(
      key: String,
      value: Wallet
  ): KeyValue[String, Wallet] = {
    val old = Option(storeWallet.get(key))

    val newValue = old match {
      case Some(previous) =>
        previous.copy(
          player_id = value.player_id.orElse(previous.player_id),
          transaction_id = value.transaction_id.orElse(previous.transaction_id),
          transaction_type_id =
            value.transaction_type_id.orElse(previous.transaction_type_id),
          transaction_status_id =
            value.transaction_status_id.orElse(previous.transaction_status_id),
          amount = value.amount.orElse(previous.amount),
          transaction_datetime =
            value.transaction_datetime.orElse(previous.transaction_datetime),
          brand_id = value.brand_id.orElse(previous.brand_id),
          payment_method = value.payment_method.orElse(previous.payment_method),
          currency_id = value.currency_id.orElse(previous.currency_id),
          first_deposit_datetime = value.first_deposit_datetime.orElse(
            previous.first_deposit_datetime
          ),
          last_deposit_datetime =
            value.last_deposit_datetime.orElse(previous.last_deposit_datetime),
          CasinoBonusBalance =
            value.CasinoBonusBalance.orElse(previous.CasinoBonusBalance),
          SportBonusBalance =
            value.SportBonusBalance.orElse(previous.SportBonusBalance),
          CasinoBonusBalanceEUR =
            value.CasinoBonusBalanceEUR.orElse(previous.CasinoBonusBalanceEUR),
          SportBonusBalanceEUR =
            value.SportBonusBalanceEUR.orElse(previous.SportBonusBalanceEUR),
          LifetimeWithdrawalCount = value.LifetimeWithdrawalCount.orElse(
            previous.LifetimeWithdrawalCount
          ),
          LifetimeWithdrawalCountEUR = value.LifetimeWithdrawalCountEUR.orElse(
            previous.LifetimeWithdrawalCountEUR
          ),
          LifetimeDepositCount =
            value.LifetimeDepositCount.orElse(previous.LifetimeDepositCount),
          TotalWithdrawalLifetime = value.TotalWithdrawalLifetime.orElse(
            previous.TotalWithdrawalLifetime
          ),
          TotalWithdrawalLifetimeEUR = value.TotalWithdrawalLifetimeEUR.orElse(
            previous.TotalWithdrawalLifetimeEUR
          ),
          TotalDepositEUR =
            value.TotalDepositEUR.orElse(previous.TotalDepositEUR),
          TotalDeposit = value.TotalDeposit.orElse(previous.TotalDeposit),
          currency = value.currency.orElse(previous.currency),
          PayMethod = value.PayMethod.orElse(previous.PayMethod)
        )
      case None => value
    }

    storeWallet.put(key, newValue)

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
    val wallets = storeWallet.prefixScan(s"${prefix}-", new StringSerializer())
    try {
      if (wallets.hasNext()) {
        val file = writeFile(prefix, wallets)
        expose(prefix, timestamp, file)
        file.toFile().delete()
      }
    } finally {
      wallets.close()
    }
    cleanStore(prefix)
  }

  def cleanStore(prefix: String): Unit = {
    val it = storeWallet.prefixScan(s"${prefix}-", new StringSerializer())
    try {
      while (it.hasNext()) {
        storeWallet.delete(it.next().key)
      }
    } finally {
      it.close()
    }
  }

  def writeFile(
      prefix: String,
      wallets: KeyValueIterator[String, Wallet]
  ): Path = {
    val tmp = Files.createTempFile(s"ldc-${prefix}", ".json")
    val writer = new FileWriter(tmp.toFile())
    logger.debugv(
      s"start - write file",
      Array(
        kv("prefix", prefix),
        kv("tmp", tmp)
      ): _*
    )
    try {

      while (wallets.hasNext()) {
        val keyAndValue = wallets.next()
        val id = keyAndValue.value.player_id.get
        val lifetimeDepositCount =
          keyAndValue.value.LifetimeDepositCount.map(_.toIntOption).flatten
        val ld = LifetimeDeposit(
          lifetimeDepositCount = lifetimeDepositCount,
          averageDeposit = keyAndValue.value.TotalDepositEUR
            .filter(_ => lifetimeDepositCount.filter(v => v > 0).isDefined)
            .map(_.toDouble)
            .map(v => v / lifetimeDepositCount.get),
          last_deposit_date =
            keyAndValue.value.last_deposit_datetime.map(keepYYYYMMDD(_)),
          LifetimeWithdrawalCount = keyAndValue.value.LifetimeWithdrawalCount,
          LifetimeWithdrawalCountEUR =
            keyAndValue.value.LifetimeWithdrawalCountEUR,
          TotalWithdrawalLifetime = keyAndValue.value.TotalWithdrawalLifetime,
          TotalWithdrawalLifetimeEUR =
            keyAndValue.value.TotalWithdrawalLifetimeEUR,
          TotalDeposit = keyAndValue.value.TotalDeposit,
          TotalDepositEUR = keyAndValue.value.TotalDepositEUR
        )

        val ldJson = printer.print(
          LifetimeDeposit.lifetimeDepositSQSEncoder.apply(ld)
        )

        val line =
          s"""{"id":"${id}","properties":${ldJson}}\n"""
        writer.write(line)
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

  def expose(prefix: String, timestamp: Long, file: Path): Unit = {
    val client = new BlobServiceClientBuilder()
      .connectionString(config.connectionString)
      .buildClient()
    val blobClient = client
      .getBlobContainerClient(config.outputContainerName)
      .getBlobClient(
        s"data/${prefix}/lifetimeDepositCount/lifetimeDepositCount-${UUID.randomUUID().toString()}.json"
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
    storeWallet = processorContext.getStateStore(walletStore)
    processorContext.schedule(
      config.punctuator,
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }
}

case class LifetimeDeposit(
    last_deposit_date: Option[String] = None,
    lifetimeDepositCount: Option[Int] = None,
    averageDeposit: Option[Double] = None,
    LifetimeWithdrawalCount: Option[String],
    LifetimeWithdrawalCountEUR: Option[String],
    TotalWithdrawalLifetime: Option[String],
    TotalWithdrawalLifetimeEUR: Option[String],
    TotalDeposit: Option[String],
    TotalDepositEUR: Option[String]
)

object LifetimeDeposit {

  import io.circe.Encoder
  import io.circe.generic.semiauto._
  implicit val lifetimeDepositSQSEncoder: Encoder[LifetimeDeposit] =
    deriveEncoder
}
