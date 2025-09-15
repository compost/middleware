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
import java.util.UUID

object Sender {

  private final val logger =
    Logger.getLogger(classOf[Sender])

  val MX = Set("233", "289")
  val Spin247 = Set("150", "249")

  val Funid = Set("214", "321")
  val IBet = Set("216", "244", "226", "41", "286")
  val FP = Set(
    "229",
    "230",
    "236",
    "237",
    "253",
    "258",
    "272",
    "273",
    "282",
    "285",
    "300",
    "311",
    "320"
  )
  // val NB = Set("274")
  val Elabet = Set("215")
  val Sga = Set("56", "76", "77", "129", "314")
  val Dk = Set("57", "213")
  val Mga =
    Set("53", "54", "55", "64", "65", "66", "67", "109", "111", "259")
  val Other =
    Set(
      "81",
      "43",
      "2",
      "34",
      "47",
      "52",
      "37",
      "85",
      "18",
      "27",
      "45",
      "39",
      "48",
      "5",
      "29",
      "28",
      "87",
      "32",
      "26",
      "36",
      "46",
      "84",
      "33",
      "16",
      "31",
      "19",
      "42",
      "35",
      "20",
      "38",
      "40",
      "50",
      "51",
      "25",
      "49",
      "17",
      "6",
      "44",
      "78",
      "107",
      "30",
      "108",
      "122",
      "125",
      "63",
      "128",
      "134",
      "147",
      "148",
      "149",
      "151",
      "152",
      "181",
      "183",
      "80",
      "184",
      "185",
      "196",
      "197",
      "199",
      "200",
      "201",
      "202",
      "203",
      "204",
      "217",
      "218",
      "219",
      "220",
      "221",
      "222",
      "224",
      "225",
      "227",
      "228",
      "234",
      "235",
      "240",
      "243",
      "245",
      "246",
      "247",
      "248",
      "250",
      "254",
      "256",
      "257",
      "261",
      "262",
      "267",
      "268",
      "269",
      "270",
      "271",
      "232",
      "280",
      "281",
      "283",
      "284",
      "287",
      "288",
      "293",
      "298",
      "299",
      "302",
      "303",
      "304",
      "305",
      "306",
      "307",
      "110",
      "198",
      "310",
      "260",
      "316",
      "317",
      "318",
      "319",
      "322",
      "323"
    )
  val Boomerang = Set("123", "124", "153", "133", "223", "308")
  val CasinoInfinity: Set[String] = Set("290", "301")
  val RO = Set("86", "135", "315")
  val RO2 = Set("309")
  val CA = Set("182")
  val Cp = Set("252")
  val Ds: Set[String] = Set()
  val Sq = Set("255", "274")

  val SgaPrefix = "sga"
  val DkPrefix = "dk"
  val DsPrefix = "ds"
  val MgaPrefix = "mga"
  val BoomerangPrefix = "boomerang"
  val CasinoInfinityPrefix = "casino"
  val RO2Prefix = "ro2"
  val ROPrefix = "ro"
  val OtherPrefix = "other"
  val CAPrefix = "ca"
  val FPPrefix = "fp"
  // val NBPrefix = "nb"
  val FunidPrefix = "funid"
  val ElabetPrefix = "elabet"
  val MXPrefix = "mx"
  val Spin247Prefix = "spin247"
  val CpPrefix = "cp"
  val SqPrefix = "sq"
  val IBetPrefix = "ibet"

  val Brands = Set(
    SgaPrefix,
    DkPrefix,
    DsPrefix,
    MgaPrefix,
    BoomerangPrefix,
    CasinoInfinityPrefix,
    RO2Prefix,
    ROPrefix,
    OtherPrefix,
    CAPrefix,
    FPPrefix,
    // NBPrefix,
    FunidPrefix,
    ElabetPrefix,
    MXPrefix,
    Spin247Prefix,
    CpPrefix,
    SqPrefix,
    IBetPrefix
  )
  def prefix(brandID: String): String = {
    if (Sender.Sga.contains(brandID)) {
      SgaPrefix
    } else if (Sender.Dk.contains(brandID)) {
      DkPrefix
    } else if (Sender.Ds.contains(brandID)) {
      DsPrefix
    } else if (Sender.Mga.contains(brandID)) {
      MgaPrefix
    } else if (Sender.Boomerang.contains(brandID)) {
      BoomerangPrefix
    } else if (Sender.CasinoInfinity.contains(brandID)) {
      CasinoInfinityPrefix
    } else if (Sender.RO2.contains(brandID)) {
      RO2Prefix
    } else if (Sender.RO.contains(brandID)) {
      ROPrefix
    } else if (Sender.Other.contains(brandID)) {
      OtherPrefix
    } else if (Sender.CA.contains(brandID)) {
      CAPrefix
    } else if (Sender.FP.contains(brandID)) {
      FPPrefix
      // } else if (Sender.NB.contains(brandID)) {
      // NBPrefix
    } else if (Sender.Funid.contains(brandID)) {
      FunidPrefix
    } else if (Sender.Elabet.contains(brandID)) {
      ElabetPrefix
    } else if (Sender.MX.contains(brandID)) {
      MXPrefix
    } else if (Sender.Spin247.contains(brandID)) {
      Spin247Prefix
    } else if (Sender.Cp.contains(brandID)) {
      CpPrefix
    } else if (Sender.Sq.contains(brandID)) {
      SqPrefix
    } else if (Sender.IBet.contains(brandID)) {
      IBetPrefix
    } else {
      logger.error(s"${brandID} missing in the configuration")
      "nothandled"
    }
  }
}
class Sender(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    both: Boolean = false,
    handleFunid: Boolean = false
) {

  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[Sender])

  def sendPlayer(
      topic: Option[String],
      previous: Option[PlayerStore],
      playerToSend: PlayerStore
  ): Unit = {
    if (
      playerToSend != null && playerToSend.brand_id.isDefined && playerToSend.player_id.isDefined
    ) {

      logger.debugv(
        "sendPlayer",
        Array(
          kv("topic", topic),
          kv("player_id", playerToSend.player_id.get)
        ): _*
      )

      val playerToSQS = PlayerStoreSQS(playerToSend)
      val hasChanged = previous
        .map(p => PlayerStoreSQS(p))
        .map(p => playerToSQS != p)
        .getOrElse(true)

      val (force, deduplicationId, mappingSelector) =
        if (topic.getOrElse("") == Common.loginRepartitionedTopic) {
          val uuid = if (!hasChanged && config.force) {
            UUID
              .nameUUIDFromBytes(
                s"${playerToSend.brand_id
                    .getOrElse("")}-${playerToSend.player_id.getOrElse("")}"
                  .getBytes()
              )
              .toString()
          } else {
            UUID.randomUUID().toString()
          }
          (
            config.force,
            uuid,
            "player_login"
          )
        } else {
          (false, UUID.randomUUID().toString(), "player_updated")
        }
      if (
        hasChanged
        || config.force
      ) {

        val body = Body[PlayerStoreSQS](
          "GENERIC_USER",
          playerToSend.player_id.get,
          mappingSelector,
          playerToSQS
        )

        sendToSQS(
          printer.print(Body.bodyEncoder[PlayerStoreSQS].apply(body)),
          playerToSend.player_id.get,
          playerToSend.brand_id.get,
          deduplicationId
        )
      }
    }
  }

  def sendToSQSByPrefix(
      body: String,
      key: String,
      prefix: String
  ): Unit = {
    logger.debugv(
      "sendToSQS",
      Array(kv("key", key), kv("body", body), kv("prefix", prefix)): _*
    )
    val (cli, queueURL) = if (Sender.SgaPrefix == prefix) {
      (ueNorthSQS, config.sqsQueueSGA)
    } else if (Sender.DkPrefix == prefix) {
      (ueNorthSQS, config.sqsQueueDK)
    } else if (Sender.DsPrefix == prefix) {
      (sqs, config.sqsQueueDS)
    } else if (Sender.MgaPrefix == prefix) {
      (ueNorthSQS, config.sqsQueueMGA)
    } else if (Sender.BoomerangPrefix == prefix) {
      (ueNorthSQS, config.sqsQueueBoomerang)
    } else if (Sender.CasinoInfinityPrefix == prefix) {
      (sqs, config.sqsQueueCasinoInfinity)
    } else if (Sender.RO2Prefix == prefix) {
      (ueNorthSQS, config.sqsQueueRO2)
    } else if (Sender.ROPrefix == prefix) {
      (ueNorthSQS, config.sqsQueueRO)
    } else if (Sender.OtherPrefix == prefix) {
      (sqs, config.sqsQueue)
    } else if (Sender.CAPrefix == prefix) {
      (sqs, config.sqsQueueCA)
    } else if (Sender.FPPrefix == prefix) {
      (sqs, config.sqsQueueFP)
      // } else if (Sender.NBPrefix == prefix) {
      //  (sqs, config.sqsQueueNB)
    } else if (Sender.FunidPrefix == prefix) {
      (sqs, config.sqsQueueFunid)
    } else if (Sender.ElabetPrefix == prefix) {
      (sqs, config.sqsQueueElabet)
    } else if (Sender.MXPrefix == prefix) {
      (sqs, config.sqsQueueMX)
    } else if (Sender.Spin247Prefix == prefix) {
      (sqs, config.sqsQueueSpin247)
    } else if (Sender.CpPrefix == prefix) {
      (sqs, config.sqsQueueCp)
    } else if (Sender.SqPrefix == prefix) {
      (sqs, config.sqsQueueSq)
    } else if (Sender.IBetPrefix == prefix) {
      (sqs, config.sqsQueueIBet)

    } else {
      logger.error(s"new brand not handled ")
      (null, null)
    }
    if (cli != null) {
      if (
        both ||
        (Sender.FunidPrefix == prefix && handleFunid) || (!(Sender.FunidPrefix == prefix) && !handleFunid)
      ) {
        send(body, key, cli, queueURL)
      }
    }
  }
  def sendToSQS(
      body: String,
      key: String,
      brandID: String,
      deduplicationId: String = s"${UUID.randomUUID().toString()}"
  ): Unit = {
    logger.debugv(
      "sendToSQS",
      Array(kv("key", key), kv("body", body), kv("brandID", brandID)): _*
    )

    val (cli, queueURL) = if (Sender.Sga.contains(brandID)) {
      (ueNorthSQS, config.sqsQueueSGA)
    } else if (Sender.Dk.contains(brandID)) {
      (ueNorthSQS, config.sqsQueueDK)
    } else if (Sender.Ds.contains(brandID)) {
      (sqs, config.sqsQueueDS)
    } else if (Sender.Mga.contains(brandID)) {
      (ueNorthSQS, config.sqsQueueMGA)
    } else if (Sender.Boomerang.contains(brandID)) {
      (ueNorthSQS, config.sqsQueueBoomerang)
    } else if (Sender.CasinoInfinity.contains(brandID)) {
      (sqs, config.sqsQueueCasinoInfinity)
    } else if (Sender.RO2.contains(brandID)) {
      (ueNorthSQS, config.sqsQueueRO2)
    } else if (Sender.RO.contains(brandID)) {
      (ueNorthSQS, config.sqsQueueRO)
    } else if (Sender.Other.contains(brandID)) {
      (sqs, config.sqsQueue)
    } else if (Sender.CA.contains(brandID)) {
      (sqs, config.sqsQueueCA)
    } else if (Sender.FP.contains(brandID)) {
      (sqs, config.sqsQueueFP)
    } else if (Sender.Funid.contains(brandID)) {
      (sqs, config.sqsQueueFunid)
    } else if (Sender.Elabet.contains(brandID)) {
      (sqs, config.sqsQueueElabet)
    } else if (Sender.Spin247.contains(brandID)) {
      (sqs, config.sqsQueueSpin247)
    } else if (Sender.Cp.contains(brandID)) {
      (sqs, config.sqsQueueCp)
    } else if (Sender.Sq.contains(brandID)) {
      (sqs, config.sqsQueueSq)
    } else if (Sender.MX.contains(brandID)) {
      (sqs, config.sqsQueueMX)
    } else if (Sender.IBet.contains(brandID)) {
      (sqs, config.sqsQueueIBet)
    } else { (null, null) }

    if (cli != null) {
      if (
        both ||
        (Sender.Funid.contains(brandID) && handleFunid) || (!Sender.Funid
          .contains(brandID) && !handleFunid)
      ) {
        logger.debugv(
          "sendToSQSWithQueue",
          Array(
            kv("key", key),
            kv("body", body),
            kv("brandID", brandID),
            kv("queue", queueURL)
          ): _*
        )
        send(body, key, cli, queueURL, deduplicationId)
      }
    } else {
      logger.debug(s"$brandID brand not handled ")
    }

  }

  def send(
      body: String,
      key: String,
      cli: software.amazon.awssdk.services.sqs.SqsClient,
      queueURL: String,
      deduplicationId: String = s"${UUID.randomUUID().toString()}"
  ): Unit = {

    logger.debugv(
      s"send",
      Array(
        kv("queueURL", queueURL),
        kv("body", body),
        kv("key", key)
      ): _*
    )
    try {

      if (config.dryRun) {
        logger.infov(
          s"dry run sent queue message",
          Array(
            kv("queueURL", queueURL),
            kv("body", body),
            kv("key", key)
          ): _*
        )

      } else {
        Option(cli.sendMessage(request => {
          request
            .queueUrl(queueURL)
            .messageBody(body)
            .messageGroupId(config.sqsGroupId)
            .messageDeduplicationId(deduplicationId)

        })) foreach { result =>
          logger.infov(
            s"sent queue message: $result",
            Array(
              kv("queueURL", queueURL),
              kv("body", body),
              kv("key", key)
            ): _*
          )
        }
      }
    } catch {
      case e: Exception =>
        logger.errorv(
          s"wasn't able to send queue message: for $key. Reason $e",
          Array(
            kv("queueURL", queueURL),
            kv("body", body),
            kv("key", key)
          ): _*
        )
        throw e
    }
  }
}
