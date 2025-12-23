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
  Verification,
  Wallet
}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.jboss.logging.Logger
import io.circe.syntax._
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._

import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv

import scala.util.{Failure, Success, Try}
import com.soft2bet.model._
import com.jada.sqs.Body
import io.circe.Printer

class FunidTransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    playerStoreName: String
) extends Transformer[String, Array[Byte], KeyValue[String, FunidPlayerStore]] {
  private var processorContext: ProcessorContext = _
  private var store: KeyValueStore[String, FunidPlayerStore] = _

  implicit val funidVerificationDecoder: Decoder[FunidVerification] =
    deriveDecoder
  implicit val funidWalletDecoder: Decoder[FunidWallet] = deriveDecoder
  implicit val funidPlayerDecoder: Decoder[FunidPlayer] = deriveDecoder
  private val sender = new Sender(
    config,
    sqs,
    ueNorthSQS,
    handleFunid = true
  )
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[PlayersTransformer])

  override def init(processorContext: ProcessorContext): Unit = {
    this.processorContext = processorContext
    this.store = getKVStore(playerStoreName)
  }

  def deserialize[T](
      data: Array[Byte]
  )(implicit decode: io.circe.Decoder[T]): T = {
    parser.decode[T](new String(data)).right.get
  }

  override def transform(
      k: String,
      v: Array[Byte]
  ): KeyValue[String, FunidPlayerStore] = {

    processorContext.topic() match {
      case Common.playersRepartitionedTopic =>
        val player =
          deserialize[FunidPlayer](v)(funidPlayerDecoder)
        val key = s"${player.brand_id.get}-${player.player_id.get}"
        val previous = Option(store.get(key))
        val newOne = FunidPlayerStore(previous, player)
        if (
          newOne.brand_id.isDefined && Sender.Funid.contains(
            newOne.brand_id.get
          )
        ) {
          store.put(key, newOne)
        } else {
          store.delete(key)
          return new KeyValue(k, null)
        }

        if (
          previous.map(FunidPlayerRegisteredSQS(_)) != FunidPlayerRegisteredSQS(
            newOne
          )
        ) {
          val body = Body[FunidPlayerRegisteredSQS](
            "GENERIC_USER",
            player.player_id.get,
            "player_registered",
            FunidPlayerRegisteredSQS(newOne)
          )
          sender.sendToSQS(
            printer.print(
              Body.bodyEncoder[FunidPlayerRegisteredSQS].apply(body)
            ),
            player.player_id.get,
            player.brand_id.get
          )
        }

        val body = Body[FunidPlayerLoginSQS](
          "GENERIC_USER",
          player.player_id.get,
          "player_login",
          FunidPlayerLoginSQS(newOne)
        )
        sender.sendToSQS(
          printer.print(Body.bodyEncoder[FunidPlayerLoginSQS].apply(body)),
          player.player_id.get,
          player.brand_id.get
        )
      case Common.verificationRepartitionedTopic =>
        val verification =
          deserialize[FunidVerification](v)(funidVerificationDecoder)

        val body = Body[FunidVerificationSQS](
          "GENERIC_USER",
          verification.player_id.get,
          "kyc",
          FunidVerificationSQS(verification)
        )
        sender.sendToSQS(
          printer.print(Body.bodyEncoder[FunidVerificationSQS].apply(body)),
          verification.player_id.get,
          verification.brand_id.get
        )
      case Common.walletRepartitionedTopic =>
        val wallet = deserialize[FunidWallet](v)(funidWalletDecoder)
        val mappingSelector = wallet.transaction_type_id match {
          case Some(v) if v.toLowerCase.startsWith("deposit") =>
            Some("deposit_event")
          case Some(v) if v.toLowerCase.startsWith("withdraw") =>
            Some("withdrawal_event")
          case _ => None
        }
        if (mappingSelector.isDefined) {
          val body = Body[FunidWalletSQS](
            "GENERIC_USER",
            wallet.player_id.get,
            mappingSelector.get,
            FunidWalletSQS(wallet)
          )
          sender.sendToSQS(
            printer.print(Body.bodyEncoder[FunidWalletSQS].apply(body)),
            wallet.player_id.get,
            wallet.brand_id.get
          )
        }

      case _ =>
        logger.debug(s"${processorContext.topic()} not handled for now")
    }
    new KeyValue(k, null)
  }

  def getKVStore[K, V](storeName: String): KeyValueStore[K, V] = {
    Try {
      processorContext
        .getStateStore(storeName)
        .asInstanceOf[KeyValueStore[K, V]]
    } match {
      case Success(kvStore) => kvStore
      case Failure(e: ClassCastException) =>
        throw new IllegalArgumentException(
          s"Please provide a KeyValueStore for $storeName, reason: $e"
        )
      case Failure(e) => throw e
    }
  }

  override def close(): Unit = {}

}
