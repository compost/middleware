package com.soft2bet.processor.players

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.soft2bet.Common
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.jboss.logging.Logger
import io.circe.syntax._
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import io.circe.Encoder
import io.circe.syntax._
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import scala.util.{Failure, Success, Try}
import com.soft2bet.model.PlayerStoreSQS
import com.soft2bet.model.Verification._
import com.jada.sqs.Body
import io.circe.Printer
import com.soft2bet.model.BonusUpdatedSQS
import com.soft2bet.model.Wagering
import com.soft2bet.model.WageringJSON
import com.soft2bet.model.keepYYYYMMDD
import com.soft2bet.model.formatYYYYMMDD
import com.soft2bet.model.WithdrawalRequestSQS
import java.util.UUID
import com.soft2bet.model.VerificationSQS
import com.soft2bet.model.VipLevel
import com.soft2bet.model.VipLevelSQS
import com.soft2bet.model.VipLevel25ChangedSQS

class VipLevelProcessor(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    storeName: String
) extends Transformer[String, VipLevel, KeyValue[String, VipLevel]] {
  private var processorContext: ProcessorContext = _

  private var store: KeyValueStore[String, VipLevel] = _
  private val sender = new Sender(config, sqs, ueNorthSQS, false, false)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[VipLevelProcessor])

  override def init(processorContext: ProcessorContext): Unit = {
    this.processorContext = processorContext
    this.store = processorContext.getStateStore(storeName)
  }

  override def transform(
      k: String,
      value: VipLevel
  ): KeyValue[String, VipLevel] = {

    val previous = Option(store.get(k))

    val newValue = previous match {
      case None => value
      case Some(toUpdate) =>
        toUpdate.copy(
          VIPLevel25 = value.VIPLevel25,
          VIPLevelIncrease = value.VIPLevelIncrease,
          CurrentVIPLevelStartDate = value.CurrentVIPLevelStartDate,
          VIP25 = value.VIP25,
          PointsToNextVIPLevel = value.PointsToNextVIPLevel,
          PointsRequiredToMaintainVIPLevel =
            value.PointsRequiredToMaintainVIPLevel,
          SafeVIPLevel = value.SafeVIPLevel,
          VIPMaxLevel25 = value.VIPMaxLevel25,
          LoyaltyProgramType = value.LoyaltyProgramType
        )
    }
    store.put(k, newValue)

    val body = Body[VipLevelSQS](
      "GENERIC_USER",
      value.player_id.get,
      "vip_update",
      VipLevelSQS(newValue)
    )
    sender.sendToSQS(
      printer.print(
        Body.bodyEncoder[VipLevelSQS].apply(body)
      ),
      value.player_id.get,
      value.brand_id.get
    )
    if (
      value.VIPLevel25.isDefined &&
      (previous.isEmpty || previous.get.VIPLevel25.isEmpty || previous.get.VIPLevel25 != value.VIPLevel25)
    ) {

      val body = Body[VipLevel25ChangedSQS](
        "GENERIC_USER",
        value.player_id.get,
        "viplevel25_changed",
        VipLevel25ChangedSQS(newValue)
      )
      sender.sendToSQS(
        printer.print(
          Body.bodyEncoder[VipLevel25ChangedSQS].apply(body)
        ),
        value.player_id.get,
        value.brand_id.get
      )

    }

    new KeyValue(k, value)
  }

  override def close(): Unit = {}
}
