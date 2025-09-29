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
import com.soft2bet.model.PlayerSegmentation
import com.soft2bet.model.PlayerSegmentationSQS
import java.time.ZonedDateTime

class PlayerSegmentationPunctuatorTransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    playerStore: String
) extends Transformer[
      String,
      PlayerSegmentation,
      KeyValue[String, PlayerSegmentation]
    ]
    with Punctuator {

  private var processorContext: ProcessorContext = _
  private var storePlayers: KeyValueStore[String, PlayerSegmentation] = _
  private val sender = new Sender(config, sqs, ueNorthSQS, false, false)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[PlayerSegmentationPunctuatorTransformer])

  override def transform(
      key: String,
      value: PlayerSegmentation
  ): KeyValue[String, PlayerSegmentation] = {
    val stored = storePlayers.get(key)
    val playerToSave = Option(stored) match {
      case None => value
      case Some(inStore) => {
        inStore.copy(
          player_id = value.player_id.orElse(inStore.player_id),
          brand_id = value.brand_id.orElse(inStore.brand_id),
          RFM_APR_NEW_SEGMENT =
            value.RFM_APR_NEW_SEGMENT.orElse(inStore.RFM_APR_NEW_SEGMENT),
          RFM_APR_RUNDAY = value.RFM_APR_RUNDAY.orElse(inStore.RFM_APR_RUNDAY),
          RFM_APR_ACTIVITY =
            value.RFM_APR_ACTIVITY.orElse(inStore.RFM_APR_ACTIVITY),
          RFM_APR_VALUE = value.RFM_APR_VALUE.orElse(inStore.RFM_APR_VALUE),
          RFM_APR_MONETARY =
            value.RFM_APR_MONETARY.orElse(inStore.RFM_APR_MONETARY),
          RFM_APR_ORIENTATION =
            value.RFM_APR_ORIENTATION.orElse(inStore.RFM_APR_ORIENTATION),
          PROB = value.PROB.orElse(inStore.PROB),
          BUCKET1_HIGH_CHANCE_TO_CONVERT = value.BUCKET1_HIGH_CHANCE_TO_CONVERT
            .orElse(inStore.BUCKET1_HIGH_CHANCE_TO_CONVERT),
          BUCKET2_MEDIUM_CHANCE_TO_CONVERT =
            value.BUCKET2_MEDIUM_CHANCE_TO_CONVERT.orElse(
              inStore.BUCKET2_MEDIUM_CHANCE_TO_CONVERT
            ),
          BUCKET3_LOW_CHANCE_TO_CONVERT = value.BUCKET3_LOW_CHANCE_TO_CONVERT
            .orElse(inStore.BUCKET3_LOW_CHANCE_TO_CONVERT),
          PREDICTION_DAY = value.PREDICTION_DAY.orElse(inStore.PREDICTION_DAY),
          TrafficGroup = value.TrafficGroup.orElse(inStore.TrafficGroup),
          TrafficType = value.TrafficType.orElse(inStore.TrafficType),
          Sport_GGR_1d = value.Sport_GGR_1d.orElse(inStore.Sport_GGR_1d),
          Sport_TO_1d = value.Sport_TO_1d.orElse(inStore.Sport_TO_1d),
          Sport_Bonus_GGR_1d =
            value.Sport_Bonus_GGR_1d.orElse(inStore.Sport_Bonus_GGR_1d),
          Sport_GGR_7d = value.Sport_GGR_7d.orElse(inStore.Sport_GGR_7d),
          Sport_TO_7d = value.Sport_TO_7d.orElse(inStore.Sport_TO_7d),
          Sport_Bonus_GGR_7d =
            value.Sport_Bonus_GGR_7d.orElse(inStore.Sport_Bonus_GGR_7d),
          Sport_GGR_30d = value.Sport_GGR_30d.orElse(inStore.Sport_GGR_30d),
          Sport_TO_30d = value.Sport_TO_30d.orElse(inStore.Sport_TO_30d),
          Sport_Bonus_GGR_30d =
            value.Sport_Bonus_GGR_30d.orElse(inStore.Sport_Bonus_GGR_30d),
          ACCOUNT_IDT = value.ACCOUNT_IDT.orElse(inStore.ACCOUNT_IDT),
          ACTIVITY_SEGMENT =
            value.ACTIVITY_SEGMENT.orElse(inStore.ACTIVITY_SEGMENT),
          DEPOSIT_SEGMENT =
            value.DEPOSIT_SEGMENT.orElse(inStore.DEPOSIT_SEGMENT),
          NGR_SEGMENT = value.NGR_SEGMENT.orElse(inStore.NGR_SEGMENT),
          ACQUISITION_SEGMENT =
            value.ACQUISITION_SEGMENT.orElse(inStore.ACQUISITION_SEGMENT),
          LT_MODE_DEPOSITS =
            value.LT_MODE_DEPOSITS.orElse(inStore.LT_MODE_DEPOSITS),
          PRODUCT_SEGMENT =
            value.PRODUCT_SEGMENT.orElse(inStore.PRODUCT_SEGMENT),
          RECORD_UPDATED = value.RECORD_UPDATED.orElse(inStore.RECORD_UPDATED),
          DEPOSIT_SEGMENT_RANGE =
            value.DEPOSIT_SEGMENT_RANGE.orElse(inStore.DEPOSIT_SEGMENT_RANGE),
          DEPOSIT_SEGMENT_MODE_DIST = value.DEPOSIT_SEGMENT_MODE_DIST.orElse(
            inStore.DEPOSIT_SEGMENT_MODE_DIST
          ),
          NGR_SEGMENT_RANGE =
            value.NGR_SEGMENT_RANGE.orElse(inStore.NGR_SEGMENT_RANGE),
          LT_NGR_SEGMENT = value.LT_NGR_SEGMENT.orElse(inStore.LT_NGR_SEGMENT),
          LT_NGR_SEGMENT_RANGE =
            value.LT_NGR_SEGMENT_RANGE.orElse(inStore.LT_NGR_SEGMENT_RANGE),
          LT_PRODUCT_SEGMENT =
            value.LT_PRODUCT_SEGMENT.orElse(inStore.LT_PRODUCT_SEGMENT),
          RECORD_CREATED = value.RECORD_CREATED.orElse(inStore.RECORD_CREATED),
          UNIQUEID = value.UNIQUEID.orElse(inStore.UNIQUEID),
          NEXT_BEST_BRAND = value.NEXT_BEST_BRAND
            .orElse(inStore.NEXT_BEST_BRAND),
          FREE_BONUS_ABUSER = value.FREE_BONUS_ABUSER
            .orElse(inStore.FREE_BONUS_ABUSER),
          ZBR_brand_id = value.ZBR_brand_id.orElse(inStore.ZBR_brand_id),
          ZBR_player_id = value.ZBR_player_id.orElse(inStore.ZBR_player_id),
          ZBR_predition_date =
            value.ZBR_predition_date.orElse(inStore.ZBR_predition_date),
          ZBR_ltv_prediction =
            value.ZBR_ltv_prediction.orElse(inStore.ZBR_ltv_prediction),
          ZBR_ltv_ggr_top_prediction = value.ZBR_ltv_ggr_top_prediction.orElse(
            inStore.ZBR_ltv_ggr_top_prediction
          ),
          ZBR_ltv_ggr_prediction =
            value.ZBR_ltv_ggr_prediction.orElse(inStore.ZBR_ltv_ggr_prediction),
          ZBR_days_from_registration = value.ZBR_days_from_registration.orElse(
            inStore.ZBR_days_from_registration
          ),
          ZBR_current_activity_days = value.ZBR_current_activity_days.orElse(
            inStore.ZBR_current_activity_days
          ),
          ZBR_churn_prediction =
            value.ZBR_churn_prediction.orElse(inStore.ZBR_churn_prediction),
          cross_activity_status = value.cross_activity_status.orElse(inStore.cross_activity_status)
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
      players: KeyValueIterator[String, PlayerSegmentation]
  ): Path = {
    val tmp = Files.createTempFile(s"ps-${prefix}", ".json")
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

        val ldJson = printer.print(
          PlayerSegmentationSQS.playerSegmentationSQSEncoder.apply(
            PlayerSegmentationSQS(player)
          )
        )

        val line =
          s"""{"id":"${player.player_id.get}","properties":${ldJson}}\n"""
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
        s"data/${prefix}/players/segmentation-${timestamp}.json"
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
      Duration.ofHours(1),
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
  }
}
