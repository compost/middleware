package com.jada.processor

import com.jada.configuration.ApplicationConfiguration
import com.jada.models._
import io.circe._
import io.circe.generic.auto._
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{
  ProcessorContext,
  PunctuationType,
  Punctuator
}
import org.apache.kafka.streams.state.KeyValueStore
import org.jboss.logging.Logger

import java.io.FileWriter
import java.nio.file.Files
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try
import java.nio.file.Path
import java.util.UUID
import java.time.Duration

class SelfExclusionPunctuator(
    config: ApplicationConfiguration,
    client: software.amazon.awssdk.services.sqs.SqsClient,
    playerStore: KeyValueStore[String, PlayerStore]
) extends Punctuator {
  private final val logger =
    Logger.getLogger(classOf[SelfExclusionPunctuator])

  val UserBlocked = "USER_BLOCKED"
  val sender = new Sender(config, client)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  override def punctuate(l: Long): Unit = {
    sendSelfExcludedOrTimeoutEvents(l)
  }

  def sendSelfExcludedOrTimeoutEvents(l: Long): Unit = {
    logger.info("sendSelfExcludedOrTimeoutEvents")
    val playersInStore = playerStore.all()
    try {

      while (playersInStore.hasNext()) {
        val keyAndValue = playersInStore.next()

        val player = keyAndValue.value
        if (
          keyAndValue.value.nb_player_send_self_exclusion.getOrElse(
            0
          ) > keyAndValue.value.nb_player_send_self_exclusion_done.getOrElse(
            0
          )
        ) {
          val body =
            s"""{"type":"${UserBlocked}",
               |"mappingSelector":"${sender.computeMappingSelector(
                player.brand_id,
                "self_exclusion"
              )}",
               |"contactId":"${player.player_id.get}",
               |"blockedUntil": "${player.is_self_excluded_until
                .getOrElse("")
                .replace(" ", "T")}",
               |"properties": {
               |  "is_self_excluded":"true"
               |}
               |}""".stripMargin
          sender.sendMessageToSQS(
            player.player_id.get,
            player.brand_id.get,
            body
          )
        }
        if (
          keyAndValue.value.nb_player_send_timeout.getOrElse(
            0
          ) > keyAndValue.value.nb_player_send_timeout_done.getOrElse(0)
        ) {
          val body =
            s"""{"type":"${UserBlocked}",
               |"mappingSelector":"${sender.computeMappingSelector(
                player.brand_id,
                "timeout"
              )}",
               |"contactId":"${player.player_id.get}",
               |"blockedUntil": "${player.is_timeout_until
                .getOrElse("")
                .replace(" ", "T")}",
               |"properties": {
               |  "is_timeout":"true"
               |}
               |}""".stripMargin
          sender.sendMessageToSQS(
            player.player_id.get,
            player.brand_id.get,
            body
          )
        }
        playerStore.delete(keyAndValue.key)
      }
    } finally {
      playersInStore.close()
    }
  }

}
class MappingTransformer(
    config: ApplicationConfiguration,
    client: software.amazon.awssdk.services.sqs.SqsClient,
    playerStoreName: String,
    playerSelfStoreName: String
) extends Transformer[String, PlayerStore, KeyValue[String, PlayerStore]]
    with Punctuator {

  val sender = new Sender(config, client)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )

  private final val logger =
    Logger.getLogger(classOf[MappingTransformer])
  private var processorContext: ProcessorContext = _
  private var playerStore: KeyValueStore[String, PlayerStore] = _
  private var playerSelfStore: KeyValueStore[String, PlayerStore] = _

  override def init(processorContext: ProcessorContext): Unit = {
    logger.info(f"${config}")
    this.processorContext = processorContext
    this.playerStore = processorContext.getStateStore(playerStoreName)
    this.playerSelfStore = processorContext.getStateStore(playerSelfStoreName)
    this.processorContext.schedule(
      config.punctuator,
      PunctuationType.WALL_CLOCK_TIME,
      this
    )
    this.processorContext.schedule(
      Duration.ofMinutes(5),
      PunctuationType.WALL_CLOCK_TIME,
      new SelfExclusionPunctuator(config, client, playerSelfStore)
    )
  }

  def deserialize[T: Decoder](data: Array[Byte]): T = {
    parser.decode[T](new String(data)).right.get
  }

  override def transform(
      key: String,
      v: PlayerStore
  ): KeyValue[String, PlayerStore] = {
    val previous = playerStore.get(key)
    val (self: Boolean, current: PlayerStore, sp: PlayerStore) =
      if (previous == null) {
        val process =
          v.nb_player_send_self_exclusion.isDefined || v.nb_player_send_timeout.isDefined
        (
          process,
          v.copy(
            nb_player_send_self_exclusion_done =
              v.nb_player_send_self_exclusion,
            nb_player_send_timeout_done = v.nb_player_send_timeout
          ),
          v
        )
      } else {
        val process = v.nb_player_send_timeout.getOrElse(
          0
        ) > previous.nb_player_send_timeout_done.getOrElse(0) ||
          v.nb_player_send_self_exclusion.getOrElse(
            0
          ) > previous.nb_player_send_self_exclusion_done.getOrElse(0)

        (
          process,
          v.copy(
            nb_player_send_self_exclusion_done =
              v.nb_player_send_self_exclusion,
            nb_player_send_timeout_done = v.nb_player_send_timeout
          ),
          v.copy(
            nb_player_send_self_exclusion_done =
              previous.nb_player_send_self_exclusion_done,
            nb_player_send_timeout_done = previous.nb_player_send_timeout_done
          )
        )
      }
    if (self) {
      playerSelfStore.put(key, sp)
    }
    playerStore.put(key, current)
    new KeyValue(key, current)
  }

  override def punctuate(l: Long): Unit = {
    sendBatchPlayerToSQSAsFile(l)
  }

  override def close() = {}

  def sendBatchPlayerToSQSAsFile(
      timestamp: Long
  ): Unit = {

    val playersInStore = playerStore.all()
    logger.infov("start - write file")

    val brandFile = scala.collection.mutable.Map[String, (Path, FileWriter)]()
    try {

      while (playersInStore.hasNext()) {
        val keyAndValue = playersInStore.next()

        if (keyAndValue.value.player_batch_to_send) {

          val id = keyAndValue.value.player_id.get
          val batchplayerJson = printer.print(
            PlayerBatchJsonSQS.batchPlayerSQSEncoder.apply(
              PlayerBatchJsonSQS(keyAndValue.value)
            )
          )

          val filewrite = brandFile.getOrElseUpdate(
            keyAndValue.value.brand_id.get, {
              val now = UUID.randomUUID.toString
              val tmp = Files.createTempFile(
                s"players-${now}-${keyAndValue.value.brand_id}",
                ".json"
              )
              logger.info(s"file: ${tmp}")
              val writer = new FileWriter(tmp.toFile())
              (tmp, writer)
            }
          )
          val line =
            s"""{"id":"$id","properties":$batchplayerJson}\n"""
          filewrite._2.write(line)
          val newPlayer = keyAndValue.value.copy(player_batch_to_send = false)
          playerStore.put(keyAndValue.key, newPlayer)
        }
      }
    } finally {
      logger.infov(
        s"end - write file",
        Array(
          kv("brandFile", brandFile)
        ): _*
      )

      brandFile.foreach { case (key, value) => value._2.close() }
      playersInStore.close()
    }

    brandFile.foreach {
      case (key, value) => {
        logger.info(s"expose ${key}")
        sender.expose(key, value._1)
      }
    }

  }

}

object MappingTransformer {

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
    if (date.length == 23) {
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

}
