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
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.w3c.dom.Node

import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import org.jboss.logging.Logger
import com.leovegas.model._
import com.leovegas.serdes._
import io.circe.Printer

class LeovegasEventTransformer(
    config: ApplicationConfiguration,
    client: software.amazon.awssdk.services.sqs.SqsClient,
    storeName: String
) extends Transformer[String, Array[Byte], KeyValue[Unit, Unit]] {

  private var processorContext: ProcessorContext = _
  private var dataBetStore: KeyValueStore[String, DataBet] = _
  private var eventStore: KeyValueStore[String, ValueAndTimestamp[Event]] = _
  private var marketStore: KeyValueStore[String, ValueAndTimestamp[Market]] = _

  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  val Vertical = "2"

  private final val logger =
    Logger.getLogger(classOf[LeovegasEventTransformer])

  override def init(processorContext: ProcessorContext): Unit = {
    this.processorContext = processorContext
    this.dataBetStore = processorContext.getStateStore(storeName)
    this.eventStore = processorContext.getStateStore(Common.eventStoreName)
    this.marketStore = processorContext.getStateStore(Common.marketStoreName)
  }

  def deserializeWagering(data: Array[Byte]): Wagering = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[Wagering](new String(data)).right.get
  }

  def deserializeSportsBet(data: Array[Byte]): SportsBets = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[SportsBets](new String(data)).right.get
  }

  override def transform(
      k: String,
      v: Array[Byte]
  ): KeyValue[Unit, Unit] = {

    val (key: String, current: Option[DataBet]) = getValue(k, v)

    val toStore = current match {
      case Some(
            DataBet(Some(sb: SportsBets), None, false, Some(false))
          ) => {
        if (
          sb.championship_id
            .map(v => config.championships.keySet.contains(v))
            .getOrElse(false)
        ) {

          val playerId = sb.player_id.get
          val market = Option(marketStore.get(sb.market_id.get))
          val event = Option(eventStore.get(sb.event_id.get))

          val b = new Body(
            `type` = "GENERIC_USER",
            mappingSelector = config.championships.get(sb.championship_id.get),
            contactId = playerId,
            properties = new Properties(
              bet_id = sb.bet_id.get,
              market_description = market
                .map(_.value().market_description)
                .map(_.get)
                .getOrElse(""),
              event_description = event
                .map(_.value().event_description)
                .map(_.get)
                .getOrElse(""),
              contains_live_bet = sb.is_live
                .map(_.toBoolean)
                .getOrElse(false)
                .toString,
              bet_won_lost = None,
              result_datetime = None 
            )
          )

          sendMessageToSQS(printer.print(Body.b.apply(b)), sb)
          Some(
            DataBet(
              sportsbet = Some(sb),
              wagering = None,
              false,
              Some(true)
            )
          )
        } else {
          current
        }
      }
      case Some(
            DataBet(Some(sb: SportsBets), Some(wg: Wagering), false, _)
          ) => {
        if (
          sb.championship_id
            .map(v => config.championships.keySet.contains(v))
            .getOrElse(false)
        ) {
          if (
            wg.vertical_id.contains(Vertical) && toBoolean(
              wg.has_resulted
            ) && wg.result_datetime.isDefined
          ) {
            val playerId = sb.player_id.get
            val lostWon = if (toBoolean(wg.result_id)) {
              "lost"
            } else {
              "won"
            }
            val market = Option(marketStore.get(sb.market_id.get))
            val event = Option(eventStore.get(sb.event_id.get))

            val b = new Body(
              `type` = "GENERIC_USER",
              mappingSelector =
                config.championships.get(sb.championship_id.get),
              contactId = playerId,
              properties = new Properties(
                bet_id = sb.bet_id.get,
                market_description = market
                  .map(_.value().market_description)
                  .map(_.get)
                  .getOrElse(""),
                event_description = event
                  .map(_.value().event_description)
                  .map(_.get)
                  .getOrElse(""),
                contains_live_bet = sb.is_live
                  .map(_.toBoolean)
                  .getOrElse(false)
                  .toString,
                bet_won_lost = Option(lostWon),
                result_datetime = wg.result_datetime
                  .map(s =>
                    if (s.length > 10) { s.substring(0, 10) }
                    else { s }
                  )
              )
            )

            sendMessageToSQS(printer.print(Body.b.apply(b)), sb)

            Some(
              DataBet(
                sportsbet = Some(sb),
                wagering = Some(wg),
                true,
                Some(true)
              )
            )
          } else if (toBoolean(wg.has_resulted)) {
            None
          } else {
            current
          }
        } else {
          if (toBoolean(wg.has_resulted)) {
            None
          } else {
            current
          }
        }
      }
      case _ => current

    }
    toStore match {
      case Some(data) => dataBetStore.put(key, data)
      case None       => dataBetStore.delete(key)
    }
    null
  }

  def sportsbets(
      previous: Option[SportsBets],
      value: SportsBets
  ): Option[SportsBets] = {
    previous match {
      case None => Option(value)
      case Some(p) =>
        Some(
          p.copy(
            bet_id = value.bet_id.orElse(p.bet_id),
            championship_id = value.championship_id.orElse(p.championship_id),
            market_id = value.market_id.orElse(p.market_id),
            player_id = value.player_id.orElse(p.player_id),
            event_id = value.event_id.orElse(p.event_id),
            is_live = value.is_live.orElse(p.is_live),
            brand_id = value.brand_id.orElse(p.brand_id),
            country_id = value.country_id.orElse(p.country_id),
            country = value.country.orElse(p.country),
            licenseuid = value.licenseuid.orElse(p.licenseuid),
            operator = value.operator.orElse(p.operator)
          )
        )

    }
  }

  def wagering(
      previous: Option[Wagering],
      value: Wagering
  ): Option[Wagering] = {
    previous match {
      case None => Option(value)
      case Some(p) =>
        Some(
          p.copy(
            bet_id = value.bet_id.orElse(p.bet_id),
            player_id = value.player_id.orElse(p.player_id),
            has_resulted = value.has_resulted.orElse(p.has_resulted),
            result_id = value.result_id.orElse(p.result_id),
            result_datetime = value.result_datetime.orElse(p.result_datetime),
            vertical_id = value.vertical_id.orElse(p.vertical_id),
            brand_id = value.brand_id.orElse(p.brand_id),
            country_id = value.country_id.orElse(p.country_id),
            country = value.country.orElse(p.country),
            licenseuid = value.licenseuid.orElse(p.licenseuid),
            operator = value.operator.orElse(p.operator)
          )
        )

    }
  }
  def getValue(k: String, v: Array[Byte]): (String, Option[DataBet]) = {
    processorContext.topic() match {
      case Common.sportsbetTopicRepartitioned =>
        val sb = Option(deserializeSportsBet(v))
        val keyBet = s"${k}-${sb.get.bet_id.get}"
        val previous: Option[DataBet] =
          Option[DataBet](dataBetStore.get(keyBet))
        (
          keyBet,
          previous match {
            case Some(d) =>
              Some[DataBet](d.copy(sportsbet = sportsbets(d.sportsbet, sb.get)))
            case None => Some[DataBet](DataBet(sportsbet = sb))
          }
        )
      case Common.wageringTopicRepartitioned =>
        val wg = Option(deserializeWagering(v))
        val keyBet = s"${k}-${wg.get.bet_id.get}"
        if (wg.get.vertical_id.contains(Vertical)) {
          val previous: Option[DataBet] =
            Option[DataBet](dataBetStore.get(keyBet))
          (
            keyBet,
            previous match {
              case Some(d) =>
                Some[DataBet](d.copy(wagering = wagering(d.wagering, wg.get)))
              case None => Some[DataBet](DataBet(wagering = wg))
            }
          )
        } else {
          (keyBet, None)
        }
      case _ => throw new IllegalStateException("should know the topic")
    }
  }

  def getSQS(ps: SportsBets): String = {
    (
      ps.licenseuid,
      ps.country,
      ps.operator
    ) match {
      case (_, Some(country), _)
          if Set("BR").contains(country.toUpperCase()) =>
        config.sqsQueueBR
      case (Some(v), _, _) if v.toUpperCase() == "IOM" =>
        config.sqsQueueLicenseuid
      case (_, Some(country), _)
          if Set("UK", "GB").contains(country.toUpperCase()) =>
        config.sqsQueueUK
      case (_, Some(country), Some(operator))
          if operator.toUpperCase() == "MGM" && Set("NL").contains(
            country.toUpperCase()
          ) =>
        config.sqsQueueOperatorNL

      case (_, _, _) => config.sqsQueue
    }
  }
  def toBoolean(v: Option[String]): Boolean = {
    v match {
      case Some("true")  => true
      case Some("false") => false
      case Some("0")     => false
      case Some("1")     => true
      case _             => false
    }
  }

  override def close(): Unit = {}

  def sendMessageToSQS(
      body: String,
      sportsbets: SportsBets
  ): Unit = {
    val playerId = sportsbets.player_id.get

    val queue = getSQS(sportsbets)
    logger.infov(
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

}

case class Body(
    `type`: String,
    contactId: String,
    mappingSelector: String,
    properties: Properties
)

case class Properties(
    bet_id: String,
    market_description: String,
    event_description: String,
    contains_live_bet: String,
    bet_won_lost: Option[String],
    result_datetime: Option[String]
)

import io.circe.Encoder
import io.circe.generic.semiauto._

object Body {
  implicit val p: Encoder[Properties] = deriveEncoder
  implicit val b: Encoder[Body] = deriveEncoder
}
