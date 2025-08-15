package com.jada.processor

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.json.JsonSerializer
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state.Stores

import javax.enterprise.context.ApplicationScoped
import javax.enterprise.inject.Produces
import javax.inject.Inject
import org.apache.kafka.streams.scala.kstream.Repartitioned
import java.time.format.DateTimeFormatter
import java.time.OffsetDateTime
import java.time.ZoneOffset
import com.jada.configuration.ApplicationConfiguration
import org.apache.kafka.streams.scala.kstream.Produced
import java.time.Clock
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import scala.util.Try
import java.time.ZoneId
import java.util.UUID
import java.time.LocalDateTime

object Topologies {

  val DateTimeformatter0DigitWithoutT = DateTimeFormatter
    .ofPattern("yyyy-MM-dd' 'HH:mm:ss")
    .withZone(ZoneId.of("UTC"))

  def formatDatetime(date: String): Option[String] = {
    Try {
      val d = Try({
        val datetime =
          LocalDateTime.parse(date, DateTimeformatter0DigitWithoutT)
        datetime.atZone(ZoneId.of("UTC")).toOffsetDateTime()
      })
        .orElse(Try(OffsetDateTime.parse(date)))
        .getOrElse(throw new RuntimeException(s"${date} not unhandled"))
      d.withNano(0)
        .withOffsetSameInstant(ZoneOffset.UTC)
        .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    }.toOption
  }

  val PlayerId = "player_id"
  val SportId = "sport_id"
  val BetId = "bet_id"
  val SportDescription = "sport_description"
  val DescriptionId = "description_id"
  val Amount = "amount"
  val TransactionTypeId = "transaction_type_id"
  val TransactionStatusId = "transaction_status_id"
  val TransactionId = "transaction_id"
  val TransactionDatetime = "transaction_datetime"
  val TransactionType_Deposit = "deposit-"
  val TransactionStatus_Complete = "complete"
  val SportStore = "sport"
  val AggregatorWalletStore = "wallet"
  val AggregatorWageringStore = "wagering"
  val AggregatorSportsbetsStore = "sportsbets"
  val AggregatorWageringSportStore = "wagering-sport"
  val AggregatorCheckerStore = "checker"

  object WageringSport {
    val Wagering = "wagering"
    val BetDatetime = "bet_datetime"
    val BetId = "bet_id"
    val WagerAmountCash = "wager_amount_cash"
    val Sportsbets = "sportsbets"
  }
}

@ApplicationScoped
class Topologies(
    @Inject val configuration: ApplicationConfiguration,
    @Inject val clock: Clock,
    @Inject mapper: ObjectMapper
) {

  import Topologies.formatDatetime
  val jsonNodeSerializer = new JsonSerializer()
  val jsonNodeDeserializer = new JsonDeserializer()
  val jsonNodeSerde =
    Serdes.serdeFrom(jsonNodeSerializer, jsonNodeDeserializer)

  val stringSerde: Serde[String] = Serdes.String
  implicit val consumed = Consumed.`with`(stringSerde, jsonNodeSerde)
  implicit val produced = Produced.`with`(stringSerde, jsonNodeSerde)

  @Produces @RepartitionerTopology
  def buildRepartitionerTopology(): Option[Topology] = {
    if (configuration.repartionerEnabled) {
      implicit val repartitioned = Repartitioned
        .`with`(stringSerde, jsonNodeSerde)
        .withNumberOfPartitions(configuration.numberOfPartitions)
        .withStreamPartitioner(
          new PlayerStreamPartitioner(stringSerde.serializer())
        )
      implicit val materializedWagering
          : Materialized[String, JsonNode, ByteArrayKeyValueStore] =
        Materialized
          .as[String, JsonNode](
            Stores
              .persistentKeyValueStore(
                Topologies.AggregatorWageringStore
              )
          )(
            stringSerde,
            jsonNodeSerde
          )
          .withKeySerde(stringSerde)
          .withValueSerde(jsonNodeSerde)

      implicit val materializedSportsbets
          : Materialized[String, JsonNode, ByteArrayKeyValueStore] =
        Materialized
          .as[String, JsonNode](
            Stores
              .persistentKeyValueStore(
                Topologies.AggregatorSportsbetsStore
              )
          )(
            stringSerde,
            jsonNodeSerde
          )
          .withKeySerde(stringSerde)
          .withValueSerde(jsonNodeSerde)

      implicit val materializedWageringSport
          : Materialized[String, JsonNode, ByteArrayKeyValueStore] =
        Materialized
          .as[String, JsonNode](
            Stores
              .persistentKeyValueStore(
                Topologies.AggregatorWageringSportStore
              )
          )(
            stringSerde,
            jsonNodeSerde
          )
          .withKeySerde(stringSerde)
          .withValueSerde(jsonNodeSerde)

      val builder = new StreamsBuilder
      // {"bet_id":"88686fe0-52d8-ed11-814a-00155da60c02","player_id":"4d319a9a-0ada-e911-8102-00155d4a2d2c","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":0,"game_id":null,"vertical_id":null,"price":"153.99","bet_datetime":"2023-04-11T10:23:26.9982753Z","currency_id":100,"has_resulted":false,"result_datetime":null,"result_id":null}
      val wagering = builder
        .stream(configuration.topicWagering)
        .filter { (_, value) =>
          Option(value.get(Topologies.BetId)).isDefined &&
          Option(value.get(Topologies.PlayerId)).isDefined
        }
        .selectKey { (_, value) =>
          Option(value.get(Topologies.PlayerId)).get
            .asText() + PlayerPartitioner.Separator + UUID.nameUUIDFromBytes(
            Option(
              value.get(Topologies.BetId)
            ).get.asText().getBytes()
          )
        }
        .repartition(repartitioned.withName("wagering"))
        .toTable(materializedWagering)

      // {"_time":1681240007.895,"EventTimeStamp":"2023-04-11T19:06:46.6615575Z","player_id":"350e33d1-f26f-ec11-8142-00155da50417","bet_id":"a559d2f5-9bd8-ed11-8147-00155da51c09","sport_id":"1265c5bd-d716-e811-80cd-00155d4cf19b","event_id":"39726067","market_id":"de677d0c-e70f-e811-80d9-00155d4cf18a","is_live":false,"championship_id":"e681e7d7-0918-e811-80d5-00155d4cf18c","selection_id":"17e975a6-99d7-ed11-80e5-00155d108919","price":2.47}
      val sportsbets = builder
        .stream(configuration.topicSportsbets)
        .filter { (_, value) =>
          Option(value.get(Topologies.BetId)).isDefined &&
          Option(value.get(Topologies.PlayerId)).isDefined
        }
        .selectKey { (_, value) =>
          Option(value.get(Topologies.PlayerId)).get
            .asText() + PlayerPartitioner.Separator + UUID.nameUUIDFromBytes(
            Option(
              value.get(Topologies.BetId)
            ).get.asText().getBytes()
          )
        }
        .repartition(repartitioned.withName("sportsbets"))
        .toTable(materializedSportsbets)

      wagering
        .join(sportsbets, materializedWageringSport) { (w, s) =>
          val merge = JsonNodeFactory.instance.objectNode
          merge.set(Topologies.WageringSport.Wagering, w)
          merge.set(Topologies.WageringSport.Sportsbets, s)
          merge.asInstanceOf[JsonNode]
        }
        .toStream
        .to(configuration.topicWageringSport)
      Some(builder.build())
    } else {
      None
    }
  }

  @Produces @ApiTopology
  def buildApiTopology(): Option[Topology] = {
    if (configuration.apiEnabled) {

      implicit val materializedSports
          : Materialized[String, JsonNode, ByteArrayKeyValueStore] =
        Materialized
          .as[String, JsonNode](
            Stores
              .persistentKeyValueStore(Topologies.SportStore)
          )(
            stringSerde,
            jsonNodeSerde
          )
          .withKeySerde(stringSerde)
          .withValueSerde(jsonNodeSerde)

      val builder = new StreamsBuilder

      builder
        .stream[String, JsonNode](configuration.topicSport)
        .filter { (_, value) =>
          Option(value.get(Topologies.SportId)).isDefined &&
          Option(value.get(Topologies.SportDescription)).isDefined
        }
        .selectKey { (_, value) =>
          Option(value.get(Topologies.SportId)).get.asText()
        }
        .toTable(materializedSports)
      Some(builder.build())
    } else {
      None
    }
  }

  @Produces @CheckerTopology
  def buildCheckerTopology(): Option[Topology] = {
    if (configuration.checkerEnabled) {
      implicit val repartitioned = Repartitioned
        .`with`(stringSerde, jsonNodeSerde)
        .withNumberOfPartitions(configuration.numberOfPartitions)
        .withStreamPartitioner(
          new PlayerStreamPartitioner(stringSerde.serializer())
        )

      implicit val materializedChecker
          : Materialized[String, JsonNode, ByteArrayKeyValueStore] =
        Materialized
          .as[String, JsonNode](
            Stores
              .persistentKeyValueStore(
                Topologies.AggregatorCheckerStore
              )
          )(
            stringSerde,
            jsonNodeSerde
          )
          .withKeySerde(stringSerde)
          .withValueSerde(jsonNodeSerde)

      implicit val materializedWallet
          : Materialized[String, JsonNode, ByteArrayKeyValueStore] =
        Materialized
          .as[String, JsonNode](
            Stores
              .persistentKeyValueStore(
                Topologies.AggregatorWalletStore
              )
          )(
            stringSerde,
            jsonNodeSerde
          )
          .withKeySerde(stringSerde)
          .withValueSerde(jsonNodeSerde)

      val builder = new StreamsBuilder

      builder
        .stream[String, JsonNode](configuration.topicChecker)
        .repartition(repartitioned.withName("checker"))
        .toTable(materializedChecker)

      val supplier
          : ValueTransformerWithKeySupplier[String, JsonNode, List[String]] =
        new CheckerSupplier(
          clock,
          mapper,
          Topologies.AggregatorCheckerStore,
          Topologies.AggregatorWalletStore,
        )

      val walletTable = builder
        .stream[String, JsonNode](configuration.topicWallet)
        .filter { (_, value) =>
          Option(value.get(Topologies.PlayerId)).isDefined &&
          Option(value.get(Topologies.Amount)).isDefined &&
          Option(
            value.get(Topologies.TransactionDatetime)
          ).isDefined &&
          formatDatetime(
            value.get(Topologies.TransactionDatetime).asText()
          ).isDefined &&
          Option(value.get(Topologies.TransactionId)).isDefined &&
          Option(value.get(Topologies.TransactionTypeId))
            .map(v =>
              v.asText("")
                .toLowerCase
                .startsWith(Topologies.TransactionType_Deposit)
            )
            .getOrElse(false) &&
          Option(value.get(Topologies.TransactionStatusId))
            .map(v =>
              v.asText("")
                .toLowerCase
                .startsWith(Topologies.TransactionStatus_Complete)
            )
            .getOrElse(false)


        }
        .selectKey { (_, value) =>
          value
            .get(Topologies.PlayerId)
            .asText() + PlayerPartitioner.Separator +
            formatDatetime(
              value.get(Topologies.TransactionDatetime).asText()
            ).get + PlayerPartitioner.Separator +
            UUID.nameUUIDFromBytes(
              value.get(Topologies.TransactionId).asText().getBytes()
            )

        }
        .repartition(repartitioned.withName("wallet"))
        .toTable(materializedWallet)
      walletTable
        .transformValues[List[String]](
          supplier,
          Topologies.AggregatorCheckerStore,
          Topologies.AggregatorWalletStore,
        )
        .toStream
        .flatMap { case (_, value) =>
          (value.map(k => (k, null.asInstanceOf[JsonNode])).toIterable)
        }
        .to(configuration.topicChecker)(
          Produced.`with`(stringSerde, jsonNodeSerde)
        )
      Some(builder.build())
    } else {
      None
    }
  }

}
