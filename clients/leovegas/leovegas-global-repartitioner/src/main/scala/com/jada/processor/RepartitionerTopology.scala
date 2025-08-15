package com.jada.processor

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.fasterxml.jackson.databind.JsonNode
import com.jada.configuration.ApplicationConfiguration
import io.quarkus.arc.properties.IfBuildProperty
import io.smallrye.config.ConfigMapping
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.json.JsonSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.TopologyDescription.GlobalStore
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import org.eclipse.microprofile.config.inject.ConfigProperty

import java.io.File
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import javax.enterprise.context.ApplicationScoped
import javax.enterprise.inject.Produces
import javax.inject.Inject
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try
import org.jboss.logging.Logger
import com.fasterxml.jackson.databind.json.JsonMapper
import com.jada.serdes.CirceSerdes
import com.jada.models.{
  Event,
  Login,
  Market,
  Player,
  PlayerExtended,
  SportsBets,
  Wagering,
  Wallet
}
import io.circe._
import io.circe.generic.auto._

@ApplicationScoped
class RepartitionerTopology @Inject() (
    config: ApplicationConfiguration
) {

  private final val logger =
    Logger.getLogger(classOf[RepartitionerTopology])

  @Produces
  def buildTopology(): Topology = {
    val extendedStorePlayerStoreName = "extended-player-store"

    val stringSerde: Serde[String] = Serdes.String
    val arrayByteSerde: Serde[Array[Byte]] = Serdes.ByteArray
    val playerStoreSerde = CirceSerdes.serde[PlayerExtended]
    implicit val consumed = Consumed.`with`(stringSerde, arrayByteSerde)

    val builder = new StreamsBuilder

    builder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(extendedStorePlayerStoreName),
        stringSerde,
        playerStoreSerde
      )
    )

    builder
      .stream[String, Player](
        config.topicPlayers
      )(Consumed.`with`(stringSerde, CirceSerdes.serde[Player]))
      .filter((_, v) =>
        v.player_id.isDefined && v.brand_id.getOrElse("") == "22"
      )
      .selectKey((_, v) => generateKey(v.brand_id, v.player_id))
      .to(config.topicPlayersRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[Player])
      )

    builder
      .stream[String, Login](
        config.topicLogins
      )(Consumed.`with`(stringSerde, CirceSerdes.serde[Login]))
      .filter((_, v) =>
        v.player_id.isDefined && v.brand_id.getOrElse("") == "22"
      )
      .selectKey((_, v) => generateKey(v.brand_id, v.player_id))
      .to(config.topicLoginsRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[Login])
      )

    builder
      .stream[String, Wallet](
        config.topicWallets
      )(Consumed.`with`(stringSerde, CirceSerdes.serde[Wallet]))
      .filter((_, v) =>
        v.player_id.isDefined && v.brand_id.getOrElse("") == "22"
      )
      .selectKey((_, v) => generateKey(v.brand_id, v.player_id))
      .to(config.topicWalletsRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[Wallet])
      )

    builder
      .stream[String, Wagering](
        config.topicWagerings
      )(Consumed.`with`(stringSerde, CirceSerdes.serde[Wagering]))
      .filter((_, v) =>
        v.player_id.isDefined && v.brand_id.getOrElse("") == "22"
      )
      .selectKey((_, v) => generateKey(v.brand_id, v.player_id))
      .to(config.topicWageringsRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[Wagering])
      )

    builder
      .stream[String, Event](
        config.topicEvents
      )(Consumed.`with`(stringSerde, CirceSerdes.serde[Event]))
      .filter((_, v) => v.event_id.isDefined)
      .selectKey((_, v) => v.event_id.get)
      .filter((_, v) => v.event_datetime.isDefined)
      .to(config.topicEventsRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[Event])
      )

    builder
      .stream[String, Market](
        config.topicMarkets
      )(Consumed.`with`(stringSerde, CirceSerdes.serde[Market]))
      .filter((_, v) => v.market_id.isDefined)
      .selectKey((_, v) => v.market_id.get)
      .to(config.topicMarketsRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[Market])
      )

    builder
      .stream[String, SportsBets](
        config.topicSportsbets
      )(Consumed.`with`(stringSerde, CirceSerdes.serde[SportsBets]))
      .filter((_, v) =>
        v.player_id.isDefined && v.brand_id.getOrElse("") == "22"
      )
      .selectKey((_, v) => generateKey(v.brand_id, v.player_id))
      .to(config.topicSportsbetsRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[SportsBets])
      )

    builder.build()
  }

  def generateKey(brandId: Option[String], playerId: Option[String]): String = {
    s"${brandId.get}-${playerId.get}"
  }
}
