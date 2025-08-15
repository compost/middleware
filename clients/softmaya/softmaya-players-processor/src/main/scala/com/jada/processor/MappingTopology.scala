package com.jada.processor

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.fasterxml.jackson.databind.JsonNode
import com.jada.Common
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
import com.jada.models.{Action, Login, Player, PlayerStore, Wagering, Wallet}
import io.circe._
import io.circe.generic.auto._

@ApplicationScoped
class MappingTopology @Inject() (
    config: ApplicationConfiguration,
    referentials: com.jada.Referentials,
    sqs: software.amazon.awssdk.services.sqs.SqsClient
) {

  private final val logger =
    Logger.getLogger(classOf[MappingTopology])

  @Produces
  def buildTopology(): Topology = {
    val storeStartStopName = "start-stop-store"
    val storePlayerBrandStoreName = "player-brand-store"

    val stringSerde: Serde[String] = Serdes.String
    val arrayByteSerde: Serde[Array[Byte]] = Serdes.ByteArray
    val playerStoreSerde = CirceSerdes.serde[PlayerStore]
    val actionSerde = CirceSerdes.serde[Action]
    implicit val consumed = Consumed.`with`(stringSerde, arrayByteSerde)

    val builder = new StreamsBuilder

    builder.addStateStore(Stores.keyValueStoreBuilder(
      Stores.inMemoryKeyValueStore(storeStartStopName),
      stringSerde,
      actionSerde
    ))

    builder.addStateStore(Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(storePlayerBrandStoreName),
      stringSerde,
      playerStoreSerde
    ))


    builder.stream[String, Player](
      config.topicPlayers
    )(Consumed.`with`(stringSerde,  CirceSerdes.serde[Player]))
      .filter((_, v) => v.player_id.isDefined)
      .selectKey((_, v) => v.player_id.get)
      .to(config.topicPlayersRepartitioned)(Produced.`with`(stringSerde, CirceSerdes.serde[Player]))

    builder.stream[String, Login](
      config.topicLogins
    )(Consumed.`with`(stringSerde,  CirceSerdes.serde[Login]))
      .filter((_, v) => v.player_id.isDefined)
      .selectKey((_, v) => v.player_id.get)
      .to(config.topicLoginsRepartitioned)(Produced.`with`(stringSerde, CirceSerdes.serde[Login]))

    builder.stream[String, Wallet](
      config.topicWallet
    )(Consumed.`with`(stringSerde,  CirceSerdes.serde[Wallet]))
      .filter((_, v) => v.player_id.isDefined)
      .selectKey((_, v) => v.player_id.get)
      .to(config.topicWalletRepartitioned)(Produced.`with`(stringSerde, CirceSerdes.serde[Wallet]))

    builder.stream[String, Wagering](
      config.topicWagering
    )(Consumed.`with`(stringSerde,  CirceSerdes.serde[Wagering]))
      .filter((_, v) => v.player_id.isDefined)
      .selectKey((_, v) => v.player_id.get)
      .to(config.topicWageringRepartitioned)(Produced.`with`(stringSerde, CirceSerdes.serde[Wagering]))

    import scala.collection.JavaConverters._
    val kstream: KStream[String, Array[Byte]] =
      builder.stream[String, Array[Byte]](Set(config.topicCoordinator, config.topicPlayersRepartitioned, config.topicLoginsRepartitioned, config.topicWalletRepartitioned, config.topicWageringRepartitioned))(consumed)

    kstream.transform[Unit, Unit](
      () => new MappingTransformer(config, referentials, sqs, storeStartStopName, storePlayerBrandStoreName),
      storeStartStopName,
      storePlayerBrandStoreName,
    )
    builder.build()
  }
}
