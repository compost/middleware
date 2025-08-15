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
import com.jada.models.{
  Action,
  Player,
  PlayerStatus,
  PlayerStore,
  ActionTrigger,
  Login
}
import io.circe._
import io.circe.generic.auto._
import java.util.UUID

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
    val storePlayerStoreName = "player-store"

    val stringSerde: Serde[String] = Serdes.String
    val arrayByteSerde: Serde[Array[Byte]] = Serdes.ByteArray
    val playerStoreSerde = CirceSerdes.serde[PlayerStore]
    val loginSerde = CirceSerdes.serde[Login]
    val actionTriggerStoreSerde = CirceSerdes.serde[ActionTrigger]
    val actionSerde = CirceSerdes.serde[Action]
    implicit val consumed = Consumed.`with`(stringSerde, arrayByteSerde)
    implicit val consumedLogin = Consumed.`with`(stringSerde, loginSerde)

    val builder = new StreamsBuilder

    builder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore(storeStartStopName),
        stringSerde,
        actionSerde
      )
    )

    builder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storePlayerStoreName),
        stringSerde,
        playerStoreSerde
      )
    )

    val brandIds = config.brandQueue.keySet()

    builder
      .stream[String, Player](
        config.topicPlayers
      )(Consumed.`with`(stringSerde, CirceSerdes.serde[Player]))
      .filter((_, v) =>
        v.player_id.isDefined && v.brand_id.exists(b => brandIds.contains(b))
      )
      .selectKey((_, v) => generateKey(v.brand_id, v.player_id))
      .to(config.topicPlayersRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[Player])
      )

    builder
      .stream[String, ActionTrigger](
        config.topicActionTriggers
      )(Consumed.`with`(stringSerde, CirceSerdes.serde[ActionTrigger]))
      .filter((_, v) =>
        v.player_id.isDefined && v.brand_id.exists(b => brandIds.contains(b))
      )
      .selectKey((_, v) => generateKey(v.brand_id, v.player_id))
      .to(config.topicActionTriggersRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[ActionTrigger])
      )

    builder
      .stream[String, Login](
        config.topicLogins
      )(consumed = consumedLogin)
      .filter((_, v) =>
        v.player_id.isDefined && v.brand_id.exists(b => brandIds.contains(b))
      )
      .selectKey((_, v) => generateKey(v.brand_id, v.player_id))
      .to(config.topicLoginsRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[Login])
      )

    builder
      .stream[String, Login](config.topicLoginsRepartitioned)(consumedLogin)
      .transform[Unit, Unit](() =>
        new LoginTransformer(
          config,
          referentials,
          sqs
        )
      )

    import scala.collection.JavaConverters._
    val kstream: KStream[String, Array[Byte]] =
      builder.stream[String, Array[Byte]](config.appTopics)(consumed)

    kstream.transform[Unit, Unit](
      () =>
        new MappingTransformer(
          config,
          referentials,
          sqs,
          storeStartStopName,
          storePlayerStoreName
        ),
      storeStartStopName,
      storePlayerStoreName
    )
    val topology = builder.build()
    println(topology.describe())
    topology
  }

  def generateKey(brandId: Option[String], playerId: Option[String]): String = {
    s"${brandId.get}-${playerId.get}"
  }
}
