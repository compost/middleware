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
  Login,
  Player,
  PlayerStatus,
  PlayerStore,
  PlayerConsent,
  Wagering,
  Wallet
}
import io.circe._
import io.circe.generic.auto._

@ApplicationScoped
class MappingTopology @Inject() (
    config: ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient
) {

  private final val logger =
    Logger.getLogger(classOf[MappingTopology])

  val ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient =
    software.amazon.awssdk.services.sqs.SqsClient
      .builder()
      .region(software.amazon.awssdk.regions.Region.EU_NORTH_1)
      .build()

  @Produces
  def buildTopology(): Topology = {
    val storePlayerStoreName = "player-store"

    val stringSerde: Serde[String] = Serdes.String
    val arrayByteSerde: Serde[Array[Byte]] = Serdes.ByteArray
    val playerStoreSerde = CirceSerdes.serde[PlayerStore]
    implicit val consumed = Consumed.`with`(stringSerde, arrayByteSerde)

    val builder = new StreamsBuilder

    builder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storePlayerStoreName),
        stringSerde,
        playerStoreSerde
      )
    )

    val brandIds = config.brandQueue.keySet()

    import scala.collection.JavaConverters._
    val kstream: KStream[String, Array[Byte]] =
      builder.stream[String, Array[Byte]](config.externalTopics)(consumed)

    kstream.transform[Unit, Unit](
      () =>
        new MappingTransformer(
          config,
          sqs,
          ueNorthSQS,
          storePlayerStoreName,
        ),
      storePlayerStoreName
    )

    val b = builder.build()
    logger.debug(s"Describe ${b.describe()}")
    b
  }
}
