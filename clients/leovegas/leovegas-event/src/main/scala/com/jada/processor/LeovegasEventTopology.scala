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

import com.leovegas.serdes._
import com.leovegas.model._
import java.io.File
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import javax.enterprise.context.ApplicationScoped
import javax.enterprise.inject.Produces
import javax.inject.Inject
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

@ApplicationScoped
class LeovegasEventTopology @Inject() (
    config: ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient
) {

  import io.circe.generic.auto._
  @Produces
  def buildTopology(): Topology = {
    import CirceSerdes._
    val stringSerde: Serde[String] = Serdes.String
    val dataBetStoreName = "databet-store"
    val builder = new StreamsBuilder

    val dataBetStore = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(dataBetStoreName),
      stringSerde,
      CirceSerdes.serde[DataBet]
    )

    builder.addStateStore(dataBetStore)

    val eventStoreSupplier = Stores.inMemoryKeyValueStore(Common.eventStoreName)
    builder.globalTable(
      Common.eventTopicRepartitioned,
      Materialized
        .as[String, Event](eventStoreSupplier)(
          stringSerde,
          CirceSerdes.serde[Event]
        )
        .withKeySerde(stringSerde)
        .withValueSerde(CirceSerdes.serde[Event])
    )(Consumed.`with`(stringSerde, CirceSerdes.serde[Event]))

    val marketStoreSupplier =
      Stores.inMemoryKeyValueStore(Common.marketStoreName)

    builder.globalTable(
      Common.marketTopicRepartitioned,
      Materialized
        .as[String, Market](marketStoreSupplier)(
          stringSerde,
          CirceSerdes.serde[Market]
        )
        .withKeySerde(stringSerde)
        .withValueSerde(CirceSerdes.serde[Market])
    )(Consumed.`with`(stringSerde, CirceSerdes.serde[Market]))

    builder
      .stream[String, Array[Byte]](
        List(
          Common.wageringTopicRepartitioned,
          Common.sportsbetTopicRepartitioned
        ).toSet
      )(Consumed `with` (stringSerde, new Serdes.ByteArraySerde()))
      .filter((_, v) => v != null)
      .transform(
        () =>
          new LeovegasEventTransformer(
            config,
            sqs,
            dataBetStoreName
          ),
        dataBetStoreName
      )

    builder.build()
  }
}
