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
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

@ApplicationScoped
class LeovegasTriggerJourneyTopology @Inject()(
    config: ApplicationConfiguration,
    @ConfigProperty(name = "country.code.uk") countryUK: java.util.List[String],
    @ConfigProperty(name = "country.code.nl") countryNL: java.util.List[String],
    @ConfigProperty(name = "country.code.br") countryBR: java.util.List[String],
    @ConfigProperty(name = "topic.input") topicInput: String,
    sqs: software.amazon.awssdk.services.sqs.SqsClient
) {

  @Produces
  def buildTopology(): Topology = {
    val stringSerde: Serde[String] = Serdes.String
    val jsonNodeSerializer = new JsonSerializer()
    val jsonNodeDeserializer = new JsonDeserializer()
    val jsonNodeSerde =
      Serdes.serdeFrom(jsonNodeSerializer, jsonNodeDeserializer)

    implicit val consumed = Consumed.`with`(stringSerde, jsonNodeSerde)

    val builder = new StreamsBuilder

    val startStopStore = Stores.keyValueStoreBuilder(
      Stores.inMemoryKeyValueStore(config.storeStartStop),
      stringSerde,
      jsonNodeSerde
    )

    builder.addStateStore(startStopStore)

    val kstream: KStream[String, JsonNode] = builder.stream[String, JsonNode](
      Set(topicInput, config.topicCoordinator)
    )
    kstream.transform[Unit, Unit](
      () =>
        new LeovegasTriggerJourneyTransformer(
          config,
          topicInput,
          countryUK,
          countryNL,
          countryBR,
          sqs,
          config.storeStartStop
        ),
      config.storeStartStop
    )
    builder.build()
  }
}
