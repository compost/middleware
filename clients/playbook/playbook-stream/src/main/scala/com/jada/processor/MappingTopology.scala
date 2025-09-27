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
import com.jada.models._
import io.circe._
import io.circe.generic.auto._
import com.jada.models.StakeFactor

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
    val storeCountryName = "country-store"
    val storeCurrencyName = "currency-store"

    val stringSerde: Serde[String] = Serdes.String
    val arrayByteSerde: Serde[Array[Byte]] = Serdes.ByteArray
    val playerStoreSerde = CirceSerdes.serde[PlayerStore]
    val currencySerde = CirceSerdes.serde[Currency]
    val countrySerde = CirceSerdes.serde[Country]
    implicit val consumed = Consumed.`with`(stringSerde, arrayByteSerde)
    implicit val consumedCountry = Consumed.`with`(stringSerde, countrySerde)
    implicit val consumedCurrency = Consumed.`with`(stringSerde, currencySerde)

    val builder = new StreamsBuilder
    builder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storePlayerStoreName),
        stringSerde,
        playerStoreSerde
      )
    )

    val brandIds = config.brandQueue.keySet()

    builder
      .stream[String, Country](
        config.topicCountries
      )(Consumed.`with`(stringSerde, CirceSerdes.serde[Country]))
      .filter((_, v) =>
        v.country_id.isDefined && v.brand_id.exists(b => brandIds.contains(b))
      )
      .selectKey((_, v) => generateKey(v.brand_id, v.country_id))
      .to(config.topicCountryRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[Country])
      )

    builder
      .stream[String, Currency](
        config.topicCurrency
      )(Consumed.`with`(stringSerde, CirceSerdes.serde[Currency]))
      .filter((_, v) =>
        v.currency_id.isDefined && v.brand_id.exists(b => brandIds.contains(b))
      )
      .selectKey((_, v) => generateKey(v.brand_id, v.currency_id))
      .to(config.topicCurrencyRepartitioned)(
        Produced.`with`(stringSerde, CirceSerdes.serde[Currency])
      )

    val currencyStateStore = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(storeCurrencyName),
      stringSerde,
      currencySerde
    )

    val countryStateStore = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(storeCountryName),
      stringSerde,
      countrySerde
    )

    builder.addGlobalStore[String, Currency](
      currencyStateStore,
      config.topicCurrencyRepartitioned,
      consumedCurrency,
      new GlobalStoreSupplier[String, com.jada.models.Currency](
        storeCurrencyName
      )
    )

    builder.addGlobalStore(
      countryStateStore,
      config.topicCountryRepartitioned,
      consumedCountry,
      new GlobalStoreSupplier[String, com.jada.models.Country](
        storeCountryName
      )
    )

    import scala.collection.JavaConverters._
    val kstream: KStream[String, Array[Byte]] =
      builder.stream[String, Array[Byte]](config.appTopics)(consumed)

    kstream.transform[Unit, Unit](
      () =>
        new MappingTransformer(
          config,
          sqs,
          ueNorthSQS, 
          storePlayerStoreName,
          storeCurrencyName,
          storeCountryName,
        ),
      storePlayerStoreName,
    )

    val b = builder.build()
    logger.debug(s"Describe ${b.describe()}")
    b
  }

  def generateKey(brandId: Option[String], playerId: Option[String]): String = {
    s"${brandId.get}-${playerId.get}"
  }
}
