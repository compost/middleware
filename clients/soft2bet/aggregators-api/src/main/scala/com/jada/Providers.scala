package com.jada

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

import javax.enterprise.context.ApplicationScoped
import javax.enterprise.inject.Produces
import javax.inject.Inject
import com.jada.configuration.ApplicationConfiguration
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.StreamsConfig
import com.jada.processor.CheckerTopology
import com.jada.processor.RepartitionerTopology
import com.jada.processor.ApiTopology
import org.apache.kafka.clients.admin.AdminClientConfig
import java.time.Clock
import io.quarkus.arc.DefaultBean

@ApplicationScoped
class Providers @Inject()(config: ApplicationConfiguration) {

  @Produces
  @ApiTopology
  def buildApiTopologyKafkaStreams(
      @ApiTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams(config.apiKafkaGroupPrefix, topology)
  }
  @Produces
  @CheckerTopology
  def buildCheckerTopologyKafkaStreams(
      @CheckerTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams(config.checkerKafkaGroupPrefix, topology)
  }

  @Produces
  @RepartitionerTopology
  def buildRepartitionerTopologyKafkaStreams(
      @RepartitionerTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams(config.repartitionerKafkaGroupPrefix, topology)
  }

  def buildKafkaStreams(
      prefix: String = "",
      topology: Option[Topology] = None
  ): Option[KafkaStreams] = {
    topology.map(t => {
      val prop = new java.util.Properties()
      config.configStreams.forEach((k, v) => {
        if (k == StreamsConfig.APPLICATION_ID_CONFIG) {
          prop.put(k, s"${prefix}${v}")
        } else {
          prop.put(k, v)
        }
      })
      new KafkaStreams(t, prop)
    })
  }
  @Produces
  def getAdminClientKafka(): AdminClient = {
    val prop = new java.util.Properties()
    config.configStreams.forEach((k, v) => {
      if (AdminClientConfig.configNames().contains(k)) {
        prop.put(k, v)
      }
    })
    AdminClient.create(prop)
  }

  @Produces
  @DefaultBean
  def clock(): Clock = {
    Clock.systemUTC()
  }
  @Produces
  def getProducer(): KafkaProducer[String, String] = {
    val prop = new java.util.Properties()
    config.configStreams.forEach((k, v) => {
      val withoutPrefix = k.replaceFirst(StreamsConfig.PRODUCER_PREFIX, "")
      if (ProducerConfig.configNames().contains(withoutPrefix)) {
        prop.put(withoutPrefix, v)
      }
    })
    prop.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer]
    )
    prop.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer]
    )
    new KafkaProducer(
      prop,
      new StringSerializer(),
      new StringSerializer()
    )
  }
}
