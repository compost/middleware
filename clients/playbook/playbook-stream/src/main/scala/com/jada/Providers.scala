package com.jada

import io.smallrye.common.annotation.Identifier
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.eclipse.microprofile.config.inject.ConfigProperty

import java.util.Properties
import javax.enterprise.context.ApplicationScoped
import javax.enterprise.inject.Produces
import javax.inject.Inject
import com.jada.configuration.ApplicationConfiguration
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.StreamsConfig

@ApplicationScoped
class Providers @Inject() (config: ApplicationConfiguration) {

  @Produces
  @com.jada.DefaultTopology
  def buildDefaultKafkaStreams(@com.jada.DefaultTopology topology: Option[Topology]): Option[KafkaStreams] = {
    buildKafkaStreams("", topology)
  }

  @Produces
  @com.jada.BalanceTopology
  def buildBalanceTopologyKafkaStreams(
      @com.jada.BalanceTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("balance-", topology)
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
    config.configStreams.forEach((k, v) => prop.put(k, v))
    return AdminClient.create(prop)
  }

  @Produces
  def getProducer(): KafkaProducer[String, String] = {
    val prop = new java.util.Properties()
    config.configStreams.forEach((k, v) => {
      if (ProducerConfig.configNames().contains(k)) {
        prop.put(k, v)
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
    return new KafkaProducer(
      prop,
      new StringSerializer(),
      new StringSerializer()
    )
  }
}
