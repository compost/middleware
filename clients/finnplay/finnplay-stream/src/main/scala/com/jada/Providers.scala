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

@ApplicationScoped
class Providers @Inject()(config: ApplicationConfiguration) {

  @Produces
  def buildKafkaStreams(topology: Topology): KafkaStreams = {
    val prop = new java.util.Properties()
    config.configStreams.forEach((k, v) => prop.put(k, v))
    new KafkaStreams(topology, prop)
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
