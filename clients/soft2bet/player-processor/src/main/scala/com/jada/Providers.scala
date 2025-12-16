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
  @DefaultTopology
  def buildDefaultTopologyKafkaStreams(
      @DefaultTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("", topology)
  }

  @Produces
  @LifetimeDepositCountTopology
  def buildLifetimeDepositCountTopologyKafkaStreams(
      @LifetimeDepositCountTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("lifetime-deposit-count-", topology)
  }

  @Produces
  @VipLevelTopology
  def buildVipLevelTopologyKafkaStreams(
      @VipLevelTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("vip-level-", topology)
  }

  @Produces
  @FirstDepositLossTopology
  def buildFirstDepositLossKafkaStreams(
      @FirstDepositLossTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("fdl-", topology)
  }

  @Produces
  @PlayerKPITopology
  def buildPlayerKPITopologyKafkaStreams(
      @PlayerKPITopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("player-kpi-", topology)
  }

  @Produces
  @LoginTopology
  def buildLoginTopologyKafkaStreams(
      @LoginTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("login-", topology)
  }

  @Produces
  @FixBlockedTopology
  def buildFixBlockedTopologyKafkaStreams(
      @FixBlockedTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("fix-blocked-", topology)
  }

  @Produces
  @MissingDataTopology
  def buildMissingDataKafkaStreams(
      @MissingDataTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("missing-data-", topology)
  }

  @Produces
  @RepartitionerTopology
  def buildRepartitionerTopologyKafkaStreams(
      @RepartitionerTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("repartitioner-", topology)
  }
  @Produces
  @PlayerSegmentationTopology
  def buildPlayerSegmentationKafkaStreams(
      @PlayerSegmentationTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("player-segmentation-", topology)
  }

  @Produces
  @FunidTopology
  def buildFunidKafkaStreams(
      @FunidTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("funid-", topology)
  }

  @Produces
  @SportPushTopology
  def buildSportPushKafkaStreams(
      @SportPushTopology topology: Option[Topology]
  ): Option[KafkaStreams] = {
    buildKafkaStreams("mw-sportpush-", topology)
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
