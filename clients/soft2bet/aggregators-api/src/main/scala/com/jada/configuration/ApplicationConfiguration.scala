package com.jada.configuration

import org.eclipse.microprofile.config.inject.ConfigProperty

import javax.enterprise.context.ApplicationScoped

@ApplicationScoped
case class ApplicationConfiguration(
    @ConfigProperty(
      name = "kafka-streams.\"application.id\"",
      defaultValue = "aggregators-api"
    ) applicationId: String,
    @ConfigProperty(name = "kafka-streams") configStreams: java.util.Map[
      String,
      String
    ],
    @ConfigProperty(
      name = "api.kafka.group.prefix",
      defaultValue = "api-"
    ) val apiKafkaGroupPrefix: String,
    @ConfigProperty(
      name = "repartitioner.kafka.group.prefix",
      defaultValue = "repartitioner-"
    ) val repartitionerKafkaGroupPrefix: String,
    @ConfigProperty(
      name = "checker.kafka.group.prefix",
      defaultValue = "checker-"
    ) val checkerKafkaGroupPrefix: String,
    @ConfigProperty(
      name = "topic.wallet",
      defaultValue = "wallet"
    ) val topicWallet: String,
    @ConfigProperty(
      name = "topic.player",
      defaultValue = "players"
    ) val topicPlayer: String,
    @ConfigProperty(
      name = "topic.sport",
      defaultValue = "sports"
    ) val topicSport: String,
    // This one is created by us
    @ConfigProperty(
      name = "topic.checkers",
      defaultValue = "checkers"
    ) val topicChecker: String,
    @ConfigProperty(
      name = "topic.sportsbets",
      defaultValue = "sportsbets"
    ) val topicSportsbets: String,
    // This one is created by us
    @ConfigProperty(
      name = "topic.wagering-sport",
      defaultValue = "wagering-sport"
    ) val topicWageringSport: String,
    @ConfigProperty(
      name = "topic.wagering",
      defaultValue = "wagering"
    ) val topicWagering: String,
    @ConfigProperty(
      name = "number.of.partitions",
      defaultValue = "32"
    ) val numberOfPartitions: Int,
    @ConfigProperty(
      name = "checker.topology.enabled",
      defaultValue = "false"
    ) val checkerEnabled: Boolean,
    @ConfigProperty(
      name = "repartitioner.topology.enabled",
      defaultValue = "false"
    ) val repartionerEnabled: Boolean,
    @ConfigProperty(
      name = "api.topology.enabled",
      defaultValue = "false"
    ) val apiEnabled: Boolean,
)
