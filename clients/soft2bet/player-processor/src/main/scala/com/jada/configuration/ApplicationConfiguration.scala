package com.jada.configuration

import org.eclipse.microprofile.config.inject.ConfigProperty

import javax.enterprise.context.ApplicationScoped
import java.time.Duration

@ApplicationScoped
case class ApplicationConfiguration(
    @ConfigProperty(
      name = "kafka-streams.\"application.id\"",
      defaultValue = "soft2bet-players-app-v8"
    ) applicationId: String,
    @ConfigProperty(
      name = "build.time",
      defaultValue = "2025-01-13"
    ) buildTime: String,
    @ConfigProperty(
      name = "punctuator",
      defaultValue = "PT30M"
    ) punctuator: Duration,
    @ConfigProperty(
      name = "azure.outputContainerName",
      defaultValue = "soft2bet"
    ) outputContainerName: String,
    @ConfigProperty(name = "azure.connectionString") connectionString: String,
    @ConfigProperty(name = "sqs.queue") sqsQueue: String,
    @ConfigProperty(name = "sqs.queue.mga") sqsQueueMGA: String,
    @ConfigProperty(name = "sqs.queue.dk") sqsQueueDK: String,
    @ConfigProperty(name = "sqs.queue.ds") sqsQueueDS: String,
    @ConfigProperty(name = "sqs.queue.sga") sqsQueueSGA: String,
    @ConfigProperty(name = "sqs.queue.boomerang") sqsQueueBoomerang: String,
    @ConfigProperty(
      name = "sqs.queue.casinoinfinity"
    ) sqsQueueCasinoInfinity: String,
    @ConfigProperty(name = "sqs.queue.ro") sqsQueueRO: String,
    @ConfigProperty(name = "sqs.queue.ro2") sqsQueueRO2: String,
    @ConfigProperty(name = "sqs.queue.ca") sqsQueueCA: String,
    @ConfigProperty(name = "sqs.queue.fp") sqsQueueFP: String,
    @ConfigProperty(name = "sqs.queue.nb") sqsQueueNB: String,
    @ConfigProperty(name = "sqs.queue.funid") sqsQueueFunid: String,
    @ConfigProperty(name = "sqs.queue.elabet") sqsQueueElabet: String,
    @ConfigProperty(name = "sqs.queue.mx") sqsQueueMX: String,
    @ConfigProperty(name = "sqs.queue.spin247") sqsQueueSpin247: String,
    @ConfigProperty(name = "sqs.queue.cp") sqsQueueCp: String,
    @ConfigProperty(name = "sqs.queue.sq") sqsQueueSq: String,
    @ConfigProperty(name = "sqs.queue.ibet") sqsQueueIBet: String,
    @ConfigProperty(name = "sqs.queue.group.id") sqsGroupId: String,
    @ConfigProperty(name = "startup") startup: Boolean,
    @ConfigProperty(name = "store.start-stop.name") storeStartStop: String,
    @ConfigProperty(
      name = "topic.coordinator",
      defaultValue = "player-processor-coordinator"
    ) topicCoordinator: String,
    @ConfigProperty(name = "kafka-streams") configStreams: java.util.Map[
      String,
      String
    ],
    @ConfigProperty(
      name = "default.topology.enabled",
      defaultValue = "true"
    ) val defaultTopologyEnabled: Boolean,
    @ConfigProperty(
      name = "lifetimeDepositCount.topology.enabled",
      defaultValue = "false"
    ) val lifetimeDepositCountTopologyEnabled: Boolean,
    @ConfigProperty(
      name = "playerkpi.topology.enabled",
      defaultValue = "false"
    ) val playerKPITopologyEnabled: Boolean,
    @ConfigProperty(
      name = "firstdepositloss.topology.enabled",
      defaultValue = "false"
    ) val firstDepositLossTopologyEnabled: Boolean,
    @ConfigProperty(
      name = "playersegmentation.topology.enabled",
      defaultValue = "false"
    ) val playerSegmentationTopologyEnabled: Boolean,
    @ConfigProperty(
      name = "sportpush.topology.enabled",
      defaultValue = "false"
    ) val sportPushTopologyEnabled: Boolean,
    @ConfigProperty(
      name = "funid.topology.enabled",
      defaultValue = "false"
    ) val funidTopologyEnabled: Boolean,
    @ConfigProperty(
      name = "repartitioner.topology.enabled",
      defaultValue = "false"
    ) val repartitionerTopologyEnabled: Boolean,
    @ConfigProperty(
      name = "login.topology.enabled",
      defaultValue = "false"
    ) val loginTopologyEnabled: Boolean,
    @ConfigProperty(
      name = "force",
      defaultValue = "true"
    ) val force: Boolean,
    @ConfigProperty(
      name = "missingdata.topology.enabled",
      defaultValue = "false"
    ) val missingDataTopologyEnabled: Boolean,
    @ConfigProperty(
      name = "dryRun",
      defaultValue = "false"
    ) val dryRun: Boolean
)
