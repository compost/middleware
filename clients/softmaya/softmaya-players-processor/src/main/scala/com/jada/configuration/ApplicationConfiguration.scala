package com.jada.configuration

import org.eclipse.microprofile.config.inject.ConfigProperty

import javax.enterprise.context.ApplicationScoped
import java.time.Duration

@ApplicationScoped
case class ApplicationConfiguration(
    @ConfigProperty(
      name = "kafka-streams.\"application.id\"",
      defaultValue = "softmaya-players-processor-v4"
    ) applicationId: String,
    @ConfigProperty(name = "sqs.queue.softmaya") sqsQueueSoftMaya: String,
    @ConfigProperty(name = "sqs.queue.softmaya_betshift") sqsQueueSoftMayaBetShift: String,
    @ConfigProperty(name = "sqs.queue.softmaya_pokies") sqsQueueSoftMayaPokies: String,
    @ConfigProperty(name = "sqs.queue.group.id") sqsGroupId: String,
    @ConfigProperty(name = "startup") startup: Boolean,
    @ConfigProperty(
      name = "topic.coordinator",
      defaultValue = "softmaya-players-processor-coordinator"
    ) topicCoordinator: String,
    @ConfigProperty(
      name = "topic.players",
      defaultValue = "players"
    ) topicPlayers: String,
    @ConfigProperty(
      name = "topic.players.repartitioned",
      defaultValue = "players-repartitioned"
    ) topicPlayersRepartitioned: String,
    @ConfigProperty(
      name = "mapping.selector.players",
      defaultValue = "player_updated"
    ) mappingSelectorPlayers: String,
    @ConfigProperty(
      name = "topic.logins",
      defaultValue = "logins"
    ) topicLogins: String,
    @ConfigProperty(
      name = "topic.logins.repartitioned",
      defaultValue = "logins-repartitioned"
    ) topicLoginsRepartitioned: String,
    @ConfigProperty(
      name = "mapping.selector.logins",
      defaultValue = "player_login"
    ) mappingSelectorLogins: String,
    @ConfigProperty(
      name = "topic.wallet",
      defaultValue = "wallet"
    ) topicWallet: String,
    @ConfigProperty(
      name = "topic.wallet.repartitioned",
      defaultValue = "wallet-repartitioned"
    ) topicWalletRepartitioned: String,
    @ConfigProperty(
      name = "topic.wagering",
      defaultValue = "wagering"
    ) topicWagering: String,
    @ConfigProperty(
      name = "topic.wagering.repartitioned",
      defaultValue = "wagering-repartitioned"
    ) topicWageringRepartitioned: String,
    @ConfigProperty(name = "kafka-streams") configStreams: java.util.Map[
      String,
      String
    ],
    @ConfigProperty(
      name = "bootstrap",
      defaultValue = "false"
    ) bootstrap: Boolean,
    @ConfigProperty(
      name = "punctuator",
      defaultValue = "P1D"
    ) punctuator: Duration,
    @ConfigProperty(name = "azure.connectionString") connectionString: String,
    @ConfigProperty(
      name = "azure.outputContainerName"
    ) outputContainerName: String,
)
