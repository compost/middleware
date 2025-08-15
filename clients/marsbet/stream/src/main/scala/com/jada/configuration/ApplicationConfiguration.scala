package com.jada.configuration

import org.eclipse.microprofile.config.inject.ConfigProperty

import javax.enterprise.context.ApplicationScoped
import java.time.Duration

@ApplicationScoped
case class ApplicationConfiguration(
    @ConfigProperty(
      name = "kafka-streams.\"application.id\"",
      defaultValue = "stream-app"
    ) applicationId: String,
    @ConfigProperty(
      name = "build.time",
      defaultValue = "2024-12-13"
    ) buildTime: String,
    @ConfigProperty(name = "azure.connectionString") connectionString: String,
    @ConfigProperty(
      name = "azure.outputContainerName"
    ) outputContainerName: String,
    @ConfigProperty(name = "kafka-streams") configStreams: java.util.Map[
      String,
      String
    ],
    @ConfigProperty(name = "brand-queue") brandQueue: java.util.Map[
      String,
      String
    ],
    @ConfigProperty(
      name = "punctuator",
      defaultValue = "P1D"
    ) punctuator: Duration,
    @ConfigProperty(
      name = "mapping-selector-dev-prefix"
    ) mappingSelectorDevPrefix: java.util.Set[
      String
    ],
    @ConfigProperty(
      name = "brands-sqs-north"
    ) brandsSqsNorth: java.util.Set[
      String
    ],
    @ConfigProperty(
      name = "bootstrap.players",
      defaultValue = "false"
    ) bootstrapPlayers: Boolean
) {

  val topicPlayers = "players"
  val topicLogins = "logins"
  val topicWallets = "wallet"
  val topicWagerings = "wagering"
  val topicPlayerStatus = "player_status"
  val topicPlayerConsent = "player_consent"
  val topicHistoryPlayers = "history-players"

  val externalTopics =
    Set(
      topicPlayers,
      topicLogins,
      topicWallets,
      topicWagerings,
      topicPlayerStatus,
      topicPlayerConsent,
      topicHistoryPlayers
    )

  val sqsGroupId = s"${applicationId}-stream-app"
}
