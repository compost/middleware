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
      name = "bootstrap.players",
      defaultValue = "false"
    ) bootstrapPlayers: Boolean,
    @ConfigProperty(
      name = "inMemory",
      defaultValue = "false"
    ) inMemory: Boolean,
    @ConfigProperty(
      name = "update.country-currency",
      defaultValue = "false"
    ) updateCountryCurrency: Boolean,
    @ConfigProperty(name = "prefix") prefix: String,
    @ConfigProperty(name = "azure.connectionString") connectionString: String,
    @ConfigProperty(
      name = "azure.outputContainerName"
    ) outputContainerName: String,
    @ConfigProperty(name = "kafka-streams") configStreams: java.util.Map[
      String,
      String
    ],
    @ConfigProperty(
      name = "punctuator",
      defaultValue = "PT10M"
    ) punctuator: Duration,
    @ConfigProperty(name = "brand-queue") brandQueue: java.util.Map[
      String,
      String
    ],
    @ConfigProperty(
      name = "mapping-selector-dev-prefix"
    ) mappingSelectorDevPrefix: java.util.Set[
      String
    ]
) {

  val topicOutput = s"${prefix}-full-players"

  val topicPlayers = "players"
  val topicPlayersBatch = "player_batch"
  val topicLogins = "logins"
  val topicWallets = "wallet"
  val topicWagerings = "wagering"
  val topicPlayerStatus = "player_status"
  val topicUserConsentUpdate = "player_consent"
  val topicActionTrigger = "action_triggers"
  val topicBonusTransaction = "bonus_transaction"

  val appTopics = Set(
    topicPlayersBatch,
    topicPlayers,
    topicWagerings,
    topicLogins,
    topicWallets,
    topicPlayerStatus,
    topicUserConsentUpdate,
    topicActionTrigger,
    topicBonusTransaction
  )

  val externalTopics =
    Set(
      topicPlayersBatch,
      topicPlayers,
      topicLogins,
      topicWallets,
      topicWagerings,
      topicPlayerStatus,
      topicUserConsentUpdate,
      topicActionTrigger,
      topicBonusTransaction
    )

  val sqsGroupId = s"${prefix}-stream-app"
}
