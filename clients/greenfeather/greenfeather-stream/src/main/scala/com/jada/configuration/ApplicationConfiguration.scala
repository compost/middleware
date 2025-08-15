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
  val topicPlayersRepartitioned = s"${prefix}-players-repartitioned"

  val topicPlayersBatch = "player_batch"
  val topicPlayersBatchRepartitioned = s"${prefix}-player-batch-repartitioned"

  val topicLogins = "logins"
  val topicLoginsRepartitioned = s"${prefix}-logins-repartitioned"

  val topicWallets = "wallet"
  val topicWalletsRepartitioned = s"${prefix}-wallets-repartitioned"

  val topicWagerings = "wagering"
  val topicWageringsRepartitioned = s"${prefix}-wagerings-repartitioned"

  val topicPlayerStatus = "player_status"
  val topicPlayerStatusRepartitioned =
    s"${prefix}-player-status-repartitioned"

  val topicUserConsentUpdate = "player_consent"
  val topicUserConsentUpdateRepartitioned =
    s"${prefix}-player-consent-repartitioned"

  val topicActionTrigger = "action_triggers"
  val topicActionTriggerRepartitioned =
    s"${prefix}-action-triggers-repartitioned"

  val topicBonusTransaction = "bonus_transaction"
  val topicBonusTransactionRepartitioned =
    s"${prefix}-bonus-transaction-repartitioned"

  val appTopics = Set(
    topicPlayersBatchRepartitioned,
    topicPlayersRepartitioned,
    topicWageringsRepartitioned,
    topicLoginsRepartitioned,
    topicWalletsRepartitioned,
    topicPlayerStatusRepartitioned,
    topicUserConsentUpdateRepartitioned,
    topicActionTriggerRepartitioned,
    topicBonusTransactionRepartitioned
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
