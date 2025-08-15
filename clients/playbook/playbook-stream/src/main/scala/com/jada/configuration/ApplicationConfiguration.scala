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
    ]
) {

  val topicCurrency = "currency"
  val topicCurrencyRepartitioned = s"${prefix}-currency-repartitioned"
  val topicCountries = "countries"
  val topicCountriesRepartitioned = s"${prefix}-countries-repartitioned"
  val topicPlayers = "players"
  val topicPlayersRepartitioned = s"${prefix}-players-repartitioned"

  val topicLogins = "logins"
  val topicLoginsRepartitioned = s"${prefix}-logins-repartitioned"

  val topicWallets = "wallet"
  val topicWalletsRepartitioned = s"${prefix}-wallets-repartitioned"

  val topicWagerings = "wagering"
  val topicWageringsRepartitioned = s"${prefix}-wagerings-repartitioned"

  val topicPlayerStatus = "player_status"
  val topicPlayerStatusRepartitioned =
    s"${prefix}-player-status-repartitioned"

  val topicStakeFactor = "stake_factor"
  val topicStakeFactorRepartitioned =
    s"${prefix}-stake-factor-repartitioned"

  val topicUserConsentUpdatePlaybook = "player_consent"
  val topicUserConsentUpdatePlaybookRepartitioned =
    s"${prefix}-player-consent-repartitioned"

  val topicPlayerConsentMulti = "player_consent_multi"
  val topicPlayerConsentMultiRepartitioned =
    s"${prefix}-player-consent-multi-repartitioned"

  // RAW_CRM_IMPORT_PLAYBOOK
  val topicRaw = "raw-crm-import-playbook"
  val topicCI1359BlockedReason = "ci-1359-blocked-reason"
  // RAW_CRM_PLAYER_STATUS_IMPORT_PLAYBOOK
  val topicStatusRaw = "raw-crm-player-status-import-playbook"
  val appTopics = Set(
    topicPlayersRepartitioned,
    topicLoginsRepartitioned,
    topicWalletsRepartitioned,
    topicWageringsRepartitioned,
    topicPlayerStatusRepartitioned,
    topicStakeFactorRepartitioned,
    topicUserConsentUpdatePlaybookRepartitioned,
    topicPlayerConsentMultiRepartitioned,
    topicStatusRaw,
    topicRaw,
    topicCI1359BlockedReason
  )

  val externalTopics =
    Set(
      topicPlayers,
      topicCountries,
      topicCurrency,
      topicLogins,
      topicWallets,
      topicWagerings,
      topicPlayerStatus,
      topicStakeFactor,
      topicUserConsentUpdatePlaybook,
      topicPlayerConsentMulti,
    )

  val sqsGroupId = s"${prefix}-stream-app"
}
