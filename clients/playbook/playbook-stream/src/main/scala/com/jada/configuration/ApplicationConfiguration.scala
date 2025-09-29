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
    ],
    @ConfigProperty(
      name = "default.topology.enabled",
      defaultValue = "true"
    ) val defaultTopologyEnabled: Boolean,
    @ConfigProperty(
      name = "balance.topology.enabled",
      defaultValue = "false"
    ) val balanceTopologyEnabled: Boolean
) {

  val topicCurrency = "currency"
  val topicCurrencyRepartitioned = "currency-repartitioned"
  val topicCountries = "countries"
  val topicCountryRepartitioned = "country-repartitioned"
  val topicPlayers = "players"
  val topicLogins = "logins"
  val topicWallets = "wallet"
  val topicWagerings = "wagering"
  val topicPlayerStatus = "player_status"
  val topicStakeFactor = "stake_factor"
  val topicUserConsentUpdatePlaybook = "player_consent"
  val topicPlayerConsentMulti = "player_consent_multi"

  val appTopics = Set(
    topicPlayers,
    topicLogins,
    topicWallets,
    topicWagerings,
    topicPlayerStatus,
    topicStakeFactor,
    topicUserConsentUpdatePlaybook,
    topicPlayerConsentMulti
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
      topicPlayerConsentMulti
    )

  val sqsGroupId = s"${prefix}-stream-app"
}
