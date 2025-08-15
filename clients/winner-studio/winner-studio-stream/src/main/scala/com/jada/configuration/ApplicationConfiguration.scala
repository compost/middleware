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
    ]
) {

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
    s"${prefix}-player-status-winner-studio-repartitioned"

  val topicUserConsentUpdateWinnerStudio = "player_consent"
  val topicUserConsentUpdateWinnerStudioRepartitioned =
    s"${prefix}-user-consent-update-winner-studio-repartitioned"


  val appTopics = Set(
    topicPlayersRepartitioned,
    topicLoginsRepartitioned,
    topicWalletsRepartitioned,
    topicWageringsRepartitioned,
    topicPlayerStatusRepartitioned,
    topicUserConsentUpdateWinnerStudioRepartitioned
  )

  val externalTopics =
    Set(
      topicPlayers,
      topicLogins,
      topicWallets,
      topicWagerings,
      topicPlayerStatus,
      topicUserConsentUpdateWinnerStudio
    )

  val sqsGroupId = s"${prefix}-stream-app"
}
