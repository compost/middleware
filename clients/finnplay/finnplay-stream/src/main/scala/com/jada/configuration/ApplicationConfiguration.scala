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
    @ConfigProperty(name = "prefix") prefix: String,
    @ConfigProperty(name = "azure.connectionString") connectionString: String,
    @ConfigProperty(
      name = "azure.outputContainerName"
    ) outputContainerName: String,
    @ConfigProperty(name = "startup") startup: Boolean,
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
      name = "topic.players",
      defaultValue = "players"
    ) topicPlayers: String
) {

  val topicCoordinator = s"${prefix}-coordinator"

  val topicPlayersRepartitioned = s"${prefix}-players-repartitioned"

  val topicLogins = "logins"
  val topicLoginsRepartitioned = s"${prefix}-logins-repartitioned"

  val topicWallets = "wallet"
  val topicWalletsRepartitioned = s"${prefix}-wallets-repartitioned"

  val topicWagerings = "wagering"
  val topicWageringsRepartitioned = s"${prefix}-wagerings-repartitioned"

  val topicUserConsentUpdateFinnplay = "user_consent_update_finnplay"
  val topicUserConsentHistoryFinnplay = "user-consent-history-finnplay"
  val topicUserConsentHistoryFinnplayRepartitioned = s"${prefix}-user-consent-history-finnplay-repartitioned"
  val topicUserConsentUpdateFinnplayRepartitioned =
    s"${prefix}-user-consent-update-finnplay-repartitioned"

  val appTopics = Set(
    topicCoordinator,
    topicPlayersRepartitioned,
    topicLoginsRepartitioned,
    topicWalletsRepartitioned,
    topicWageringsRepartitioned,
    topicUserConsentUpdateFinnplayRepartitioned,
    topicUserConsentHistoryFinnplayRepartitioned,
  )
  val externalTopics =
    Set(
      topicPlayers,
      topicLogins,
      topicWallets,
      topicWagerings,
      topicUserConsentUpdateFinnplay,
      topicUserConsentHistoryFinnplay
    )

  val sqsGroupId = s"${prefix}-stream-app"
}
