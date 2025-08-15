package com.jada.configuration

import org.eclipse.microprofile.config.inject.ConfigProperty

import javax.enterprise.context.ApplicationScoped
import java.time.Duration

@ApplicationScoped
case class ApplicationConfiguration(
    @ConfigProperty(
      name = "kafka-streams.\"application.id\"",
      defaultValue = "repartitioner-app"
    ) applicationId: String,
    @ConfigProperty(name = "prefix") prefix: String,
    @ConfigProperty(name = "startup") startup: Boolean,
    @ConfigProperty(name = "kafka-streams") configStreams: java.util.Map[
      String,
      String
    ]
) {

  val topicPlayers = "players"
  val topicPlayersRepartitioned = s"${prefix}-players-repartitioned-new"

  val topicLogins = "logins"
  val topicLoginsRepartitioned = s"${prefix}-logins-repartitioned-new"

  val topicWallets = "wallet"
  val topicWalletsRepartitioned = s"${prefix}-wallets-repartitioned-new"

  val topicWagerings = "wagering"
  val topicWageringsRepartitioned = s"${prefix}-wagerings-repartitioned-new"

  val topicSportsbets = "sportsbets"
  val topicSportsbetsRepartitioned = s"${prefix}-sportsbets-repartitioned-new"

  val topicEvents = "events"
  val topicEventsRepartitioned = s"${prefix}-events-repartitioned-new"

  val topicMarkets = "markets"
  val topicMarketsRepartitioned = s"${prefix}-markets-repartitioned-new"

  val topicExtendedPlayers = "extended_players_leovegas"
  val topicExtendedPlayersRepartitioned =
    s"${prefix}-extended-players-leovegas-repartitioned-new"

}
