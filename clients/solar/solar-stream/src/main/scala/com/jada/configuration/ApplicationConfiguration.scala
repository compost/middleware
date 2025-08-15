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
    @ConfigProperty(name = "prefix") prefix: String,
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
      name = "mapping-selector-prefix"
    ) mappingSelectorPrefix: java.util.Set[
      String
    ]
) {

  val topicCoordinator = s"${prefix}-coordinator"

  val topicPlayers = "players"
  val topicPlayersRepartitioned = s"${prefix}-players-repartitioned"

  val topicLogins = "logins"
  val topicLoginsRepartitioned = s"${prefix}-logins-repartitioned"



  val topicActionTriggers = "action_triggers"
  val topicActionTriggersRepartitioned =
    s"${prefix}-action-triggers-repartitioned"

  val appTopics = Set(
    topicCoordinator,
    topicPlayersRepartitioned,
    topicActionTriggersRepartitioned
  )
  val externalTopics =
    Set(
      topicPlayers,
      topicActionTriggers
    )

  val sqsGroupId = s"${prefix}-stream-app"
}
