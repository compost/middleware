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
    @ConfigProperty(name = "kafka-streams") configStreams: java.util.Map[
      String,
      String
    ],
    @ConfigProperty(name = "sqs.queue") sqsQueue: String,
    @ConfigProperty(name = "sqs.queue.uk") sqsQueueUK: String,
    @ConfigProperty(name = "sqs.queue.br") sqsQueueBR: String,
    @ConfigProperty(name = "sqs.queue.operator-nl") sqsQueueOperatorNL: String,
    @ConfigProperty(
      name = "sqs.queue.licenseuid-iom"
    ) sqsQueueLicenseuid: String,
    @ConfigProperty(
      name = "punctuator",
      defaultValue = "PT10M"
    ) punctuator: Duration
) {

  val topicPlayersRepartitioned = "leovegas-players-repartitioned-new"
  val topicPlayersHistory = "leovegas-players-history"
  val topicWageringsRepartitioned = "leovegas-wagerings-repartitioned-new"
  val topicExtendedPlayersRepartitioned =
    "leovegas-extended-players-leovegas-repartitioned-new"

  val appTopics =
    Set(
      topicPlayersRepartitioned,
      topicWageringsRepartitioned,
      topicExtendedPlayersRepartitioned,
      topicPlayersHistory
    )

  val sqsGroupId = s"${prefix}-stream-app"
}
