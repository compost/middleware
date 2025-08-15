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
      defaultValue = "PT20M"
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

  val topicIn = s"greenfeather-full-players"


  val appTopics = Set(
    topicIn
  )


  val sqsGroupId = s"${prefix}-stream-app"
}
