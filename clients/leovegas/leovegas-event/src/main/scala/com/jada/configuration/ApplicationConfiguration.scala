package com.jada.configuration

import org.eclipse.microprofile.config.inject.ConfigProperty

import javax.enterprise.context.ApplicationScoped

@ApplicationScoped
case class ApplicationConfiguration(
    @ConfigProperty(
      name = "kafka-streams.\"application.id\"",
      defaultValue = "leovegas-event-200013089"
    ) applicationId: String,
    @ConfigProperty(name = "sqs.queue") sqsQueue: String,
    @ConfigProperty(name = "sqs.queue.uk") sqsQueueUK: String,
    @ConfigProperty(name = "sqs.queue.br") sqsQueueBR: String,
    @ConfigProperty(name = "sqs.queue.licenseuid") sqsQueueLicenseuid: String,
    @ConfigProperty(name = "sqs.queue.nl") sqsQueueOperatorNL: String,
    @ConfigProperty(name = "punctuation.seconds") punctuationSeconds: Int,
    @ConfigProperty(name = "sqs.queue.group.id") sqsGroupId: String,
    @ConfigProperty(
      name = "championships"
    ) championships: java.util.Map[String, String],
    @ConfigProperty(name = "startup") startup: Boolean,
    @ConfigProperty(name = "kafka-streams") configStreams: java.util.Map[
      String,
      String
    ]
)
