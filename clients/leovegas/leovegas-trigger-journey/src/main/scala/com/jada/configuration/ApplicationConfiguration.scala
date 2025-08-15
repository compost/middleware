package com.jada.configuration

import org.eclipse.microprofile.config.inject.ConfigProperty

import javax.enterprise.context.ApplicationScoped

@ApplicationScoped
case class ApplicationConfiguration(
    @ConfigProperty(
      name = "kafka-streams.\"application.id\"",
      defaultValue = "leovegas-trigger-journey"
    ) applicationId: String,
    @ConfigProperty(name = "sqs.queue") sqsQueue: String,
    @ConfigProperty(name = "sqs.queue.uk") sqsQueueUK: String,
    @ConfigProperty(name = "sqs.queue.br") sqsQueueBR: String,
    @ConfigProperty(name = "sqs.queue.operator-nl") sqsQueueOperatorNL: String,
    @ConfigProperty(name = "sqs.queue.licenseuid-iom") sqsQueueLicenseuid: String,
    @ConfigProperty(name = "sqs.queue.group.id") sqsGroupId: String,
    @ConfigProperty(name = "startup") startup: Boolean,
    @ConfigProperty(name = "store.start-stop.name") storeStartStop: String,
    @ConfigProperty(name = "topic.coordinator") topicCoordinator: String,
    @ConfigProperty(name = "kafka-streams") configStreams: java.util.Map[
      String,
      String
    ]
)
