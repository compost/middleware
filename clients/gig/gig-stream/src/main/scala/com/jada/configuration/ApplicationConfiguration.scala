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
    @ConfigProperty(name = "brand-queue") brandQueue: java.util.Map[
      String,
      String
    ],
    @ConfigProperty(
      name = "punctuator",
      defaultValue = "PT30M"
    ) punctuator: Duration,
    @ConfigProperty(
      name = "mapping-selector-dev-prefix"
    ) mappingSelectorDevPrefix: java.util.Set[
      String
    ]
) {

  val topicCustomerDetail = "customer_detail"
  val topicCustomerDetailRepartitioned =
    s"${prefix}-customer-detail-repartitioned"

  val topicWallet = "wallet"
  val topicWalletRepartitioned =
    s"${prefix}-wallet-repartitioned"

  val topicHistory = "customer-detail-history"
  val topicHistoryBlocked = "history-blocked"
  val topicHistoryConsent = "history-consent"

  val appTopics = Set(
    topicCustomerDetailRepartitioned,
    topicWalletRepartitioned,
    topicHistory,
    topicHistoryBlocked,
    topicHistoryConsent
  )

  val externalTopics =
    Set(
      topicCustomerDetail
    )

  val sqsGroupId = s"${prefix}-stream-app"
}
