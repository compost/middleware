package com.jada

import com.jada.configuration.ApplicationConfiguration
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import kafka.zk.AdminZkClient
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.KafkaStreams
import org.apache.zookeeper.server.command.NopCommand
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.engine.execution.BeforeEachMethodAdapter
import org.scalatest.Ignore
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest
import software.amazon.awssdk.services.sqs.model.QueueAttributeName
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest

import java.util.Optional
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import javax.inject.Inject
import scala.collection.JavaConverters._
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.junit.jupiter.params.provider.CsvFileSource
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode

import java.lang.Thread
import com.azure.storage.blob.BlobServiceClientBuilder
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApplicationTopologyTest {

  @Inject
  var config: ApplicationConfiguration = null

  @Inject
  var adminClientKafka: AdminClient = null

  @Inject
  var producer: KafkaProducer[String, String] = null

  @Inject
  var sqs: software.amazon.awssdk.services.sqs.SqsClient = null

  @Test
  def player_registration(): Unit = {
    val bodyMessage =
      s"""
{
"player_id":"51354",
"brand_id":"999",
"reg_datetime":"2023-05-12 12:58",
"first_name":"Bob",
"last_name":"The sponge",
"email":"test",
"phone_number":"test",
"language":"en",
"currency_id":"978",
"brand_description":"Bcasino_DEV",
"login_name":"login",
"domain_url":"test",
"verification_url":"defined",
"bonus_currency":"0",
"reg_bonus_amount_cash":"0",
"reg_bonus_amount_frb":"0",
"reg_supported_games_frb":"0",
"ver_bonus_amount_cash":"0",
"ver_bonus_amount_frb":"0",
"ver_supported_games_frb":"0"  
}
    """.stripMargin

    producer
      .send(new ProducerRecord(config.topicPlayers, "1", bodyMessage))
      .get(1, TimeUnit.SECONDS)

    Thread.sleep(20000)

    val messages = sqs
      .receiveMessage(
        ReceiveMessageRequest
          .builder()
          .queueUrl(config.brandQueue.get("999"))
          .maxNumberOfMessages(10)
          .build()
      )
      .messages()
    Assertions.assertEquals(
      2,
      messages.size()
    )
    Assertions.assertEquals(
      messages.get(0).body().replaceAll("\\s", ""),
      """
      {
        "type":"GENERIC_USER",
        "contactId":"51354",
        "mappingSelector":"DEV_player_registration",
        "properties":
        {
          "originalId":"51354",
          "brand_id":"999",
          "reg_date":"2023-05-12",
          "firstName":"Bob",
          "lastName":"Thesponge",
          "emailAddress":"test",
          "mobileNumber":"test",
          "language":"en",
          "brand_description":"Bcasino_DEV",
          "login_name":"login",
          "domain_url":"test",
          "verification_url":"defined",
          "reg_bonus_amount_cash":"0",
          "reg_bonus_amount_frb":"0",
          "ver_bonus_amount_cash":"0",
          "ver_bonus_amount_frb":"0",
          "reg_supported_games_frb":"0",
          "ver_supported_games_frb":"0"
      }}
      """.replaceAll("\\s", "")
    )

    sqs.deleteMessage(
      DeleteMessageRequest
        .builder()
        .receiptHandle(messages.get(0).receiptHandle())
        .queueUrl(config.brandQueue.get("999"))
        .build()
    )
    Assertions.assertEquals(
      messages.get(1).body().replaceAll("\\s", ""),
      """
      {
        "type":"GENERIC_USER",
        "mappingSelector":"DEV_player_email_verification",
        "contactId":"51354",
        "properties":
          {
            "email_verification_url":"defined",
            "brand_id":"999"
          }
      }
      """.replaceAll("\\s", "")
    )

    sqs.deleteMessage(
      DeleteMessageRequest
        .builder()
        .receiptHandle(messages.get(1).receiptHandle())
        .queueUrl(config.brandQueue.get("999"))
        .build()
    )

    // same email
    {

      val bodyMessage =
        s"""
{
"player_id":"51354",
"brand_id":"999",
"reg_datetime":"2023-05-12 12:58",
"first_name":"Bob",
"last_name":"The sponge",
"email":"test",
"phone_number":"test",
"language":"en",
"currency_id":"978",
"brand_description":"Bcasino_DEV",
"login_name":"login",
"domain_url":"test",
"verification_url":"defined",
"bonus_currency":"0",
"reg_bonus_amount_cash":"0",
"reg_bonus_amount_frb":"0",
"reg_supported_games_frb":"0",
"ver_bonus_amount_cash":"0",
"ver_bonus_amount_frb":"0",
"ver_supported_games_frb":"0"  
}
    """.stripMargin

      producer
        .send(new ProducerRecord(config.topicPlayers, "1", bodyMessage))
        .get(1, TimeUnit.SECONDS)

      Thread.sleep(20000)
      val messages = sqs
        .receiveMessage(
          ReceiveMessageRequest
            .builder()
            .queueUrl(config.brandQueue.get("999"))
            .maxNumberOfMessages(10)
            .build()
        )
        .messages()
      Assertions.assertEquals(
        0,
        messages.size(),
        s"$messages"
      )

    }

    // not same email
    {

      val bodyMessage =
        s"""
{
"player_id":"51354",
"brand_id":"999",
"reg_datetime":"2023-05-12 12:58",
"first_name":"Bob",
"last_name":"The sponge",
"email":"boom",
"phone_number":"test",
"language":"en",
"currency_id":"978",
"brand_description":"Bcasino_DEV",
"login_name":"login",
"domain_url":"test",
"verification_url":"defined",
"bonus_currency":"0",
"reg_bonus_amount_cash":"0",
"reg_bonus_amount_frb":"0",
"reg_supported_games_frb":"0",
"ver_bonus_amount_cash":"0",
"ver_bonus_amount_frb":"0",
"ver_supported_games_frb":"0"  
}
    """.stripMargin

      producer
        .send(new ProducerRecord(config.topicPlayers, "1", bodyMessage))
        .get(1, TimeUnit.SECONDS)

      Thread.sleep(20000)
      val messages = sqs
        .receiveMessage(
          ReceiveMessageRequest
            .builder()
            .queueUrl(config.brandQueue.get("999"))
            .maxNumberOfMessages(10)
            .build()
        )
        .messages()
      Assertions.assertEquals(
        1,
        messages.size()
      )

      Assertions.assertEquals(
        messages.get(0).body().replaceAll("\\s", ""),
        """
        {
          "type":"GENERIC_USER",
          "mappingSelector":"DEV_player_email_change_request",
          "contactId":"51354",
          "properties":
            {
              "emailAddress":"boom",
              "email_verification_url":"defined",
              "brand_id":"999"
            }
      }
      """.replaceAll("\\s", "")
      )
      sqs.deleteMessage(
        DeleteMessageRequest
          .builder()
          .receiptHandle(messages.get(0).receiptHandle())
          .queueUrl(config.brandQueue.get("999"))
          .build()
      )

    }

    // self exclusion
    {

      val bodyMessage =
        s"""
{
  "player_id":"51354",
  "brand_id":"999",
  "player_status_id":"3",
  "player_blocked_reason":"RG",
  "blocked_start_date":"2023-12-29 09:13:27",
  "blocked_end_date":"2028-12-29 09:13:27",
  "kafka_timestamp":"2023-12-19 09:13:27.215" 
}
    """.stripMargin

      producer
        .send(new ProducerRecord(config.topicPlayerStatus, "1", bodyMessage))
        .get(1, TimeUnit.SECONDS)

      Thread.sleep(30000) // wait for the punctuator
      val messages = sqs
        .receiveMessage(
          ReceiveMessageRequest
            .builder()
            .queueUrl(config.brandQueue.get("999"))
            .maxNumberOfMessages(10)
            .build()
        )
        .messages()
      Assertions.assertEquals(
        2,
        messages.size()
      )

      Assertions.assertEquals(
        messages.get(0).body().replaceAll("\\s", ""),
        """
        {
          "type":"GENERIC_USER",
          "mappingSelector":"DEV_self_exclusion_communication",
          "contactId":"51354",
          "properties":
            {
              "is_self_excluded_startdate":"2023-12-2909:13:27",
              "is_self_excluded_enddate":"2028-12-2909:13:27","brand_id":"999"}}
      """.replaceAll("\\s", "")
      )
      sqs.deleteMessage(
        DeleteMessageRequest
          .builder()
          .receiptHandle(messages.get(0).receiptHandle())
          .queueUrl(config.brandQueue.get("999"))
          .build()
      )

      Assertions.assertEquals(
        messages.get(1).body().replaceAll("\\s", ""),
        """
        {
          "type":"USER_BLOCKED",
          "mappingSelector":"DEV_self_exclusion",
          "contactId":"51354",
          "blockedUntil":"2028-12-29T09:13:27",
          "properties":{"is_self_excluded":"true"}}
      """.replaceAll("\\s", "")
      )
      sqs.deleteMessage(
        DeleteMessageRequest
          .builder()
          .receiptHandle(messages.get(1).receiptHandle())
          .queueUrl(config.brandQueue.get("999"))
          .build()
      )

    }
  }

  @BeforeEach
  def beforeEach(): Unit = {
    val queues = config.brandQueue.values().asScala.toSet

    queues.foreach { queue =>
      try {
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queue).build())
      } catch {
        case e: QueueDoesNotExistException => // Nop
      }
    }

    queues.map(queue => queue.substring(queue.lastIndexOf("/") + 1)).foreach {
      queue =>
        sqs.createQueue(
          CreateQueueRequest
            .builder()
            .queueName(queue)
            .attributes(Map(QueueAttributeName.FIFO_QUEUE -> "true").asJava)
            .build()
        )
    }
  }

  @BeforeAll
  def beforeAll(): Unit = {
    val client = new BlobServiceClientBuilder()
      .connectionString(config.connectionString)
      .buildClient()
    client.createBlobContainerIfNotExists(config.outputContainerName)

    val topics =
      adminClientKafka.createTopics(
        (config.appTopics ++ config.externalTopics ++ Set(config.topicOutput))
          .map(t => new NewTopic(t, 1, 1.toShort))
          .asJavaCollection
      )
  }
}
