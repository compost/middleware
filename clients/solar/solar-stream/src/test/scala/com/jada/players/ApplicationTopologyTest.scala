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
  def shouldSendAMessageInQueue(
  ): Unit = {

    {
      val playerMessage1 =
        s"""{"player_id":"1","brand_id":"131","reg_datetime":"2023-10-02T22:22:22.222","first_name":"ELSA HELENA","last_name":"TAURIAINEN",
         |"email":"elsa.ta@luukku.com","phone_number":"+358505146945","language":"FI","affiliate_id":"655951","is_self_excluded":"false",
         |"first_dep_datetime":"2019-07-17 14:49:04","dob":"1958-07-07T00:00:00","vip":"0","test_user":"0","country_id":"246",
         |"currency_id":"978","is_suppressed":"1","brand_name":"UPPERTOLOWER","Country":"FI","VIP":null,"Postcode":"67800",
         |"IsSelfExcludedOtherBrand":"0","AccountType":"casino","BonusProgram":"1","Currency":"EUR","Duplicate":"1",
         |"GDPR":"1","Gender":"F","ReferralType":"affiliate","RegistrationPlatform":"","Username":"WZB_070758-196E",
         |"IsEmailValid":"true","FraudScore":null,"License":"curacao","Avatar":"1","BonusChosen":"First Deposit Bonus",
         |"JackpotPlayer":null,"IsVerified":"0","ZeroBounce":"valid", "isZeroBalance":"true"}
          |""".stripMargin

      producer
        .send(new ProducerRecord(config.topicPlayers, "1", playerMessage1))
        .get(1, TimeUnit.SECONDS)

      Thread.sleep(30000)

      val messages = sqs
        .receiveMessage(
          ReceiveMessageRequest
            .builder()
            .queueUrl(config.brandQueue.get("131"))
            .maxNumberOfMessages(10)
            .build()
        )
        .messages()
      Assertions.assertEquals(
        messages.size(),
        1
      )
      Assertions.assertEquals(
        messages.get(0).body().replaceAll("\\s", ""),
        """
        {"type":"GENERIC_USER","contactId":"1","mappingSelector":"DEV_player_updated","properties":{"originalId":"1","reg_date":"2023-10-0222:22:22.222","brand_id":"131","brand_name":"UPPERTOLOWER","firstName":"ELSAHELENA","lastName":"TAURIAINEN","emailAddress":"elsa.ta@luukku.com","mobileNumber":"+358505146945","language":"FI","affiliate_id":"655951","is_self_excluded":"false","first_dep_datetime":"2019-07-1714:49:04","dateOfBirth":"1958-07-07T00:00:00","country_id":"","country":"","vip":"0","currency":"","test_user":"0"}}
      """.replaceAll("\\s", "")
      )
    }

    {
      val actionTrigger = s"""
      {
        "player_id": "2",
        "brand_id": "130",
        "action":"withdrawal",
        "action_value":"SUCCESS"
      }
      """.stripMargin

      producer
        .send(
          new ProducerRecord(config.topicActionTriggers, "1", actionTrigger)
        )
        .get(1, TimeUnit.SECONDS)

      val playerMessage1 =
        s"""{"player_id":"2","brand_id":"130","reg_datetime":"2023-10-02T22:22:22.222","first_name":"ELSA HELENA","last_name":"TAURIAINEN",
         |"email":"elsa.ta@luukku.com","phone_number":"+358505146945","language":"FI","affiliate_id":"655951","is_self_excluded":"false",
         |"first_dep_datetime":"2019-07-17 14:49:04","dob":"1958-07-07T00:00:00","vip":"0","test_user":"0","country_id":"246",
         |"currency_id":"978","is_suppressed":"1","brand_name":"UPPERTOLOWER","Country":"FI","VIP":null,"Postcode":"67800",
         |"IsSelfExcludedOtherBrand":"0","AccountType":"casino","BonusProgram":"1","Currency":"EUR","Duplicate":"1",
         |"GDPR":"1","Gender":"F","ReferralType":"affiliate","RegistrationPlatform":"","Username":"WZB_070758-196E",
         |"IsEmailValid":"true","FraudScore":null,"License":"curacao","Avatar":"1","BonusChosen":"First Deposit Bonus",
         |"JackpotPlayer":null,"IsVerified":"0","ZeroBounce":"valid", "isZeroBalance":"true"}
          |""".stripMargin

      producer
        .send(new ProducerRecord(config.topicPlayers, "1", playerMessage1))
        .get(1, TimeUnit.SECONDS)

      Thread.sleep(30000)

      val messages = sqs
        .receiveMessage(
          ReceiveMessageRequest
            .builder()
            .queueUrl(config.brandQueue.get("130"))
            .maxNumberOfMessages(10)
            .build()
        )
        .messages()
      Assertions.assertEquals(
        messages.size(),
        3
      )
      Assertions.assertEquals(
        messages.get(1).body().replaceAll("\\s", ""),
        """
        {"type":"GENERIC_USER","contactId":"2","mappingSelector":"player_updated","properties":{"originalId":"2","reg_date":"2023-10-0222:22:22.222","brand_id":"130","brand_name":"130","firstName":"ELSAHELENA","lastName":"TAURIAINEN","emailAddress":"elsa.ta@luukku.com","mobileNumber":"+358505146945","language":"FI","affiliate_id":"655951","is_self_excluded":"false","first_dep_datetime":"2019-07-1714:49:04","dateOfBirth":"1958-07-07T00:00:00","country_id":"","country":"","vip":"0","currency":"","test_user":"0"}}
      """.replaceAll("\\s", "")
      )
      Assertions.assertEquals(
        messages.get(2).body().replaceAll("\\s", ""),
        """
         {"type":"GENERIC_USER","contactId":"2","mappingSelector":"player_action_triggers_withdrawal_success","properties":{"originalId":"2","brand_id":"130","withdrawalStatus":"SUCCESS"}} 
      """.replaceAll("\\s", "")
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
    val topics =
      adminClientKafka.createTopics(
        (config.appTopics ++ config.externalTopics)
          .map(t => new NewTopic(t, 1, 1.toShort))
          .asJavaCollection
      )
  }
}
