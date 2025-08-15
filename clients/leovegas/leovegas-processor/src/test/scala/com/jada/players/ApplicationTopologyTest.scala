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
  def shouldTest(): Unit = {
    {
      val playerMessage1 =
        s"""{"player_id":"1","brand_id":"5","reg_datetime":"2023-10-23T22:22:22.222","first_name":"ELSA HELENA","last_name":"TAURIAINEN",
         |"email":"elsa.ta@luukku.com","phone_number":"+358505146945","language":"FI","affiliate_id":"655951","is_self_excluded":"false",
         |"first_dep_datetime":"2019-07-17 14:49:04","dob":"1958-07-07T00:00:00","vip":"0","test_user":"0","country_id":"826",
         |"currency_id":"978","is_suppressed":"1","brand_name":"UPPERTOLOWER","Country":"FI","VIP":null,"Postcode":"67800",
         |"IsSelfExcludedOtherBrand":"0","AccountType":"casino","BonusProgram":"1","Currency":"EUR","Duplicate":"1",
         |"GDPR":"1","Gender":"F","ReferralType":"affiliate","RegistrationPlatform":"","Username":"WZB_070758-196E",
         |"IsEmailValid":"true","FraudScore":null,"License":"curacao","Avatar":"1","BonusChosen":"First Deposit Bonus",
         |"JackpotPlayer":null,"IsVerified":"0","ZeroBounce":"valid", "isZeroBalance":"true"}
          |""".stripMargin

      producer
        .send(
          new ProducerRecord(
            config.topicPlayersRepartitioned,
            "5-1",
            playerMessage1
          )
        )
        .get(1, TimeUnit.SECONDS)

      producer
        .send(
          new ProducerRecord(
            config.topicExtendedPlayersRepartitioned,
            "5-1",
            """{"originalId":"1","brand_id":"5","licenseuid":"AAMS","operator":"Gutro"}"""
          )
        )
        .get(1, TimeUnit.SECONDS)

      producer.send(
        new ProducerRecord(
          config.topicWageringsRepartitioned,
          "5-1",
          """
        {"bet_id":"bet_1","brand_id":"5","player_id":"1","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":"6","price":"153.99","bet_datetime":"2023-10-24T10:23:26.999","currency_id":100,"has_resulted":"false","result_datetime":null,"result_id":"0"} 
      """
        )
      )

      producer.send(
        new ProducerRecord(
          config.topicWageringsRepartitioned,
          "5-1",
          """
        {"bet_id":"bet_2","brand_id":"5","player_id":"1","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":"7","price":"153.99","bet_datetime":"2023-10-24T14:23:26.999","currency_id":100,"has_resulted":"false","result_datetime":null,"result_id":"0"} 
      """
        )
      )

      producer.send(
        new ProducerRecord(
          config.topicWageringsRepartitioned,
          "5-1",
          """
        {"bet_id":"bet_2","brand_id":"5","player_id":"1","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":"7","price":"153.99","bet_datetime":"2023-10-24T14:23:26.999","currency_id":100,"has_resulted":"true","result_datetime":"2023-10-24T14:23:26.999","result_id":"0"} 
      """
        )
      )

      producer.send(
        new ProducerRecord(
          config.topicWageringsRepartitioned,
          "5-1",
          """
        {"bet_id":"bet_1","brand_id":"5","player_id":"1","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":"6","price":"153.99","bet_datetime":"2023-10-24T10:23:26.999","currency_id":100,"has_resulted":"true","result_datetime":"2023-10-24T10:23:26.999","result_id":"0"} 
      """
        )
      )

      Thread.sleep(20000)

      val messages = sqs
        .receiveMessage(
          ReceiveMessageRequest
            .builder()
            .queueUrl(config.sqsQueueUK)
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
       {
          "type":"GENERIC_USER",
          "mappingSelector":"LeoVegas_first_winning_bet",
          "contactId":"1",
          "properties":{
             "bet_id":"bet_1",
              "bet_amount":"0",
              "result":"0",
              "vertical":"6" 
           }
       }
      """.replaceAll("\\s", "")
      )
    }
    {
      val playerMessage1 =
        s"""{"player_id":"3","brand_id":"5","reg_datetime":"2023-10-23T22:22:22.222","first_name":"ELSA HELENA","last_name":"TAURIAINEN",
         |"email":"elsa.ta@luukku.com","phone_number":"+358505146945","language":"FI","affiliate_id":"655951","is_self_excluded":"false",
         |"first_dep_datetime":"2019-07-17 14:49:04","dob":"1958-07-07T00:00:00","vip":"0","test_user":"0","country_id":"246",
         |"currency_id":"978","is_suppressed":"1","brand_name":"UPPERTOLOWER","Country":"FI","VIP":null,"Postcode":"67800",
         |"IsSelfExcludedOtherBrand":"0","AccountType":"casino","BonusProgram":"1","Currency":"EUR","Duplicate":"1",
         |"GDPR":"1","Gender":"F","ReferralType":"affiliate","RegistrationPlatform":"","Username":"WZB_070758-196E",
         |"IsEmailValid":"true","FraudScore":null,"License":"curacao","Avatar":"1","BonusChosen":"First Deposit Bonus",
         |"JackpotPlayer":null,"IsVerified":"0","ZeroBounce":"valid", "isZeroBalance":"true"}
          |""".stripMargin

      producer
        .send(
          new ProducerRecord(
            config.topicPlayersRepartitioned,
            "5-3",
            playerMessage1
          )
        )
        .get(1, TimeUnit.SECONDS)

      producer
        .send(
          new ProducerRecord(
            config.topicExtendedPlayersRepartitioned,
            "5-3",
            """{"originalId":"3","brand_id":"5","licenseuid":"AAMS","operator":"Gutro"}"""
          )
        )
        .get(1, TimeUnit.SECONDS)

      producer.send(
        new ProducerRecord(
          config.topicWageringsRepartitioned,
          "5-3",
          """
        {"bet_id":"bet_1","brand_id":"5","player_id":"3","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":"6","price":"153.99","bet_datetime":"2023-10-24T10:23:26.999","currency_id":100,"has_resulted":"false","result_datetime":null,"result_id":"0"} 
      """
        )
      )

      producer.send(
        new ProducerRecord(
          config.topicWageringsRepartitioned,
          "5-3",
          """
        {"bet_id":"bet_2","brand_id":"5","player_id":"3","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":"7","price":"153.99","bet_datetime":"2023-10-24T14:23:26.999","currency_id":100,"has_resulted":"false","result_datetime":null,"result_id":"0"} 
      """
        )
      )

      producer.send(
        new ProducerRecord(
          config.topicWageringsRepartitioned,
          "5-3",
          """
        {"bet_id":"bet_2","brand_id":"5","player_id":"3","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":"7","price":"153.99","bet_datetime":"2023-10-24T14:23:26.999","currency_id":100,"has_resulted":"true","result_datetime":"2023-10-24T14:23:26.999","result_id":"1"} 
      """
        )
      )

      producer.send(
        new ProducerRecord(
          config.topicWageringsRepartitioned,
          "5-3",
          """
        {"bet_id":"bet_1","brand_id":"5","player_id":"3","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":"6","price":"153.99","bet_datetime":"2023-10-24T10:23:26.999","currency_id":100,"has_resulted":"true","result_datetime":"2023-10-24T10:23:26.999","result_id":"2"} 
      """
        )
      )

      Thread.sleep(30000)

      val messages = sqs
        .receiveMessage(
          ReceiveMessageRequest
            .builder()
            .queueUrl(config.sqsQueue)
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
       {
          "type":"GENERIC_USER",
          "mappingSelector":"LeoVegas_first_losing_bet",
          "contactId":"3",
          "properties":{
             "bet_id":"bet_2",
              "bet_amount":"0",
              "result":"1",
              "vertical":"7" 
           }
       }
      """.replaceAll("\\s", "")
      )
    }
  }

  @BeforeEach
  def beforeEach(): Unit = {
    val queues = Set(
      config.sqsQueue,
      config.sqsQueueUK,
      config.sqsQueueOperatorNL,
      config.sqsQueueLicenseuid
    )

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
        (config.appTopics)
          .map(t => new NewTopic(t, 1, 1.toShort))
          .asJavaCollection
      )
  }
}
