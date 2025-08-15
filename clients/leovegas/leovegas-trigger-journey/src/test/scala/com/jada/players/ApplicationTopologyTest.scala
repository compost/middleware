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

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApplicationTopologyTest {

  @Inject
  var streams: KafkaStreams = null

  @ConfigProperty(name = "topic.input")
  var topicInput: String = null

  @Inject
  var config: ApplicationConfiguration = null

  @Inject
  var adminClientKafka: AdminClient = null

  @Inject
  var producer: KafkaProducer[String, String] = null

  @Inject
  var sqs: software.amazon.awssdk.services.sqs.SqsClient = null
  @Test def shouldSendAMessageInUK(): Unit = {
    val contentUK =
      """
        { "originalId": "333", 
          "mappingSelector": "ms", 
          "parameters": {"notification_PLAYER_ID":"player_id","licenseuid": "NOPE", "country":"uK","firstname":"real_json","lastname":"lastname_value"} 
        }
    """

    producer
      .send(new ProducerRecord(topicInput, "key", contentUK))
      .get(10, TimeUnit.SECONDS)

    await()
      .atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() => {
        val receiveMessageRequestUK = sqs.receiveMessage(
          ReceiveMessageRequest.builder().queueUrl(config.sqsQueueUK).build()
        )
        Assertions.assertEquals(1, receiveMessageRequestUK.messages().size())

        val receiveMessageRequestNonUK = sqs.receiveMessage(
          ReceiveMessageRequest.builder().queueUrl(config.sqsQueue).build()
        )
        Assertions.assertEquals(0, receiveMessageRequestNonUK.messages().size())
      })
  }

  @Test def shouldSendAMessageInIOM(): Unit = {
    val content =
      """
        { "originalId": "333", 
          "mappingSelector": "ms", 
          "parameters": {"notification_PLAYER_ID":"player_id", "licenseuid": "IOM", "country":"uK","firstname":"real_json","lastname":"lastname_value"} 
        }
    """

    producer
      .send(new ProducerRecord(topicInput, "key", content))
      .get(10, TimeUnit.SECONDS)

    await()
      .atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() => {
        val receiveMessageRequestUK = sqs.receiveMessage(
          ReceiveMessageRequest.builder().queueUrl(config.sqsQueueUK).build()
        )

        val receiveMessageRequestIOM = sqs.receiveMessage(
          ReceiveMessageRequest
            .builder()
            .queueUrl(config.sqsQueueLicenseuid)
            .build()
        )
        Assertions.assertEquals(1, receiveMessageRequestIOM.messages().size())

        val receiveMessageRequestNonUK = sqs.receiveMessage(
          ReceiveMessageRequest.builder().queueUrl(config.sqsQueue).build()
        )
        Assertions.assertEquals(0, receiveMessageRequestNonUK.messages().size())
      })
  }

  @Test def shouldSendAMessageInNonUK(): Unit = {
    val contentSE =
      """
        { "originalId": "333", 
          "mappingSelector": "ms", 
          "parameters": "{\"notification_PLAYER_ID\":\"player_id\", \"country\":\"SE\",\"firstname\":\"firstname_value\",\"lastname\":\"lastname_value\"}" 
        }
    """

    producer
      .send(new ProducerRecord(topicInput, "key", contentSE))
      .get(10, TimeUnit.SECONDS)

    await()
      .atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() => {
        val receiveMessageRequestUK = sqs.receiveMessage(
          ReceiveMessageRequest.builder().queueUrl(config.sqsQueueUK).build()
        )
        Assertions.assertEquals(0, receiveMessageRequestUK.messages().size())

        val receiveMessageRequestNonUK = sqs.receiveMessage(
          ReceiveMessageRequest.builder().queueUrl(config.sqsQueue).build()
        )
        Assertions.assertEquals(1, receiveMessageRequestNonUK.messages().size())
      })
  }

  @Test def shouldNotSendAMessage(): Unit = {
    val contentSE =
      """
        { "originalId": "333", 
          "mappingSelector": "ms", 
          "parameters": "" 
        }
    """

    producer
      .send(new ProducerRecord(topicInput, "key", contentSE))
      .get(10, TimeUnit.SECONDS)

    Thread.sleep(7000)
    val receiveMessageRequestUK = sqs.receiveMessage(
      ReceiveMessageRequest.builder().queueUrl(config.sqsQueueUK).build()
    )
    Assertions.assertEquals(0, receiveMessageRequestUK.messages().size())

    val receiveMessageRequestNonUK = sqs.receiveMessage(
      ReceiveMessageRequest.builder().queueUrl(config.sqsQueue).build()
    )
    Assertions.assertEquals(0, receiveMessageRequestNonUK.messages().size())
  }

  @Test def shouldSendAMessageWithUnknown(): Unit = {
    val contentUK =
      """
        { "originalId": "555", 
          "mappingSelector": "ms", 
          "parameters": "{\"country\":\"gB\",\"firstname\":\"firstname_value\",\"lastname\":\"lastname_value\"}" 
        }
    """

    producer
      .send(new ProducerRecord(topicInput, "key", contentUK))
      .get(10, TimeUnit.SECONDS)

    await()
      .atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() => {
        val receiveMessageRequestUK = sqs.receiveMessage(
          ReceiveMessageRequest.builder().queueUrl(config.sqsQueueUK).build()
        )
        Assertions.assertEquals(1, receiveMessageRequestUK.messages().size())

        val receiveMessageRequestNonUK = sqs.receiveMessage(
          ReceiveMessageRequest.builder().queueUrl(config.sqsQueue).build()
        )
        Assertions.assertEquals(0, receiveMessageRequestNonUK.messages().size())
      })
  }
  @BeforeEach
  def beforeEach(): Unit = {
    val queues = Set(
      config.sqsQueue,
      config.sqsQueueUK,
      config.sqsQueueLicenseuid
    )

    queues.foreach { queue =>
      try {
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queue).build())
      } catch {
        case e: QueueDoesNotExistException => //Nop
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
    val topics = Set(
      topicInput,
      config.topicCoordinator
    )
    adminClientKafka.deleteTopics(topics.asJavaCollection)
    adminClientKafka.deleteConsumerGroups(
      Set(config.applicationId).asJavaCollection
    )
    adminClientKafka.createTopics(
      topics.map(t => new NewTopic(t, 1, 1.toShort)).asJavaCollection
    )
    streams.start()
  }
}
