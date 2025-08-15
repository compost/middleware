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

  @ParameterizedTest
  @CsvFileSource(
    resources = Array("/shouldSendAMessageInQueue.csv"),
    quoteCharacter = '\'',
    numLinesToSkip = 1,
    delimiter = ';'
  )
  def shouldSendAMessageInQueue(
      topic: String,
      content: String,
      expectedMp: String,
      expectedOriginalId: String,
      expectedQueue: String,
      withUserBlocked: Boolean
  ): Unit = {
    producer
      .send(new ProducerRecord(topic, "key", content))
      .get(1, TimeUnit.SECONDS)

    Thread.sleep(15000)
    val receiveMessageRequest = sqs.receiveMessage(
      ReceiveMessageRequest
        .builder()
        .queueUrl(expectedQueue)
        .maxNumberOfMessages(10)
        .build()
    )

    val objMapper = new ObjectMapper()
    val msgs = receiveMessageRequest
      .messages()
      .asScala
      .toList
      .map(m => objMapper.readValue(m.body, classOf[JsonNode]))
      .filter(node => node.get("mappingSelector").textValue() != "batch-import")
    Assertions.assertFalse(msgs.isEmpty, s"${topic} ${expectedMp} ${msgs}")

    val node = msgs(0)
    Assertions
      .assertEquals(expectedMp, node.get("mappingSelector").textValue())
    Assertions.assertEquals(
      expectedOriginalId,
      node.get("properties").get("originalId").textValue(),
      node.toPrettyString()
    )

    if (withUserBlocked) {
      Assertions.assertEquals(
        2,
        msgs.size,
        s"${topic} ${expectedMp}"
      )
      val userBlocked = msgs(1)
      Assertions
        .assertEquals(
          "self_exclusion",
          userBlocked.get("mappingSelector").textValue(),
          s"${topic} ${expectedMp} ${msgs}"
        )
    } else {
      Assertions.assertEquals(
        3,
        msgs.size,
        s"${topic} ${expectedMp} ${msgs}"
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
        (config.appTopics ++ config.externalTopics ++ Set("finnplay-processor-v6-players-store-changelog", "fp-players-changelog","teammexico-player-store-changelog", "tm-players-changelog"))
          .map(t => new NewTopic(t, 1, 1.toShort))
          .asJavaCollection
      )
  }
}
