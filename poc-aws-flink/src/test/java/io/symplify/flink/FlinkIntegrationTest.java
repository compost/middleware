package io.symplify.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import io.symplify.streams.Configuration.Topic;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class FlinkIntegrationTest {

    public static KafkaContainer kafka;
    public static LocalStackContainer localstack;
    private static boolean containersInitialized = false;

    static {
        try {
            System.out.println("DEBUG: DOCKER_HOST=" + System.getenv("DOCKER_HOST"));
            kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
            localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.3"))
                    .withServices(LocalStackContainer.Service.SQS);
            containersInitialized = true;
        } catch (Throwable t) {
            System.err.println("Failed to initialize containers: " + t.getMessage());
        }
    }

    private static SqsClient sqsClient;
    private static String queueUrl;
    private static final String BRAND = "leovegas";
    private static final String QUEUE_NAME = "leovegas-queue";

    @BeforeAll
    public static void setup() {
        Assumptions.assumeTrue(containersInitialized, "Docker containers failed to initialize");
        
        kafka.start();
        localstack.start();

        sqsClient = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                ))
                .region(software.amazon.awssdk.regions.Region.of(localstack.getRegion()))
                .build();

        queueUrl = sqsClient.createQueue(CreateQueueRequest.builder().queueName(QUEUE_NAME).build()).queueUrl();
    }
    
    @AfterAll
    public static void tearDown() {
        if (localstack != null) localstack.stop();
        if (kafka != null) kafka.stop();
    }

    @Test
    public void testPlayerRegistration() throws Exception {
        // 1. Produce message to Kafka
        produceMessage(Topic.PLAYERS, "player-123", "{"
                + "\"player_id\": \"player-123\","
                + "\"brand_id\": \"" + BRAND + "\","
                + "\"reg_datetime\": \"2023-01-01T12:00:00Z\","
                + "\"email\": \"test@example.com\""
                + "}");

        // 2. Run Flink Job in a separate thread
        CompletableFuture.runAsync(() -> {
            try {
                FlinkJob.main(new String[]{
                        "--bootstrap.servers", kafka.getBootstrapServers(),
                        "--group.id", "test-group",
                        "--brand-queues", BRAND + "=" + queueUrl,
                        "--sqs.endpoint", localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString(),
                        "--aws.region", localstack.getRegion()
                });
            } catch (Exception e) {
                // Flink job might fail when test finishes and containers stop, which is expected
            }
        });

        // 3. Verify message in SQS
        await().atMost(Duration.ofMinutes(2)).untilAsserted(() -> {
            var messages = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(1)
                    .build())
                    .messages();

            assertThat(messages).isNotEmpty();
            assertThat(messages.get(0).body()).contains("player-123");
            assertThat(messages.get(0).body()).contains("player_registration");
        });
    }

    private void produceMessage(String topic, String key, String value) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, key, value.getBytes()));
            producer.flush();
        }
    }
}
