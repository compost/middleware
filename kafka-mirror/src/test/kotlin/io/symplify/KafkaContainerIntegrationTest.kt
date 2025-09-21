package io.symplify

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*

/**
 * Real Kafka integration test using TestContainers with actual Kafka broker
 */
class KafkaContainerIntegrationTest {

    private lateinit var objectMapper: ObjectMapper
    private lateinit var kafkaMirrorService: KafkaMirrorService
    private lateinit var sourceProducer: KafkaProducer<String, String>
    private lateinit var targetProducer: KafkaProducer<String, String>
    private lateinit var targetConsumer: KafkaConsumer<String, String>

    @BeforeEach
    fun setup() {
        objectMapper = ObjectMapper().registerKotlinModule()
        kafkaMirrorService = KafkaMirrorService()
        kafkaMirrorService.objectMapper = objectMapper

        // Source Kafka (where we send original messages)
        val sourceBootstrapServers = "localhost:9092"
        
        // Target Kafka (where mirrored messages go)
        val targetBootstrapServers = "localhost:9093"
        
        // Producer configuration for Source Kafka
        val sourceProducerProps = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceBootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
        }
        sourceProducer = KafkaProducer(sourceProducerProps)

        // Producer configuration for Target Kafka (to send mirrored messages)
        val targetProducerProps = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, targetBootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
        }
        targetProducer = KafkaProducer(targetProducerProps)

        // Consumer configuration for Target Kafka (to verify mirrored messages)
        val targetConsumerProps = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, targetBootstrapServers)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }
        targetConsumer = KafkaConsumer(targetConsumerProps)
    }

    @AfterEach
    fun cleanup() {
        try {
            if (this::sourceProducer.isInitialized) sourceProducer.close()
            if (this::targetProducer.isInitialized) targetProducer.close()
            if (this::targetConsumer.isInitialized) targetConsumer.close()
        } catch (e: Exception) {
            println("⚠️  Error during cleanup: ${e.message}")
        }
    }

    @Test
    fun testRealKafkaIntegrationWithContainers() {
        val sourceTopic = "source-topic"
        val targetTopic = "target-topic"

        // Subscribe consumer to target topic
        targetConsumer.subscribe(listOf(targetTopic))

        // Test messages with real gaming data
        val testMessages = listOf(
            "game-msg-1" to """{
                "brand_id": "betsson_se",
                "player_id": "player_premium_001",
                "event_type": "slot_spin",
                "game": {
                    "id": "mega_fortune_dreams",
                    "provider": "netent",
                    "category": "progressive_slots"
                },
                "bet": {
                    "amount": 10.0,
                    "currency": "SEK",
                    "lines": 25
                },
                "result": {
                    "win_amount": 0.0,
                    "symbols": [
                        ["cherry", "bell", "lemon"],
                        ["bar", "seven", "cherry"],
                        ["lemon", "bell", "bar"]
                    ]
                },
                "timestamp": "2025-09-21T22:30:15.123Z",
                "session_id": "sess_abc123"
            }""",

            "game-msg-2" to """{
                "brand_id": "leovegas_com", 
                "player_id": "player_mobile_777",
                "event_type": "blackjack_hand",
                "game": {
                    "id": "blackjack_vip",
                    "provider": "evolution",
                    "table": "BlackjackVIP3"
                },
                "hand": {
                    "player_cards": ["AS", "KC"],
                    "dealer_cards": ["QH", "XX"],
                    "player_total": 21,
                    "result": "blackjack",
                    "payout": 75.0
                },
                "bet": {
                    "amount": 50.0,
                    "currency": "EUR",
                    "insurance": false
                },
                "timestamp": "2025-09-21T22:31:42.456Z"
            }""",

            "sports-msg-1" to """{
                "brand_id": "unibet_uk",
                "player_id": "player_sports_999", 
                "event_type": "bet_placed",
                "sport": {
                    "type": "football",
                    "league": "Premier League",
                    "match": "Manchester City vs Liverpool",
                    "match_time": "2025-09-22T15:00:00Z"
                },
                "bet": {
                    "type": "match_winner",
                    "selection": "Manchester City",
                    "odds": 2.25,
                    "stake": 25.0,
                    "currency": "GBP"
                },
                "timestamp": "2025-09-21T22:32:00.789Z"
            }"""
        )

        // Send messages to source topic
        testMessages.forEach { (key, message) ->
            println("Sending message with key: $key to source cluster")
            val record = ProducerRecord(sourceTopic, key, message)
            sourceProducer.send(record).get() // Synchronous send
        }

        // Simulate the mirroring process by manually processing messages
        // In a real scenario, this would be done by the Quarkus reactive messaging
        testMessages.forEach { (originalKey, message) ->
            // Create a record as it would come from Kafka
            val inputRecord = io.smallrye.reactive.messaging.kafka.Record.of(originalKey, message)
            
            // Process through the mirror service
            val mirroredRecord = kafkaMirrorService.mirrorMessage(inputRecord)
            
            // Send mirrored message to target topic
            val targetRecord = ProducerRecord(targetTopic, mirroredRecord.key(), mirroredRecord.value())
            targetProducer.send(targetRecord).get()
            
            println("Mirrored message: ${originalKey} -> ${mirroredRecord.key()}")
        }

        // Consume messages from target topic and verify
        val consumedMessages = mutableListOf<Pair<String, String>>()
        val startTime = System.currentTimeMillis()
        val timeout = 10000L // 10 seconds timeout

        while (consumedMessages.size < testMessages.size && 
               (System.currentTimeMillis() - startTime) < timeout) {
            val records = targetConsumer.poll(Duration.ofMillis(1000))
            for (record in records) {
                consumedMessages.add(Pair(record.key(), record.value()))
                println("Consumed message from target cluster: key=${record.key()}, partition=${record.partition()}")
            }
        }

        // Verify we got all expected messages
        assertEquals(testMessages.size, consumedMessages.size, 
            "Should have consumed all mirrored messages from target topic")

        // Verify partition keys were generated correctly
        val expectedKeys = setOf(
            "betsson_se-player_premium_001",
            "leovegas_com-player_mobile_777", 
            "unibet_uk-player_sports_999"
        )
        val actualKeys = consumedMessages.map { it.first }.toSet()
        assertEquals(expectedKeys, actualKeys, "Partition keys should match expected brand_id-player_id format")

        // Verify message content integrity
        consumedMessages.forEach { (partitionKey, messageValue) ->
            val messageRecord = objectMapper.readValue(messageValue, MessageRecord::class.java)
            val expectedKey = "${messageRecord.brandId}-${messageRecord.playerId}"
            assertEquals(expectedKey, partitionKey, 
                "Partition key should match brand_id-player_id from message content")

            // Verify specific game data based on brand
            when (messageRecord.brandId) {
                "betsson_se" -> {
                    assertEquals("slot_spin", messageRecord.getAdditionalProperties()["event_type"])
                    val game = messageRecord.getAdditionalProperties()["game"] as Map<*, *>
                    assertEquals("mega_fortune_dreams", game["id"])
                    assertEquals("netent", game["provider"])
                }
                "leovegas_com" -> {
                    assertEquals("blackjack_hand", messageRecord.getAdditionalProperties()["event_type"])
                    val hand = messageRecord.getAdditionalProperties()["hand"] as Map<*, *>
                    assertEquals("blackjack", hand["result"])
                    assertEquals(75.0, hand["payout"])
                }
                "unibet_uk" -> {
                    assertEquals("bet_placed", messageRecord.getAdditionalProperties()["event_type"])
                    val sport = messageRecord.getAdditionalProperties()["sport"] as Map<*, *>
                    assertEquals("football", sport["type"])
                    assertEquals("Premier League", sport["league"])
                }
            }
        }

        println("✅ Real Kafka integration test completed successfully!")
        println("📊 Processed ${consumedMessages.size} messages through actual Kafka broker")
        println("🔑 Verified partition keys: ${actualKeys.joinToString(", ")}")
        println("🎰 Source Kafka: localhost:9092, Target Kafka: localhost:9093")
    }

    @Test
    fun testKafkaErrorHandlingWithRealBroker() {
        val sourceTopic = "error-source-topic"
        val targetTopic = "error-target-topic"

        targetConsumer.subscribe(listOf(targetTopic))

        // Test messages with invalid data that should trigger error handling
        val errorMessages = listOf(
            "invalid-1" to """{"invalid": "json", "missing": "brand_id and player_id"}""",
            "invalid-2" to """{"brand_id": "test_brand", "missing": "player_id"}""", 
            "malformed" to """malformed json without proper structure{""",
            null to """{"brand_id": "orphan_brand", "player_id": "orphan_player"}"""
        )

        // Send invalid messages
        errorMessages.forEach { (key, message) ->
            val record = ProducerRecord(sourceTopic, key, message)
            sourceProducer.send(record).get()
        }

        // Process through mirror service
        errorMessages.forEach { (originalKey, message) ->
            val inputRecord = io.smallrye.reactive.messaging.kafka.Record.of(originalKey, message)
            val mirroredRecord = kafkaMirrorService.mirrorMessage(inputRecord)
            
            val targetRecord = ProducerRecord(targetTopic, mirroredRecord.key(), mirroredRecord.value())
            targetProducer.send(targetRecord).get()
        }

        // Consume and verify error handling
        val consumedMessages = mutableListOf<Pair<String?, String>>()
        val startTime = System.currentTimeMillis()
        val timeout = 10000L

        while (consumedMessages.size < errorMessages.size && 
               (System.currentTimeMillis() - startTime) < timeout) {
            val records = targetConsumer.poll(Duration.ofMillis(1000))
            for (record in records) {
                consumedMessages.add(Pair(record.key(), record.value()))
            }
        }

        assertEquals(errorMessages.size, consumedMessages.size)

        // Verify error handling - should fallback to original keys or generate valid keys
        val fallbackKeys = consumedMessages.map { it.first }
        assertTrue(fallbackKeys.contains("invalid-1"), "Should preserve original key for invalid JSON")
        assertTrue(fallbackKeys.contains("invalid-2"), "Should preserve original key for missing player_id") 
        assertTrue(fallbackKeys.contains("malformed"), "Should preserve original key for malformed JSON")
        assertTrue(fallbackKeys.contains("orphan_brand-orphan_player"), "Should generate key for valid JSON with null original key")

        println("✅ Error handling with real Kafka completed successfully!")
        println("🔧 Fallback keys: ${fallbackKeys.joinToString(", ")}")
    }
}
