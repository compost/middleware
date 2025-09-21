package io.symplify

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.smallrye.reactive.messaging.kafka.Record
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach

/**
 * Integration test that simulates the full Kafka mirroring workflow
 * without requiring actual Kafka containers (for environments where Docker is not available)
 */
class KafkaIntegrationTest {

    private lateinit var objectMapper: ObjectMapper
    private lateinit var kafkaMirrorService: KafkaMirrorService

    @BeforeEach
    fun setup() {
        objectMapper = ObjectMapper().registerKotlinModule()
        kafkaMirrorService = KafkaMirrorService()
        kafkaMirrorService.objectMapper = objectMapper
    }

    @Test
    fun testEndToEndKafkaMirroringWorkflow() {

        // Simulate realistic gaming event messages as they would come from source Kafka topic
        val gameEvents = listOf(
            Triple("original-key-1", """{
                "brand_id": "betsson_se",
                "player_id": "player_vip_001",
                "event_type": "bet_placed",
                "game": {
                    "id": "blackjack_premium",
                    "provider": "evolution",
                    "table": "BlackjackVIP1"
                },
                "bet": {
                    "amount": 250.0,
                    "currency": "SEK",
                    "side_bets": ["perfect_pairs", "21_plus_3"]
                },
                "timestamp": "2025-09-21T21:45:12.123Z",
                "session_id": "sess_abc123"
            }""", "betsson_se-player_vip_001"),
            
            Triple("original-key-2", """{
                "brand_id": "leovegas_com",
                "player_id": "player_mobile_999",
                "event_type": "spin_result",
                "game": {
                    "id": "starburst_netent",
                    "provider": "netent",
                    "category": "video_slots"
                },
                "spin": {
                    "bet_amount": 10.0,
                    "currency": "EUR",
                    "win_amount": 75.0,
                    "multiplier": 7.5,
                    "symbols": [
                        ["starburst", "bar", "seven"],
                        ["starburst", "starburst", "starburst"],
                        ["bell", "starburst", "cherry"]
                    ]
                },
                "timestamp": "2025-09-21T21:46:30.456Z",
                "balance_after": 1275.50
            }""", "leovegas_com-player_mobile_999"),
            
            Triple("original-key-3", """{
                "brand_id": "unibet_uk",
                "player_id": "player_sports_777",
                "event_type": "bet_settlement",
                "sport": {
                    "type": "football",
                    "match": "Arsenal vs Chelsea",
                    "league": "Premier League",
                    "match_id": "epl_2025_week8_001"
                },
                "bet": {
                    "type": "match_winner",
                    "selection": "Arsenal",
                    "odds": 2.10,
                    "stake": 50.0,
                    "currency": "GBP"
                },
                "settlement": {
                    "status": "won",
                    "payout": 105.0,
                    "profit": 55.0
                },
                "timestamp": "2025-09-21T23:00:00.000Z"
            }""", "unibet_uk-player_sports_777")
        )

        // Process each message through the mirroring service (simulates Quarkus reactive messaging)
        val mirroredMessages = mutableListOf<Pair<String, String>>()
        
        gameEvents.forEach { (originalKey, eventJson, expectedKey) ->
            println("Processing message with original key: $originalKey")
            
            // Create Kafka record as it would come from source topic
            val inputRecord = Record.of(originalKey, eventJson) as Record<String?, String>
            
            // Process through mirroring service
            val mirroredRecord = kafkaMirrorService.mirrorMessage(inputRecord)
            
            // Verify the partition key was generated correctly
            assertEquals(expectedKey, mirroredRecord.key(), 
                "Partition key should be generated from brand_id-player_id")
            
            // Store mirrored message (simulates sending to target topic)
            mirroredMessages.add(Pair(mirroredRecord.key(), mirroredRecord.value()))
            println("Generated partition key: ${mirroredRecord.key()}")
        }

        // Verify all messages were processed correctly
        assertEquals(3, mirroredMessages.size, "Should process all 3 messages")

        // Verify message content integrity and partition key correctness
        mirroredMessages.forEach { (partitionKey, messageValue) ->
            val messageRecord = objectMapper.readValue(messageValue, MessageRecord::class.java)
            val expectedKey = "${messageRecord.brandId}-${messageRecord.playerId}"
            assertEquals(expectedKey, partitionKey, "Partition key should match brand_id-player_id pattern")
            
            // Verify the message can be properly deserialized and contains expected data
            assertTrue(messageRecord.getAdditionalProperties().containsKey("event_type"))
            assertTrue(messageRecord.getAdditionalProperties().containsKey("timestamp"))
            
            when (messageRecord.brandId) {
                "betsson_se" -> {
                    assertEquals("bet_placed", messageRecord.getAdditionalProperties()["event_type"])
                    val game = messageRecord.getAdditionalProperties()["game"] as Map<*, *>
                    assertEquals("blackjack_premium", game["id"])
                    val bet = messageRecord.getAdditionalProperties()["bet"] as Map<*, *>
                    assertEquals(250.0, bet["amount"])
                    assertEquals("SEK", bet["currency"])
                }
                "leovegas_com" -> {
                    assertEquals("spin_result", messageRecord.getAdditionalProperties()["event_type"])
                    val spin = messageRecord.getAdditionalProperties()["spin"] as Map<*, *>
                    assertEquals(75.0, spin["win_amount"])
                    assertEquals(7.5, spin["multiplier"])
                    val symbols = spin["symbols"] as List<*>
                    assertEquals(3, symbols.size)
                }
                "unibet_uk" -> {
                    assertEquals("bet_settlement", messageRecord.getAdditionalProperties()["event_type"])
                    val settlement = messageRecord.getAdditionalProperties()["settlement"] as Map<*, *>
                    assertEquals("won", settlement["status"])
                    assertEquals(105.0, settlement["payout"])
                }
            }
        }

        println("✅ End-to-end Kafka mirroring workflow test completed successfully!")
        println("📊 Processed ${mirroredMessages.size} messages with correct partition keys")
        println("🔑 Generated partition keys: ${mirroredMessages.map { it.first }}")
    }

    @Test
    fun testKafkaMirroringErrorHandlingWorkflow() {
        // Test invalid messages that should fall back to original keys
        val invalidMessages = listOf(
            Triple("fallback-key-1", """{"invalid": "json", "missing": "brand_id and player_id"}""", "fallback-key-1"),
            Triple("fallback-key-2", """{"brand_id": "test_brand", "missing": "player_id"}""", "fallback-key-2"),
            Triple("fallback-key-3", """malformed json that cannot be parsed""", "fallback-key-3"),
            Triple(null, """{"brand_id": "orphan_brand", "player_id": "orphan_player"}""", "orphan_brand-orphan_player")
        )

        invalidMessages.forEach { (originalKey, invalidJson, expectedFallbackKey) ->
            println("Processing invalid message with original key: $originalKey")
            
            // Create Kafka record as it would come from source topic
            val inputRecord = Record.of(originalKey, invalidJson) as Record<String?, String>
            
            // Process through mirroring service (should handle errors gracefully)
            val mirroredRecord = kafkaMirrorService.mirrorMessage(inputRecord)
            
            // For invalid messages, should fall back to original key or "unknown"
            assertEquals(expectedFallbackKey, mirroredRecord.key(), 
                "Invalid messages should preserve original key or fallback to 'unknown'")
            
            println("Fallback key: ${mirroredRecord.key()}")
        }

        println("✅ Error handling workflow test completed successfully!")
    }

    @Test
    fun testFullIntegrationWorkflowWithMixedMessages() {
        // Test a mix of valid and invalid messages as would occur in production
        val mixedMessages = listOf(
            // Valid messages
            Triple("msg-1", """{
                "brand_id": "pokerstars_com",
                "player_id": "poker_pro_123",
                "event_type": "tournament_entry",
                "tournament": {
                    "id": "sunday_storm_2025",
                    "buy_in": 11.0,
                    "currency": "USD",
                    "guaranteed_prize": 300000
                }
            }""", "pokerstars_com-poker_pro_123"),
            
            // Invalid message - missing player_id
            Triple("msg-2", """{
                "brand_id": "invalid_brand",
                "event_type": "invalid_event"
            }""", "msg-2"),
            
            // Valid message
            Triple("msg-3", """{
                "brand_id": "bwin_com",
                "player_id": "casual_gamer_456",
                "event_type": "deposit",
                "transaction": {
                    "amount": 100.0,
                    "currency": "EUR",
                    "method": "credit_card",
                    "processor": "stripe"
                }
            }""", "bwin_com-casual_gamer_456"),
            
            // Malformed JSON
            Triple("msg-4", """{"malformed": json without closing brace""", "msg-4")
        )

        val results = mutableListOf<Triple<String, String, Boolean>>()
        
        mixedMessages.forEach { (originalKey, messageJson, expectedKey) ->
            val inputRecord = Record.of(originalKey, messageJson) as Record<String?, String>
            val mirroredRecord = kafkaMirrorService.mirrorMessage(inputRecord)
            
            val isValidMessage = try {
                val messageRecord = objectMapper.readValue(mirroredRecord.value(), MessageRecord::class.java)
                messageRecord.brandId.isNotEmpty() && messageRecord.playerId.isNotEmpty()
            } catch (e: Exception) {
                false
            }
            
            results.add(Triple(mirroredRecord.key(), mirroredRecord.value(), isValidMessage))
            assertEquals(expectedKey, mirroredRecord.key())
        }

        // Verify we processed all messages
        assertEquals(4, results.size)
        
        // Verify correct handling of valid vs invalid messages
        val validMessages = results.filter { it.third }
        val invalidMessages = results.filter { !it.third }
        
        assertEquals(2, validMessages.size, "Should have 2 valid messages")
        assertEquals(2, invalidMessages.size, "Should have 2 invalid messages")
        
        // Valid messages should have generated partition keys
        assertTrue(validMessages.any { it.first == "pokerstars_com-poker_pro_123" })
        assertTrue(validMessages.any { it.first == "bwin_com-casual_gamer_456" })
        
        // Invalid messages should have fallback keys
        assertTrue(invalidMessages.any { it.first == "msg-2" })
        assertTrue(invalidMessages.any { it.first == "msg-4" })

        println("✅ Mixed message workflow test completed successfully!")
        println("📊 Valid messages: ${validMessages.size}, Invalid messages: ${invalidMessages.size}")
    }
}