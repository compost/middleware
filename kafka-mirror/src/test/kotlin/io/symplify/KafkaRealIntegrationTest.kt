package io.symplify

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.smallrye.reactive.messaging.kafka.Record
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals

class KafkaRealIntegrationTest {

    @Test
    fun testKafkaMirrorServiceLogicWithComplexRealWorldData() {
        val objectMapper = ObjectMapper().registerKotlinModule()
        val kafkaMirrorService = KafkaMirrorService()
        kafkaMirrorService.objectMapper = objectMapper


        // Real JSON message as it would come from Kafka
        val messageJson = """{
            "brand_id": "betsson_se",
            "player_id": "player_premium_456789",
            "event_type": "bet_placed",
            "game": {
                "id": "slot_mega_fortune",
                "provider": "netent",
                "category": "slots",
                "rtp": 96.6
            },
            "bet": {
                "amount": 5.0,
                "currency": "EUR",
                "lines": 25,
                "bet_per_line": 0.2,
                "auto_spin": false
            },
            "result": {
                "win_amount": 12.5,
                "multiplier": 2.5,
                "symbols": [
                    ["cherry", "lemon", "bell"],
                    ["seven", "seven", "seven"],
                    ["bar", "cherry", "lemon"]
                ],
                "winning_lines": [2, 7, 15]
            },
            "player_stats": {
                "total_bets": 1247,
                "total_wins": 892,
                "session_duration_minutes": 35,
                "balance_before": 127.80,
                "balance_after": 135.30
            },
            "context": {
                "timestamp": "2025-09-21T21:45:12.123Z",
                "session_id": "sess_98765",
                "ip_address": "192.168.1.100",
                "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1)",
                "promo": {
                    "code": "WELCOME100",
                    "bonus_percentage": 100,
                    "max_bonus": 200.0
                }
            }
        }"""


        // Test the mirroring logic
        val inputRecord = Record.of("original-key-123", messageJson) as Record<String?, String>
        val mirroredRecord = kafkaMirrorService.mirrorMessage(inputRecord)

        // Verify partition key generation
        val expectedPartitionKey = "betsson_se-player_premium_456789"
        assertEquals(expectedPartitionKey, mirroredRecord.key())
        assertEquals(messageJson, mirroredRecord.value())

        // Verify the JSON can be deserialized back correctly
        val deserializedMessage = objectMapper.readValue(mirroredRecord.value(), MessageRecord::class.java)
        assertEquals("betsson_se", deserializedMessage.brandId)
        assertEquals("player_premium_456789", deserializedMessage.playerId)

        // Verify complex nested data integrity
        val gameData = deserializedMessage.getAdditionalProperties()["game"] as Map<*, *>
        assertEquals("slot_mega_fortune", gameData["id"])
        assertEquals("netent", gameData["provider"])
        assertEquals(96.6, gameData["rtp"])

        val betData = deserializedMessage.getAdditionalProperties()["bet"] as Map<*, *>
        assertEquals(5.0, betData["amount"])
        assertEquals(25, betData["lines"])
        assertEquals(false, betData["auto_spin"])

        val resultData = deserializedMessage.getAdditionalProperties()["result"] as Map<*, *>
        assertEquals(12.5, resultData["win_amount"])
        assertEquals(2.5, resultData["multiplier"])
        val symbols = resultData["symbols"] as List<*>
        assertEquals(3, symbols.size)
        val firstRow = symbols[0] as List<*>
        assertEquals(listOf("cherry", "lemon", "bell"), firstRow)

        val winningLines = resultData["winning_lines"] as List<*>
        assertEquals(listOf(2, 7, 15), winningLines)

        val playerStats = deserializedMessage.getAdditionalProperties()["player_stats"] as Map<*, *>
        assertEquals(1247, playerStats["total_bets"])
        assertEquals(127.80, playerStats["balance_before"])

        val context = deserializedMessage.getAdditionalProperties()["context"] as Map<*, *>
        assertEquals("sess_98765", context["session_id"])
        val promo = context["promo"] as Map<*, *>
        assertEquals("WELCOME100", promo["code"])
        assertEquals(200.0, promo["max_bonus"])

        // Test partition key generation with complex real-world scenario
        println("Generated partition key: ${mirroredRecord.key()}")
        println("Message size: ${messageJson.length} characters")
        
        // Test multiple scenarios with different brand/player combinations using real JSON
        val testScenarios = listOf(
            """{
                "brand_id": "casino_royal",
                "player_id": "vip_player_001",
                "event_type": "jackpot_win",
                "amount": 50000.0,
                "currency": "USD",
                "game": "progressive_slots",
                "timestamp": "2025-09-21T22:00:00Z"
            }""" to "casino_royal-vip_player_001",
            
            """{
                "brand_id": "betsson_com",
                "player_id": "regular_user_12345",
                "event_type": "sports_bet",
                "match": "Barcelona vs Real Madrid",
                "bet_amount": 25.0,
                "currency": "EUR",
                "odds": 2.5
            }""" to "betsson_com-regular_user_12345",
            
            """{
                "brand_id": "leovegas_de",
                "player_id": "premium_customer_999",
                "event_type": "live_casino",
                "game": "blackjack",
                "dealer": "Emma_Berlin",
                "bet": 100.0,
                "result": "player_wins"
            }""" to "leovegas_de-premium_customer_999",
            
            """{
                "brand_id": "unibet_uk",
                "player_id": "new_player_abc123",
                "event_type": "first_deposit",
                "amount": 50.0,
                "currency": "GBP",
                "bonus_applied": true,
                "bonus_amount": 50.0
            }""" to "unibet_uk-new_player_abc123"
        )
        
        testScenarios.forEach { (testJson, expectedKey) ->
            val testRecord = Record.of("test-key", testJson) as Record<String?, String>
            val testResult = kafkaMirrorService.mirrorMessage(testRecord)
            
            assertEquals(expectedKey, testResult.key())
            assertEquals(testJson.replace("\\s".toRegex(), ""), testResult.value().replace("\\s".toRegex(), ""))
            
            // Verify JSON round-trip
            val deserializedTest = objectMapper.readValue(testResult.value(), MessageRecord::class.java)
            val expectedBrand = expectedKey.split("-")[0]
            val expectedPlayer = expectedKey.split("-").drop(1).joinToString("-")
            assertEquals(expectedBrand, deserializedTest.brandId)
            assertEquals(expectedPlayer, deserializedTest.playerId)
        }
    }
}