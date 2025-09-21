package io.symplify

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals

class KafkaMirrorIntegrationTest {

    @Test
    fun testMessageRecordSerializationWithNestedData() {
        val objectMapper = ObjectMapper()
        objectMapper.findAndRegisterModules()
        
        // Real JSON input as it would come from Kafka
        val realJson = """{
            "brand_id": "leovegas_fr",
            "player_id": "player_789012",
            "transaction": {
                "id": "txn_123456789",
                "amount": 2500,
                "currency": "EUR",
                "timestamp": "2025-09-21T21:30:00Z",
                "metadata": {
                    "source": "mobile_app",
                    "version": "3.2.1",
                    "location": {
                        "country": "FR",
                        "timezone": "Europe/Paris"
                    }
                }
            },
            "player": {
                "level": 42,
                "vip_status": "platinum",
                "preferences": {
                    "language": "fr",
                    "notifications": true,
                    "themes": ["dark", "casino"]
                }
            },
            "session": {
                "id": "sess_abc123def456",
                "start_time": "2025-09-21T20:15:30Z",
                "device": {
                    "type": "mobile",
                    "os": "iOS",
                    "version": "17.1"
                }
            }
        }"""
        
        val deserializedRecord = objectMapper.readValue(realJson, MessageRecord::class.java)
        
        assertEquals("leovegas_fr", deserializedRecord.brandId)
        assertEquals("player_789012", deserializedRecord.playerId)
        
        // Verify nested transaction data
        val transaction = deserializedRecord.getAdditionalProperties()["transaction"] as Map<*, *>
        assertEquals("txn_123456789", transaction["id"])
        assertEquals(2500, transaction["amount"])
        assertEquals("EUR", transaction["currency"])
        
        // Verify deeply nested metadata
        val metadata = transaction["metadata"] as Map<*, *>
        assertEquals("mobile_app", metadata["source"])
        val location = metadata["location"] as Map<*, *>
        assertEquals("FR", location["country"])
        
        // Verify player data
        val player = deserializedRecord.getAdditionalProperties()["player"] as Map<*, *>
        assertEquals(42, player["level"])
        assertEquals("platinum", player["vip_status"])
        
        // Verify preferences with list
        val preferences = player["preferences"] as Map<*, *>
        assertEquals("fr", preferences["language"])
        assertEquals(true, preferences["notifications"])
        val themes = preferences["themes"] as List<*>
        assertEquals(listOf("dark", "casino"), themes)
        
        // Verify session data
        val session = deserializedRecord.getAdditionalProperties()["session"] as Map<*, *>
        assertEquals("sess_abc123def456", session["id"])
        val device = session["device"] as Map<*, *>
        assertEquals("mobile", device["type"])
        assertEquals("iOS", device["os"])
        
        // Test that we can serialize it back to JSON
        val serializedJson = objectMapper.writeValueAsString(deserializedRecord)
        val reDeserializedRecord = objectMapper.readValue(serializedJson, MessageRecord::class.java)
        assertEquals("leovegas_fr", reDeserializedRecord.brandId)
        assertEquals("player_789012", reDeserializedRecord.playerId)
    }
}
