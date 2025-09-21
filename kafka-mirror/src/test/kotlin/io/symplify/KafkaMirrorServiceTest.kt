package io.symplify

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.smallrye.reactive.messaging.kafka.Record
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`

class KafkaMirrorServiceTest {

    private lateinit var kafkaMirrorService: KafkaMirrorService
    private lateinit var objectMapper: ObjectMapper

    @BeforeEach
    fun setup() {
        kafkaMirrorService = KafkaMirrorService()
        objectMapper = ObjectMapper().registerKotlinModule()
        kafkaMirrorService.objectMapper = objectMapper
    }

    @Test
    fun testMirrorMessageWithValidJson() {
        val json = """{
            "brand_id": "brand123",
            "player_id": "player456",
            "amount": 100,
            "currency": "EUR",
            "transaction_id": "txn_987654321"
        }"""
        val inputRecord = Record.of("original-key", json) as Record<String?, String>

        val result = kafkaMirrorService.mirrorMessage(inputRecord)

        assertEquals("brand123-player456", result.key())
        assertEquals(json.replace("\\s".toRegex(), ""), result.value().replace("\\s".toRegex(), ""))
    }

    @Test
    fun testMirrorMessageWithInvalidJson() {
        val invalidJson = """{"invalid": "json", "missing": "required fields"}"""
        val inputRecord = Record.of("original-key", invalidJson)

        val result = kafkaMirrorService.mirrorMessage(inputRecord)

        assertEquals("original-key", result.key())
        assertEquals(invalidJson, result.value())
    }

    @Test
    fun testMirrorMessageWithNullKey() {
        val json = """{
            "brand_id": "brand789",
            "player_id": "player101",
            "event": "login",
            "timestamp": "2025-09-21T20:00:00Z"
        }"""
        val inputRecord = Record.of(null as String?, json)

        val result = kafkaMirrorService.mirrorMessage(inputRecord)

        assertEquals("brand789-player101", result.key())
        assertEquals(json.replace("\\s".toRegex(), ""), result.value().replace("\\s".toRegex(), ""))
    }

    @Test
    fun testMirrorMessageWithMalformedJson() {
        val malformedJson = """{"brand_id": "brand123", "player_id":"""
        val inputRecord = Record.of("fallback-key", malformedJson) as Record<String?, String>

        val result = kafkaMirrorService.mirrorMessage(inputRecord)

        assertEquals("fallback-key", result.key())
        assertEquals(malformedJson, result.value())
    }

    @Test
    fun testMirrorMessageWithSpecialCharactersInIds() {
        val json = """{
            "brand_id": "brand-with-dashes_123",
            "player_id": "player.with.dots@456",
            "special_chars_test": true,
            "symbols": ["@", "#", "$", "%"],
            "unicode": "café_résumé_naïve"
        }"""
        val inputRecord = Record.of("original-key", json) as Record<String?, String>

        val result = kafkaMirrorService.mirrorMessage(inputRecord)

        assertEquals("brand-with-dashes_123-player.with.dots@456", result.key())
        assertEquals(json.replace("\\s".toRegex(), ""), result.value().replace("\\s".toRegex(), ""))
    }
}
