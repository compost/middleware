package io.symplify

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.smallrye.reactive.messaging.kafka.Record
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.mockito.kotlin.any

class KafkaMirrorErrorHandlingTest {

    private lateinit var kafkaMirrorService: KafkaMirrorService
    private lateinit var mockObjectMapper: ObjectMapper

    @BeforeEach
    fun setup() {
        kafkaMirrorService = KafkaMirrorService()
        mockObjectMapper = mock<ObjectMapper>()
        kafkaMirrorService.objectMapper = mockObjectMapper
    }

    @Test
    fun testMirrorMessageWhenObjectMapperThrowsException() {
        val json = """{"brand_id": "brand123", "player_id": "player456"}"""
        val inputRecord = Record.of("original-key", json) as Record<String?, String>
        
        whenever(mockObjectMapper.readValue(any<String>(), any<Class<MessageRecord>>()))
            .thenThrow(RuntimeException("JSON parsing failed"))

        val result = kafkaMirrorService.mirrorMessage(inputRecord)

        assertEquals("original-key", result.key())
        assertEquals(json, result.value())
    }

    @Test
    fun testMirrorMessageWhenNullKeyFallback() {
        val json = """{"brand_id": "brand123", "player_id": "player456"}"""
        val inputRecord = Record.of(null as String?, json)
        
        whenever(mockObjectMapper.readValue(any<String>(), any<Class<MessageRecord>>()))
            .thenThrow(RuntimeException("JSON parsing failed"))

        val result = kafkaMirrorService.mirrorMessage(inputRecord)

        assertEquals("unknown", result.key())
        assertEquals(json, result.value())
    }

    @Test
    fun testMessageRecordWithEmptyIds() {
        val realObjectMapper = ObjectMapper().registerKotlinModule()
        kafkaMirrorService.objectMapper = realObjectMapper
        
        val json = """{
            "brand_id": "",
            "player_id": "",
            "test": "empty_ids_scenario",
            "timestamp": "2025-09-21T20:30:00Z"
        }"""
        val inputRecord = Record.of("original-key", json) as Record<String?, String>

        val result = kafkaMirrorService.mirrorMessage(inputRecord)

        assertEquals("-", result.key())
        assertEquals(json.replace("\\s".toRegex(), ""), result.value().replace("\\s".toRegex(), ""))
    }

    @Test
    fun testMessageRecordWithNullValues() {
        val realObjectMapper = ObjectMapper().registerKotlinModule()
        kafkaMirrorService.objectMapper = realObjectMapper
        
        val jsonWithNulls = """{"brand_id": null, "player_id": null}"""
        val inputRecord = Record.of("fallback-key", jsonWithNulls) as Record<String?, String>

        val result = kafkaMirrorService.mirrorMessage(inputRecord)

        assertEquals("fallback-key", result.key())
        assertEquals(jsonWithNulls, result.value())
    }
}