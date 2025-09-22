package io.symplify

import com.fasterxml.jackson.databind.ObjectMapper
import io.smallrye.reactive.messaging.annotations.Blocking
import io.smallrye.reactive.messaging.kafka.Record
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.reactive.messaging.Incoming
import org.eclipse.microprofile.reactive.messaging.Outgoing
import org.jboss.logging.Logger

@ApplicationScoped
class KafkaMirrorService {
    
    @Inject
    lateinit var objectMapper: ObjectMapper
    
    companion object {
        private val logger = Logger.getLogger(KafkaMirrorService::class.java)
    }
    
    @Incoming("source")
    @Outgoing("target")
    @Blocking
    fun mirrorMessage(record: Record<String?, String>): Record<String, String> {
        return try {
            val messageRecord = objectMapper.readValue(record.value(), MessageRecord::class.java)
            val partitionKey = "${messageRecord.brandId}-${messageRecord.playerId}"
            
            logger.info("Mirroring message with partition key: $partitionKey")
            
            Record.of(partitionKey, record.value())
        } catch (e: Exception) {
            logger.error("Error processing message: ${e.message}", e)
            throw e
        }
    }
}
