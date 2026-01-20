package io.symplify.flink;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.symplify.sqs.Body;
import io.symplify.sqs.Transformer;

public class Sender implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(Sender.class);
    private static final ObjectMapper mapper = Transformer.getObjectMapper();

    private final Map<String, String> brandQueue;
    private final Set<String> mappingSelectorDevPrefix;
    private final String messageGroupId;

    public Sender(Map<String, String> brandQueue, Set<String> mappingSelectorDevPrefix, String messageGroupId) {
        this.brandQueue = brandQueue;
        this.mappingSelectorDevPrefix = mappingSelectorDevPrefix != null ? mappingSelectorDevPrefix : Collections.emptySet();
        this.messageGroupId = messageGroupId;
    }

    private String withPrefix(String brandId, String mappingSelector) {
        if (mappingSelectorDevPrefix.contains(brandId)) {
            return "DEV_" + mappingSelector;
        }
        return mappingSelector;
    }

    public <T> Optional<SqsRequest> createRequest(String brandId, String playerId, String type, String mappingSelector, T data) {
        Body<T> body = new Body<T>();
        body.contactId = playerId;
        body.type = type;
        body.mappingSelector = withPrefix(brandId, mappingSelector);
        body.properties = data;
        return createRequestFull(brandId, playerId, type, mappingSelector, body);
    }

    public <T> Optional<SqsRequest> createRequestFull(String brandId, String playerId, String type, String mappingSelector, T data) {
        try {
            var body = mapper.writeValueAsString(data);
            return createRequest(brandId, playerId, type, mappingSelector, body);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<SqsRequest> createRequest(String brandId, String playerId, String type, String mappingSelector, String body) {
        var messageId = UUID.randomUUID().toString();
        var queue = brandQueue.get(brandId);
        
        if (queue == null) {
            logger.warn("missing queue for brandId: {}, playerId: {}", brandId, playerId);
            return Optional.empty();
        } else {
            return Optional.of(SqsRequest.builder()
                    .queueUrl(queue)
                    .body(body)
                    .deduplicationId(messageId)
                    .messageGroupId(messageGroupId)
                    .build());
        }
    }
}
