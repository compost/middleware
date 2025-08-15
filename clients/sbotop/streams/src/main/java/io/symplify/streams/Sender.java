package io.symplify.streams;

import java.util.Collections;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.symplify.sqs.Body;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;
import static io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.*;

@ApplicationScoped
public class Sender {

  private final Logger logger = Logger.getLogger(this.getClass());

  private final ObjectMapper mapper = SqsProcessor.getObjectMapper();

  @Inject
  SqsClient sqs;

  @ConfigProperty(name = "brand-queue")
  java.util.Map<String, String> brandQueue;

  @ConfigProperty(name = "mapping-selector-dev-prefix")
  java.util.Set<String> mappingSelectorDevPrefix = Collections.emptySet();

  @ConfigProperty(name = "message-group-id")
  String messageGroupId;

  @ConfigProperty(name = "dry-run")
  Boolean dryRun;

  private String withPrefix(String brandId, String mappingSelector) {
    if (mappingSelectorDevPrefix.contains(brandId)) {
      return "DEV_" + mappingSelector;
    }
    return mappingSelector;
  }

  public <T> void send(
      String brandId,
      String playerId,
      String type,
      String mappingSelector,
      T data) {
    Body<T> body = new Body<T>();
    body.contactId = playerId;
    body.type = type;
    body.mappingSelector = withPrefix(brandId, mappingSelector);
    body.properties = data;
    sendFull(brandId, playerId, type, mappingSelector, body);
  }

  public <T> void sendFull(
      String brandId,
      String playerId,
      String type,
      String mappingSelector,
      T data) {
    try {
      var body = mapper.writeValueAsString(data);
      send(brandId, playerId, type, mappingSelector, body);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void send(
      String brandId,
      String playerId,
      String type,
      String mappingSelector,
      String body) {
    var messageId = UUID.randomUUID().toString();
    var queue = brandQueue.get(brandId);
    if (queue == null) {
      logger.warnv(
          "missing queue",
          kv("playerId", playerId),
          kv("brandId", brandId),
          kv("messageId", messageId),
          kv("type", type),
          kv("body", body),
          kv("mappingSelector", mappingSelector));
    } else {
      try {
        logger.debugv(
            "sending message",
            kv("playerId", playerId),
            kv("brandId", brandId),
            kv("url", queue),
            kv("messageId", messageId),
            kv("type", type),
            kv("mappingSelector", mappingSelector),
            kv("body", body));
        if (!dryRun) {
          sqs.sendMessage(m -> m.queueUrl(queue)
              .messageBody(body)
              .messageDeduplicationId(messageId)
              .messageGroupId(messageGroupId));
        }
        logger.infov(
            "sent message",
            kv("playerId", playerId),
            kv("brandId", brandId),
            kv("url", queue),
            kv("messageId", messageId),
            kv("type", type),
            kv("mappingSelector", mappingSelector),
            kv("body", body));
      } catch (Exception e) {
        logger.errorv(
            "unable to send message",
            kv("playerId", playerId),
            kv("brandId", brandId),
            kv("url", queue),
            kv("messageId", messageId),
            kv("type", type),
            kv("mappingSelector", mappingSelector),
            kv("body", body),
            e);
        throw e;

      }
    }
  }

}
