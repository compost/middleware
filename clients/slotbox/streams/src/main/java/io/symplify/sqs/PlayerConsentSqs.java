package io.symplify.sqs;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class PlayerConsentSqs {

  public String type;
  public String contactId;
  public String mappingSelector;

  public Optional<String> player_id = Optional.empty();
  public Optional<String> brand_id = Optional.empty();
  public Optional<String> consented = Optional.empty();
  public Optional<String> channel = Optional.empty();
  public Map<String, String> properties = new HashMap<String, String>();

  public static Optional<PlayerConsentSqs> transform(io.symplify.kafka.PlayerConsentKafka input, String type,
      String mappingSelector) {
    if (input.channel.isEmpty()) {
      return Optional.empty();
    }
    PlayerConsentSqs sqs = new PlayerConsentSqs();
    sqs.player_id = input.player_id;
    sqs.contactId = input.player_id.orElse("");
    sqs.type = type;
    sqs.mappingSelector = mappingSelector;
    sqs.brand_id = input.brand_id;
    sqs.consented = input.consented;
    sqs.channel = input.channel;

    sqs.properties.put("consent_" + input.channel.get(), input.consented.orElse("undefined"));
    return Optional.of(sqs);
  }
}
