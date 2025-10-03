package io.symplify.sqs;

import java.util.Optional;
import io.symplify.kafka.PlayerKafka;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.symplify.streams.Configuration.Mapping.Selector;

public class PlayerBlockedSqs {
  public String mappingSelector;
  public String type;
  public String originalId;
  public String blocked = "true";
  public java.util.Map<String, String> properties = java.util.Map.of("locked", "true");

}
