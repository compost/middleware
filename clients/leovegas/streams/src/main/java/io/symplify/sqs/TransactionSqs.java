package io.symplify.sqs;

import java.util.Map;

import io.symplify.streams.Configuration;

public class TransactionSqs {
  public String type;
  public String player_id;
  public String originalId;
  public String contactId;
  public String brand_id;
  public String mappingSelector;
  public Map<String, Object> properties;

  public static TransactionSqs transform(io.symplify.kafka.TransactionKafka in) {
    var out = new TransactionSqs();
    out.type = Configuration.Mapping.Type.GENERIC_USER;
    out.mappingSelector = in.mappingSelector.orElse("unknown");
    out.contactId = in.player_id.orElse("unknown");
    out.player_id = in.player_id.orElse("unknown");
    out.brand_id = in.brand_id.orElse("unknown");
    out.originalId = in.player_id.orElse("unknown");
    out.properties = in.parameters;
    return out;

  }
}
