package io.symplify.sqs;

import java.util.Optional;

import io.symplify.streams.Configuration.Mapping.Selector;
import io.symplify.streams.Configuration.Mapping.Type;

public class PlayerBlockedSqs {
  public String mappingSelector;
  public String type;
  public String contactId;
  public String blocked;
  public java.util.Map<String, String> properties;

  public static PlayerBlockedSqs transform(String originalId, String locked, Optional<String> brand) {
    PlayerBlockedSqs sqs = new PlayerBlockedSqs();
    sqs.contactId = originalId;
    sqs.mappingSelector = Selector.PLAYER_BLOCKED;
    sqs.type = Type.USER_BLOCKED_TOGGLE;

    sqs.blocked = locked;
    sqs.properties = java.util.Map.of("locked", locked, "brand", brand.orElse("unknown"));
    return sqs;

  }

}
