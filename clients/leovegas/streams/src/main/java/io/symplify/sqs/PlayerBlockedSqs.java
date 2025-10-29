package io.symplify.sqs;

import io.symplify.streams.Configuration.Mapping.Selector;
import io.symplify.streams.Configuration.Mapping.Type;

public class PlayerBlockedSqs {
  public String mappingSelector;
  public String type;
  public String originalId;
  public String blocked;
  public java.util.Map<String, String> properties;

  public static PlayerBlockedSqs transform(String originalId, String locked) {
    PlayerBlockedSqs sqs = new PlayerBlockedSqs();
    sqs.originalId = originalId;
    sqs.mappingSelector = Selector.PLAYER_BLOCKED;
    sqs.type = Type.USER_BLOCK;

    sqs.blocked = locked;
    sqs.properties = java.util.Map.of("locked", locked);
    return sqs;

  }

}
