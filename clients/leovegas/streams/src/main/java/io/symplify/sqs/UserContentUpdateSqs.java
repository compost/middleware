package io.symplify.sqs;

import java.util.Optional;

import io.symplify.kafka.UserContentUpdateKafka;
import io.symplify.streams.Configuration.Mapping.Selector;
import io.symplify.streams.Configuration.Mapping.Type;

public class UserContentUpdateSqs {
  public Optional<String> player_id = Optional.empty();
  public Optional<String> contactId = Optional.empty();
  public Optional<String> brand_id = Optional.empty();
  public Optional<String> consented = Optional.empty();
  public Optional<String> channel = Optional.empty();
  public String type;
  public String mappingSelector;

  public static UserContentUpdateSqs transform(UserContentUpdateKafka in) {
    var out = new UserContentUpdateSqs();
    out.type = Type.USER_CONSENT_UPDATE;
    out.mappingSelector = Selector.CONSENT_CHANGE;
    out.player_id = in.player_id;
    out.contactId = in.player_id;
    out.brand_id = in.brand_id;
    out.channel = in.channel;
    out.consented = in.consented.flatMap(s -> {
      if ("1".equals(s))
        return Optional.of("true");
      if ("0".equals(s))
        return Optional.of("false");
      return Optional.empty();
    });
    return out;
  }
}
