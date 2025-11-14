package io.symplify.sqs;

import java.util.Optional;

import io.symplify.kafka.UserContentUpdateKafka;

public class UserContentUpdateSqs {
  public Optional<String> player_id = Optional.empty();
  public Optional<String> brand_id = Optional.empty();
  public Optional<String> consented = Optional.empty();
  public Optional<String> channel = Optional.empty();

  public static UserContentUpdateSqs transform(UserContentUpdateKafka in) {
    var out = new UserContentUpdateSqs();
    out.player_id = in.player_id;
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
