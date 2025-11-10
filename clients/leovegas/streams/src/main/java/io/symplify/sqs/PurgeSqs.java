package io.symplify.sqs;

import java.util.Optional;

public class PurgeSqs {
  public Optional<String> player_id = Optional.empty();
  public Optional<String> brand_id = Optional.empty();

  public static PurgeSqs transform(io.symplify.kafka.PlayerKafka input) {
    PurgeSqs sqs = new PurgeSqs();
    sqs.player_id = input.player_id;
    sqs.brand_id = input.brand_id;
    return sqs;
  }
}
