package io.symplify.kafka;

import java.util.Optional;

public class PlayerStatusKafka {
  public Optional<String> player_id = Optional.empty();
  public Optional<String> brand_id = Optional.empty();
  public Optional<String> player_blocked_reason = Optional.empty();
  public Optional<String> blocked_end_date = Optional.empty();
}
