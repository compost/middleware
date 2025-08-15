package io.symplify.sqs;

import java.util.Optional;
import io.symplify.kafka.PlayerStatusKafka;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.symplify.streams.Configuration.Mapping.Selector;

public class PlayerBlockedSqs {
  @JsonIgnore
  public String mappingSelector;
  public Optional<String> player_id;
  public Optional<String> blocked_reason;

  public PlayerBlockedSqs with(PlayerStatusKafka kafka) {
    this.mappingSelector = Selector.PLAYER_BLOCKED;
    this.player_id = kafka.player_id;
    this.blocked_reason = kafka.player_blocked_reason;
    return this;
  }

  public static Optional<PlayerBlockedSqs> transform(PlayerStatusKafka kafka) {
    if (kafka.blocked_end_date.filter(v -> !v.isBlank()).isPresent()) {
      return Optional.of(new SelfExclusionSqs().with(kafka));
    }
    return Optional.of(new PlayerBlockedSqs().with(kafka));
  }

  public static class SelfExclusionSqs extends PlayerBlockedSqs {
    public Optional<String> blockedUntil;

    @Override
    public SelfExclusionSqs with(PlayerStatusKafka kafka) {
      super.with(kafka);
      this.mappingSelector = Selector.SELF_EXCLUSION;
      this.blockedUntil = kafka.blocked_end_date.map(Transformer::truncateOrKeep);
      return this;
    }
  }

}
