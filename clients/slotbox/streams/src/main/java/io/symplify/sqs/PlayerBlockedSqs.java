package io.symplify.sqs;

import java.util.Optional;

import io.symplify.kafka.PlayerStatusKafka;
import io.symplify.streams.Configuration.Mapping.Selector;
import io.symplify.streams.Configuration.Mapping.Type;

public class PlayerBlockedSqs {
  public String type;
  public String mappingSelector;
  public Optional<String> contactId;
  public String blockedUntil;

  public PlayerBlockedSqs with(PlayerStatusKafka kafka) {
    this.type = Type.USER_BLOCK;
    this.mappingSelector = Selector.PLAYER_BLOCKED;
    this.contactId = kafka.player_id;
    if (kafka.is_blocked.orElse("").toLowerCase().equals("true")) {
      blockedUntil = "2040-01-01";
    } else {
      blockedUntil = "2020-01-01";
    }
    return this;
  }

  public static Optional<PlayerBlockedSqs> transform(PlayerStatusKafka kafka) {
    if (kafka.is_blocked.filter(v -> !v.isBlank()).isPresent()) {
      return Optional.of(new PlayerBlockedSqs().with(kafka));
    }
    return Optional.empty();
  }

}
