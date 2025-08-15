package io.symplify.kafka;

import java.util.Optional;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class PlayerConsentKafka {
  public Optional<String> player_id = Optional.empty();
  public Optional<String> brand_id = Optional.empty();
  public Optional<String> channel = Optional.empty();
  public Optional<String> consented = Optional.empty();
}
