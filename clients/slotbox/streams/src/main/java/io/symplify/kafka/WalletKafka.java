package io.symplify.kafka;

import java.util.Optional;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class WalletKafka {
  public Optional<String> player_id = Optional.empty();
  public Optional<String> brand_id = Optional.empty();
  public Optional<String> transaction_datetime = Optional.empty();
  public Optional<String> transaction_type_id = Optional.empty();
  public Optional<String> transaction_status_id = Optional.empty();
  public Optional<String> amount = Optional.empty();
  public Optional<String> currency_description = Optional.empty();
}
