package io.symplify.kafka;

import java.util.Optional;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class PlayerKafka {
  public Optional<String> player_id = Optional.empty();
  public Optional<String> reg_datetime = Optional.empty();
  public Optional<String> first_name = Optional.empty();
  public Optional<String> last_name = Optional.empty();
  public Optional<String> email = Optional.empty();
  public Optional<String> phone_number = Optional.empty();
  public Optional<String> language = Optional.empty();
  public Optional<String> affiliate_id = Optional.empty();
  public Optional<String> is_self_excluded = Optional.empty();
  public Optional<String> first_dep_datetime = Optional.empty();
  public Optional<String> dob = Optional.empty();
  public Optional<String> country_id = Optional.empty();
  public Optional<String> VIP = Optional.empty();
  public Optional<String> test_user = Optional.empty();
  public Optional<String> currency_id = Optional.empty();
  public Optional<String> brand_id = Optional.empty();
}
