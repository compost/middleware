package io.symplify.sqs;

import java.util.Optional;

public class LoginSqs {
  public Optional<String> player_id = Optional.empty();
  public Optional<String> brand_id = Optional.empty();
  public Optional<String> brand = Optional.empty();
  public Optional<String> login_datetime = Optional.empty();
  public Optional<String> login_success = Optional.empty();

  public static LoginSqs transform(io.symplify.kafka.LoginKafka input) {
    LoginSqs sqs = new LoginSqs();
    sqs.player_id = input.player_id;
    sqs.brand_id = input.brand_id;
    sqs.brand = input.brand;
    sqs.login_datetime = input.login_datetime.map(Transformer::truncateOrKeep);
    sqs.login_success = input.login_success;
    return sqs;
  }
}
