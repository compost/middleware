package io.symplify.sqs;

import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.symplify.kafka.WalletKafka;
import io.symplify.streams.Configuration.Mapping.Selector;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class WalletSqs {

  private static final String PREFIX_DEPOSIT = "deposit";
  private static final String PREFIX_WITHDRAW = "withdraw";
  private static final Set<String> DEPOSIT_SUCCESS = Set.of("completed", "approved");
  private static final Set<String> WITHDRAW_SUCCESS = Set.of("approved");

  @JsonIgnore
  public String mappingSelector;
  public Optional<String> player_id = Optional.empty();
  public Optional<String> currency = Optional.empty();
  public Optional<String> Transaction_Type = Optional.empty();
  public Optional<String> Transaction_Amount = Optional.empty();

  public void with(WalletKafka kafka) {
    this.player_id = kafka.player_id;
    this.Transaction_Amount = kafka.amount;
    this.Transaction_Type = kafka.transaction_type;
    this.currency = kafka.currency_description;
  }

  public static Optional<WalletSqs> transform(WalletKafka kafka) {
    var type = kafka.transaction_type.orElse("").toLowerCase();

    if (type.startsWith(PREFIX_DEPOSIT)) {
      return transformDeposit(kafka);
    }

    if (type.startsWith(PREFIX_WITHDRAW)) {
      return transformWithdraw(kafka);
    }

    return Optional.empty();

  }

  private static Optional<WalletSqs> transformWithdraw(WalletKafka kafka) {
    var status = kafka.transaction_status.orElse("").toLowerCase();
    if (WITHDRAW_SUCCESS.contains(status)) {
      var sqs = new WalletSqs.SuccessWithdrawal();
      sqs.with(kafka);
      return Optional.of(sqs);
    }
    return Optional.empty();
  }

  private static Optional<WalletSqs> transformDeposit(WalletKafka kafka) {
    var status = kafka.transaction_status.orElse("").toLowerCase();
    if (DEPOSIT_SUCCESS.contains(status)) {
      var sqs = new WalletSqs.SuccessDeposit();
      sqs.with(kafka);
      return Optional.of(sqs);
    }
    return Optional.empty();
  }

  @RegisterForReflection
  public static class SuccessWithdrawal extends WalletSqs {
    public Optional<String> Amount = Optional.empty();
    public Optional<String> Date = Optional.empty();

    @Override
    public void with(WalletKafka kafka) {
      super.with(kafka);
      this.mappingSelector = Selector.WITHDRAWAL_APPROVED;
      this.Amount = kafka.amount;
      this.Date = kafka.transaction_datetime.map(Transformer::truncateOrKeep);
    }
  }

  @RegisterForReflection
  public static class SuccessDeposit extends WalletSqs {
    public Optional<String> Amount = Optional.empty();
    public Optional<String> Date = Optional.empty();

    @Override
    public void with(WalletKafka kafka) {
      super.with(kafka);
      this.mappingSelector = Selector.DEPOSIT_APPROVED;
      this.Amount = kafka.amount;
      this.Date = kafka.transaction_datetime.map(Transformer::truncateOrKeep);
    }
  }
}
