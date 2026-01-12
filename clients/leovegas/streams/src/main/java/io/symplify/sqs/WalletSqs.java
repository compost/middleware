package io.symplify.sqs;

import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.symplify.kafka.WalletKafka;
import io.symplify.streams.Configuration.Mapping.Selector;

public class WalletSqs {

  private static final String PREFIX_DEPOSIT = "deposit_";
  private static final Set<String> DEPOSIT_FAILED = Set.of("failed");
  private static final String PREFIX_WITHDRAW = "withdraw_";
  private static final Set<String> DEPOSIT_SUCCESS = Set.of("success");
  private static final Set<String> WITHDRAW_FAILED = Set.of("failed");
  private static final Set<String> WITHDRAW_SUCCESS = Set.of("success");

  @JsonIgnore
  public String mappingSelector;
  public Optional<String> player_id = Optional.empty();
  public Optional<String> brand = Optional.empty();
  public Optional<String> currency = Optional.empty();
  public Optional<String> Transaction_Type = Optional.empty();
  public Optional<String> Transaction_Amount = Optional.empty();

  public void with(WalletKafka kafka) {
    this.player_id = kafka.player_id;
    this.brand = kafka.brand;
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
    if (WITHDRAW_FAILED.contains(status)) {
      var sqs = new WalletSqs.FailedWithdrawal();
      sqs.with(kafka);
      return Optional.of(sqs);
    }

    var disableSuccess = false; // DISABLE Not in the mapping sheet
    if (disableSuccess && WITHDRAW_SUCCESS.contains(status)) {
      var sqs = new WalletSqs.SuccessWithdrawal();
      sqs.with(kafka);
      return Optional.of(sqs);
    }
    return Optional.empty();
  }

  private static Optional<WalletSqs> transformDeposit(WalletKafka kafka) {
    var status = kafka.transaction_status.orElse("").toLowerCase();
    if (DEPOSIT_FAILED.contains(status)) {
      var sqs = new WalletSqs.FailedDeposit();
      sqs.with(kafka);
      return Optional.of(sqs);
    }
    var disableSuccess = false; // DISABLE Not in the mapping sheet
    if (disableSuccess && DEPOSIT_SUCCESS.contains(status)) {
      var sqs = new WalletSqs.SuccessDeposit();
      sqs.with(kafka);
      return Optional.of(sqs);
    }
    return Optional.empty();
  }

  public static class FailedWithdrawal extends WalletSqs {
    public Optional<String> failed_withdrawal_date = Optional.empty();

    @Override
    public void with(WalletKafka kafka) {
      super.with(kafka);
      this.mappingSelector = Selector.FAILED_WITHDRAWAL;
      this.failed_withdrawal_date = kafka.transaction_datetime.map(Transformer::truncateOrKeep);
    }

  }

  public static class SuccessWithdrawal extends WalletSqs {
    public Optional<String> success_withdrawal_date = Optional.empty();

    @Override
    public void with(WalletKafka kafka) {
      super.with(kafka);
      this.mappingSelector = Selector.SUCCESS_WITHDRAWAL;
      this.success_withdrawal_date = kafka.transaction_datetime.map(Transformer::truncateOrKeep);
    }
  }

  public static class FailedDeposit extends WalletSqs {
    public Optional<String> failed_deposit_date = Optional.empty();

    @Override
    public void with(WalletKafka kafka) {
      super.with(kafka);
      this.mappingSelector = Selector.FAILED_DEPOSIT;
      this.failed_deposit_date = kafka.transaction_datetime.map(Transformer::truncateOrKeep);
    }
  }

  public static class SuccessDeposit extends WalletSqs {
    public Optional<String> success_deposit_date = Optional.empty();

    @Override
    public void with(WalletKafka kafka) {
      super.with(kafka);
      this.mappingSelector = Selector.SUCCESS_DEPOSIT;
      this.success_deposit_date = kafka.transaction_datetime.map(Transformer::truncateOrKeep);
    }
  }
}
