package io.symplify.sqs;

import java.util.Optional;

public class FirstDepositDatetimeSqs {
  public Optional<String> first_dep_datetime = Optional.empty();

  public static Optional<FirstDepositDatetimeSqs> transform(io.symplify.store.PlayerStore store) {
    return store.player.map(p -> {
      FirstDepositDatetimeSqs sqs = new FirstDepositDatetimeSqs();
      sqs.first_dep_datetime = p.first_dep_datetime.map(Transformer::truncateOrKeep);
      return sqs;
    });
  }
}
