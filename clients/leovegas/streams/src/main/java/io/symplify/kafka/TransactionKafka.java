package io.symplify.kafka;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class TransactionKafka {
  public Optional<String> player_id = Optional.empty();
  public Optional<String> brand_id = Optional.empty();
  public Optional<String> mappingSelector = Optional.empty();
  public Map<String, Object> parameters = Collections.emptyMap();
}
