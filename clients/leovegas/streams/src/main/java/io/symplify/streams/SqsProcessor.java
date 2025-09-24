package io.symplify.streams;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import org.jboss.logging.Logger;
import io.symplify.kafka.*;
import io.symplify.sqs.*;
import io.symplify.store.PlayerStore;
import io.symplify.streams.Configuration.Mapping.Selector;
import io.symplify.streams.Configuration.Mapping.Type;

public class SqsProcessor implements Processor<String, byte[], Void, Void> {

  private final Logger logger = Logger.getLogger(this.getClass());
  public static ObjectMapper getObjectMapper() {
    var objMp = new ObjectMapper();
    objMp.registerModules(new Jdk8Module());
    objMp.setSerializationInclusion(Include.NON_ABSENT);
    objMp.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return objMp;
  }

  private KeyValueStore<String, io.symplify.store.PlayerStore> store;
  private ProcessorContext<Void, Void> context;
  final private Sender sender;
  final private ObjectMapper mapper = getObjectMapper();

  public SqsProcessor(final Sender sender) {
    this.sender = sender;
  }

  @Override
  public void init(final ProcessorContext<Void, Void> context) {
    this.store = context.getStateStore(Configuration.Store.PLAYERS);
    this.context = context;
  }

  @Override
  public void process(Record<String, byte[]> record) {
    final Optional<PlayerStore> newPlayer = this.context.recordMetadata().flatMap(
        (RecordMetadata metadata) -> processMesssage(metadata, record));
    newPlayer.ifPresent(p -> store.put(record.key(), p));
  }

  private Optional<PlayerStore> processMesssage(RecordMetadata metadata, Record<String, byte[]> record) {
    if(record.key() == null){
        return Optional.empty();
    }
    try {
      switch (metadata.topic()) {
        case Configuration.Topic.PLAYERS:
          return onTopicPlayers(record);
        case Configuration.Topic.LOGINS:
          return onTopicLogins(record);
        case Configuration.Topic.WAGERING:
          return onTopicWagering(record);
        case Configuration.Topic.WALLET:
          return onTopicWallet(record);
        case Configuration.Topic.PLAYER_STATUS:
          return onTopicPlayerStatus(record);
        default:
          return Optional.empty();
      }
    } catch(com.fasterxml.jackson.core.JsonParseException pfff) {
        logger.warn("unable to read", pfff);  
        return Optional.empty();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Optional<PlayerStore> onTopicPlayerStatus(Record<String, byte[]> record)
      throws StreamReadException, DatabindException, IOException {
    final PlayerStatusKafka kafka = mapper.readValue(record.value(), PlayerStatusKafka.class);
    PlayerBlockedSqs.transform(kafka)
        .ifPresent(
            sqs -> sender.send(kafka.brand_id.get(), kafka.player_id.get(), Type.GENERIC_USER, sqs.mappingSelector,
                sqs));

    return Optional.empty();
  }

  private Optional<PlayerStore> onTopicWallet(Record<String, byte[]> record)
      throws StreamReadException, DatabindException, IOException {
    final WalletKafka kafka = mapper.readValue(record.value(), WalletKafka.class);
    WalletSqs.transform(kafka)
        .ifPresent(
            sqs -> sender.send(kafka.brand_id.get(), kafka.player_id.get(), Type.GENERIC_USER, sqs.mappingSelector,
                sqs)

        );
    return Optional.empty();
  }

  private Optional<PlayerStore> onTopicWagering(Record<String, byte[]> record) throws IOException {
    final WageringKafka kafka = mapper.readValue(record.value(),
        WageringKafka.class);

    // TODO missing specification for 1000
    if (kafka.player_id.isPresent() && kafka.brand_id.isPresent()
        && kafka.win_amount.map(Double::parseDouble).filter(v -> v > 1000).isPresent()) {

      final BigWinSqs bw = new BigWinSqs();
      bw.player_id = kafka.player_id.get();
      bw.big_win = kafka.win_amount.get();
      sender.send(kafka.brand_id.get(), kafka.player_id.get(), Type.GENERIC_USER, Selector.BIG_WIN,
          bw);
    }
    return Optional.empty();
  }

  private Optional<PlayerStore> onTopicLogins(Record<String, byte[]> record) throws IOException {
    final LoginKafka kafka = mapper.readValue(record.value(),
        LoginKafka.class);

    final LoginSqs sqs = LoginSqs.transform(kafka);
    sender.send(sqs.brand_id.get(), sqs.player_id.get(), Type.GENERIC_USER, Selector.PLAYER_LOGIN,
        sqs);

    return Optional.empty();
  }

  private Optional<PlayerStore> onTopicPlayers(Record<String, byte[]> record) throws IOException {

    final PlayerKafka player = mapper.readValue(record.value(),
        PlayerKafka.class);

    final Optional<PlayerStore> playerInStore = Optional.ofNullable(store.get(record.key()));
    var mappingSelector = playerInStore.isEmpty() || playerInStore.get().player.isEmpty() ? Selector.PLAYER_REGISTRATION
        : Selector.PLAYER_UPDATED;

    final Optional<PlayerSqs> oldPlayerSqs = playerInStore.flatMap(PlayerSqs::transform);

    final Optional<PlayerStore> newPlayer = playerInStore.or(() -> Optional.ofNullable(new PlayerStore()))
        .flatMap(ps -> ps.with(player));
    final Optional<PlayerSqs> newPlayerSqs = newPlayer.flatMap(PlayerSqs::transform);

    if (!Objects.equals(oldPlayerSqs, newPlayerSqs)) {
      sender.send(player.brand_id.get(), player.player_id.get(), Type.GENERIC_USER, mappingSelector,
          newPlayerSqs.get());
    }

    if ((oldPlayerSqs.isEmpty() || oldPlayerSqs.get().first_dep_datetime.isEmpty())
        && newPlayerSqs.flatMap(p -> p.first_dep_datetime).isPresent()) {
      var firstDepositDatetime = FirstDepositDatetimeSqs.transform(newPlayer.get());
      sender.send(player.brand_id.get(), player.player_id.get(), Type.GENERIC_USER, Selector.FIRST_DEPOSIT,
          firstDepositDatetime.get());

    }
    return newPlayer;
  }
}
