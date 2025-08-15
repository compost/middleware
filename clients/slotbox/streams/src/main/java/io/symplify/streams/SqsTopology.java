package io.symplify.streams;

import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.symplify.kafka.*;
import io.symplify.store.PlayerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

@ApplicationScoped
public class SqsTopology {

  @Inject
  Sender sender;

  @Produces
  public Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    ObjectMapperSerde<PlayerStore> playerStoreSerde = new ObjectMapperSerde<>(
        PlayerStore.class);

    ObjectMapperSerde<PlayerKafka> playerSerde = new ObjectMapperSerde<>(
        PlayerKafka.class);

    ObjectMapperSerde<PlayerConsentKafka> playerConsentSerde = new ObjectMapperSerde<>(
        PlayerConsentKafka.class);

    ObjectMapperSerde<WalletKafka> walletSerde = new ObjectMapperSerde<>(
        WalletKafka.class);

    ObjectMapperSerde<PlayerStatusKafka> playerStatusSerde = new ObjectMapperSerde<>(
        PlayerStatusKafka.class);

    KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(
        Configuration.Store.PLAYERS);
    builder.addStateStore(Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), playerStoreSerde));

    builder.stream(Set.of(Configuration.Topic.PLAYERS),
        Consumed.with(Serdes.String(), playerSerde))
        .selectKey((k, v) -> selectKey(v.brand_id, v.player_id))
        .to(Configuration.Topic.REPARTITIONED_PLAYERS, Produced.with(Serdes.String(), playerSerde));

    builder.stream(Set.of(Configuration.Topic.PLAYER_CONSENT),
        Consumed.with(Serdes.String(), playerConsentSerde))
        .selectKey((k, v) -> selectKey(v.brand_id, v.player_id))
        .to(Configuration.Topic.REPARTITIONED_PLAYER_CONSENT, Produced.with(Serdes.String(), playerConsentSerde));

    builder.stream(Set.of(Configuration.Topic.WALLET),
        Consumed.with(Serdes.String(), walletSerde))
        .selectKey((k, v) -> selectKey(v.brand_id, v.player_id))
        .to(Configuration.Topic.REPARTITIONED_WALLET, Produced.with(Serdes.String(), walletSerde));

    builder.stream(Set.of(Configuration.Topic.PLAYER_STATUS),
        Consumed.with(Serdes.String(), playerStatusSerde))
        .selectKey((k, v) -> selectKey(v.brand_id, v.player_id))
        .to(Configuration.Topic.REPARTITIONED_PLAYER_STATUS, Produced.with(Serdes.String(), playerStatusSerde));


    builder.stream(
        Set.of(
            Configuration.Topic.REPARTITIONED_PLAYERS,
            Configuration.Topic.REPARTITIONED_WALLET,
            Configuration.Topic.REPARTITIONED_PLAYER_STATUS,
            Configuration.Topic.REPARTITIONED_PLAYER_CONSENT),
        Consumed.with(Serdes.String(), Serdes.ByteArray()))
        .process(() -> new SqsProcessor(sender), Configuration.Store.PLAYERS);

    return builder.build();
  }

  private String selectKey(Optional<String> brandId, Optional<String> playerId) {
    return brandId.orElse("") + "-" + playerId.orElse("");
  }
}
