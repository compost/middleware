package io.symplify.streams;

import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
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

    KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(
        Configuration.Store.PLAYERS);
    builder.addStateStore(Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), playerStoreSerde));

    builder.stream(
        Set.of(
            Configuration.Topic.PLAYERS,
            Configuration.Topic.LOGINS,
            Configuration.Topic.WAGERING,
            Configuration.Topic.TRANSACTIONS,
            Configuration.Topic.WALLET),
        Consumed.with(Serdes.String(), Serdes.ByteArray()))
        .process(() -> new SqsProcessor(sender), Configuration.Store.PLAYERS);
    return builder.build();
  }
}
