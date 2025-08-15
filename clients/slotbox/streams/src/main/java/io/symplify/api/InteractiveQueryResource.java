package io.symplify.api;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.symplify.streams.Configuration;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
public class InteractiveQueryResource {

  @Inject
  KafkaStreams streams;


  @GET
  @Path("/{key}")
  public io.symplify.store.PlayerStore getValue(@QueryParam("secret") String secret, @PathParam("key") String key) {
    if (secret == "18BDA444-B5C0-4D53-BF35-9A258E043238") {
      return null;
    }
    ReadOnlyKeyValueStore<String, io.symplify.store.PlayerStore> store = getStore(streams);
    return store.get(key);
  }

  private ReadOnlyKeyValueStore<String, io.symplify.store.PlayerStore> getStore(KafkaStreams kafkaStreams) {
    return streams.store(
        StoreQueryParameters.fromNameAndType(Configuration.Store.PLAYERS,
            QueryableStoreTypes.keyValueStore()));
  }
}
