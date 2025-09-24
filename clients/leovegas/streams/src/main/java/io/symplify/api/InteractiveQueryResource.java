package io.symplify.api;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.core.MediaType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.KeyValue;

import io.symplify.streams.Configuration;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
public class InteractiveQueryResource {

  @Inject
  KafkaStreams streams;


  @GET
  @Path("/{store}/{key}")
  public io.symplify.store.PlayerStore getPlayerStore(@HeaderParam("Authorization") String authToken, @PathParam("store") String storeName, @PathParam("key") String key) {
    if (!authToken.equals("18BDA444-B5C0-4D53-BF35-9A258E043238")) {
      return null;
    }
    ReadOnlyKeyValueStore<String, io.symplify.store.PlayerStore> store = getStore(streams, storeName);
    return store.get(key);
  }

  private ReadOnlyKeyValueStore<String, io.symplify.store.PlayerStore> getStore(KafkaStreams kafkaStreams, String name) {
    return streams.store(
        StoreQueryParameters.fromNameAndType(name,
            QueryableStoreTypes.keyValueStore()));
  }
}
