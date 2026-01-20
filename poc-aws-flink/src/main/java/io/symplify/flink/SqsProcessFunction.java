package io.symplify.flink;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.symplify.kafka.LoginKafka;
import io.symplify.kafka.PlayerKafka;
import io.symplify.kafka.TransactionKafka;
import io.symplify.kafka.UserContentUpdateKafka;
import io.symplify.kafka.WageringKafka;
import io.symplify.kafka.WalletKafka;
import io.symplify.sqs.BigWinSqs;
import io.symplify.sqs.FirstDepositDatetimeSqs;
import io.symplify.sqs.LoginSqs;
import io.symplify.sqs.PlayerBlockedSqs;
import io.symplify.sqs.PlayerSqs;
import io.symplify.sqs.PurgeSqs;
import io.symplify.sqs.TransactionSqs;
import io.symplify.sqs.Transformer;
import io.symplify.sqs.UserContentUpdateSqs;
import io.symplify.sqs.WalletSqs;
import io.symplify.store.PlayerStore;
import io.symplify.streams.Configuration.Mapping.Selector;
import io.symplify.streams.Configuration.Mapping.Type;
import io.symplify.streams.Configuration.Topic;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.sqs.SqsClient;

public class SqsProcessFunction extends KeyedProcessFunction<String, Envelope, SqsRequest> {

    private static final Logger logger = LoggerFactory.getLogger(SqsProcessFunction.class);
    private static final ObjectMapper mapper = Transformer.getObjectMapper();

    private final Sender sender;
    private transient ValueState<PlayerStore> store;

    public SqsProcessFunction(Sender sender) {
        this.sender = sender;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        store = getRuntimeContext().getState(new ValueStateDescriptor<>("player-store", PlayerStore.class));
    }
    
    @Override
    public void processElement(Envelope envelope, Context ctx, Collector<SqsRequest> out) throws Exception {
        try {
            switch (envelope.topic) {
                case Topic.PLAYERS:
                case Topic.PLAYERS_NEW:
                    onTopicPlayers(envelope, out);
                    break;
                case Topic.LOGINS:
                    onTopicLogins(envelope, out);
                    break;
                case Topic.WAGERING:
                    onTopicWagering(envelope, out);
                    break;
                case Topic.WALLET:
                    onTopicWallet(envelope, out);
                    break;
                case Topic.TRANSACTIONS:
                    onTopicTransactions(envelope, out);
                    break;
                case Topic.USER_CONSENT_UPDATE:
                    onTopicUserContentUpdate(envelope, out);
                    break;
                default:
                    // Ignore
            }
        } catch (Exception e) {
            logger.error("Error processing element for topic {}", envelope.topic, e);
        }
    }

    private void onTopicUserContentUpdate(Envelope record, Collector<SqsRequest> out) throws IOException {
        final var kafka = mapper.readValue(record.value, UserContentUpdateKafka.class);
        final var sqs = UserContentUpdateSqs.transform(kafka);
        sender.createRequestFull(sqs.brand_id.orElse("unknown"), sqs.player_id.orElse("unknown"), sqs.type, sqs.mappingSelector, sqs)
                .ifPresent(out::collect);
    }

    private void onTopicTransactions(Envelope record, Collector<SqsRequest> out) throws IOException {
        final var kafka = mapper.readValue(record.value, TransactionKafka.class);
        final var sqs = TransactionSqs.transform(kafka);
        sender.createRequestFull(sqs.brand_id, sqs.player_id, sqs.type, sqs.mappingSelector, sqs)
                .ifPresent(out::collect);
    }

    private void onTopicWallet(Envelope record, Collector<SqsRequest> out) throws IOException {
        final WalletKafka kafka = mapper.readValue(record.value, WalletKafka.class);
        WalletSqs.transform(kafka)
                .ifPresent(sqs -> sender.createRequest(kafka.brand_id.orElse("unknown"), kafka.player_id.orElse("unknown"), Type.GENERIC_USER, sqs.mappingSelector, sqs)
                        .ifPresent(out::collect));
    }

    private void onTopicWagering(Envelope record, Collector<SqsRequest> out) throws IOException {
        final WageringKafka kafka = mapper.readValue(record.value, WageringKafka.class);

        if (kafka.player_id.isPresent() && kafka.brand_id.isPresent()
                && kafka.win_amount.map(Double::parseDouble).filter(v -> v > 1000).isPresent()) {

            final BigWinSqs bw = new BigWinSqs();
            bw.player_id = kafka.player_id.get();
            bw.big_win = kafka.win_amount.get();
            bw.brand = kafka.brand;
            sender.createRequest(kafka.brand_id.get(), kafka.player_id.get(), Type.GENERIC_USER, Selector.BIG_WIN, bw)
                    .ifPresent(out::collect);
        }
    }

    private void onTopicLogins(Envelope record, Collector<SqsRequest> out) throws IOException {
        final LoginKafka kafka = mapper.readValue(record.value, LoginKafka.class);
        final LoginSqs sqs = LoginSqs.transform(kafka);
        sender.createRequest(sqs.brand_id.orElse("unknown"), sqs.player_id.orElse("unknown"), Type.GENERIC_USER, Selector.PLAYER_LOGIN, sqs)
                .ifPresent(out::collect);
    }

    private void onTopicPlayers(Envelope record, Collector<SqsRequest> out) throws IOException {
        final PlayerKafka player = mapper.readValue(record.value, PlayerKafka.class);
        final Optional<PlayerStore> playerInStore = Optional.ofNullable(store.value());
        
        var mappingSelector = playerInStore.isEmpty() || playerInStore.get().player.isEmpty() 
                ? Selector.PLAYER_REGISTRATION
                : Selector.PLAYER_UPDATED;

        final Optional<PlayerSqs> oldPlayerSqs = playerInStore.flatMap(PlayerSqs::transform);

        final Optional<PlayerStore> newPlayer = playerInStore.or(() -> Optional.ofNullable(new PlayerStore()))
                .flatMap(ps -> ps.with(player));
        
        if (newPlayer.isPresent()) {
            store.update(newPlayer.get());
        }

        final Optional<PlayerSqs> newPlayerSqs = newPlayer.flatMap(PlayerSqs::transform);

        if (player.brand_id.isPresent() && player.player_id.isPresent()) {
            if (!Objects.equals(oldPlayerSqs, newPlayerSqs)) {
                sender.createRequest(player.brand_id.get(), player.player_id.get(), Type.GENERIC_USER, mappingSelector, newPlayerSqs.get())
                        .ifPresent(out::collect);
            }

            if ((oldPlayerSqs.isEmpty() || oldPlayerSqs.get().first_dep_datetime.isEmpty())
                    && newPlayerSqs.flatMap(p -> p.first_dep_datetime).isPresent()) {
                var firstDepositDatetime = FirstDepositDatetimeSqs.transform(newPlayer.get());
                sender.createRequest(player.brand_id.get(), player.player_id.get(), Type.GENERIC_USER, Selector.FIRST_DEPOSIT, firstDepositDatetime.get())
                        .ifPresent(out::collect);
            }

            if (player.accountStatus.map(v -> v.equalsIgnoreCase(PlayerKafka.PURGE)).orElse(false)) {
                var toSend = PurgeSqs.transform(player);
                sender.createRequest(player.brand_id.get(), player.player_id.get(), Type.GENERIC_USER, Selector.PLAYER_PURGE, toSend)
                        .ifPresent(out::collect);
            }

            if (newPlayerSqs.isPresent() && newPlayerSqs.get().locked.isPresent()) {
                boolean oldLockedEmpty = oldPlayerSqs.isEmpty() || oldPlayerSqs.get().locked.isEmpty();
                boolean lockedChanged = oldLockedEmpty || !newPlayerSqs.get().locked.get().equalsIgnoreCase(oldPlayerSqs.get().locked.get());
                
                if (lockedChanged) {
                    var playerId = player.player_id.get();
                    var locked = newPlayerSqs.flatMap(p -> p.locked).map(String::toLowerCase).get();
                    var brand = newPlayerSqs.get().brand;
                    PlayerBlockedSqs sqs = PlayerBlockedSqs.transform(playerId, locked, brand);
                    sender.createRequestFull(player.brand_id.get(), player.player_id.get(), Type.USER_BLOCKED_TOGGLE, Selector.PLAYER_BLOCKED, sqs)
                            .ifPresent(out::collect);
                }
            }
        }
    }
}

