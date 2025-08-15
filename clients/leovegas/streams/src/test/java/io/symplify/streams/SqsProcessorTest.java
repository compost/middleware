package io.symplify.streams;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.test.junit.QuarkusTest;
import io.symplify.store.PlayerStore;

import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

@QuarkusTest
public class SqsProcessorTest {
  MockProcessorContext<Void, Void> context;
  private SqsProcessor processor;
  @Inject
  SqsClient sqs;

  @ConfigProperty(name = "brand-queue")
  java.util.Map<String, String> brandQueue;

  @ConfigProperty(name = "mapping-selector-dev-prefix")
  java.util.Set<String> mappingSelectorDevPrefix = Collections.emptySet();

  @ConfigProperty(name = "message-group-id")
  String messageGroupId;

  @Test
  public void testProcessorPlayer() {

    context.setRecordMetadata(Configuration.Topic.PLAYERS, 0, 0);
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","brand_id":"1","country_id":"JP"}""".getBytes(), 0L));

    var messages = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"player_registration","properties":{"originalId":"p1","player_id":"p1","country_id":"JP","brand_id":"1"}}""",
        messages.get(0).body());
  }

  @Test
  public void testProcessorPlayerStatusWithBlockedEndDate() {

    context.setRecordMetadata(Configuration.Topic.PLAYER_STATUS, 0, 0);
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","brand_id":"1","player_blocked_reason":"asshole","blocked_end_date":"2099-09-09R"}"""
            .getBytes(),
        0L));

    var messages = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"self_exclusion","properties":{"player_id":"p1","blocked_reason":"asshole","blockedUntil":"2099-09-09"}}""",
        messages.get(0).body());
  }

  @Test
  public void testProcessorPlayerStatusWithoutBlockedEndDate() {

    context.setRecordMetadata(Configuration.Topic.PLAYER_STATUS, 0, 0);
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","brand_id":"1","player_blocked_reason":"asshole"}"""
            .getBytes(),
        0L));

    var messages = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"player_blocked","properties":{"player_id":"p1","blocked_reason":"asshole"}}""",
        messages.get(0).body());
  }

  //DISABLE @Test
  public void testProcessorWalletSuccessDeposit() {

    context.setRecordMetadata(Configuration.Topic.WALLET, 0, 0);
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","no":"yes", "brand_id":"1","transaction_type":"deposit_money", "transaction_status":"success", "transaction_datetime":"3333-33-33T", "amount": "12", "currency_description": "EURO"}"""
            .getBytes(),
        0L));

    var messages = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"success_deposit","properties":{"player_id":"p1","currency":"EURO","Transaction_Type":"deposit_money","Transaction_Amount":"12","success_deposit_date":"3333-33-33"}}""",
        messages.get(0).body());
  }

  @Test
  public void testProcessorWalletFailedDeposit() {

    context.setRecordMetadata(Configuration.Topic.WALLET, 0, 0);
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","no":"yes", "brand_id":"1","transaction_type":"deposit_money", "transaction_status":"failed", "transaction_datetime":"3333-33-33T", "amount": "12", "currency_description": "EURO"}"""
            .getBytes(),
        0L));

    var messages = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"failed_deposit","properties":{"player_id":"p1","currency":"EURO","Transaction_Type":"deposit_money","Transaction_Amount":"12","failed_deposit_date":"3333-33-33"}}""",
        messages.get(0).body());
  }

  // DISABLE @Test
  public void testProcessorWalletSuccessWithdrawal() {

    context.setRecordMetadata(Configuration.Topic.WALLET, 0, 0);
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","no":"yes", "brand_id":"1","transaction_type":"withdraw_money", "transaction_status":"success", "transaction_datetime":"3333-33-33T", "amount": "12", "currency_description": "EURO"}"""
            .getBytes(),
        0L));

    var messages = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"success_withdrawal","properties":{"player_id":"p1","currency":"EURO","Transaction_Type":"withdraw_money","Transaction_Amount":"12","success_withdrawal_date":"3333-33-33"}}""",
        messages.get(0).body());
  }

  @Test
  public void testProcessorWalletFailedWithdrawal() {

    context.setRecordMetadata(Configuration.Topic.WALLET, 0, 0);
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","no":"yes", "brand_id":"1","transaction_type":"withdraw_money", "transaction_status":"failed", "transaction_datetime":"3333-33-33T", "amount": "12", "currency_description": "EURO"}"""
            .getBytes(),
        0L));

    var messages = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"failed_withdrawal","properties":{"player_id":"p1","currency":"EURO","Transaction_Type":"withdraw_money","Transaction_Amount":"12","failed_withdrawal_date":"3333-33-33"}}""",
        messages.get(0).body());
  }

  @Test
  public void testProcessorPlayerFirstDepDatetime() {

    context.setRecordMetadata(Configuration.Topic.PLAYERS, 0, 0);
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","brand_id":"1","first_dep_datetime":"2029-09-12Tddd"}""".getBytes(), 0L));

    var messages = sqs
        .receiveMessage(ReceiveMessageRequest.builder().maxNumberOfMessages(2).queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(2, messages.size());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"player_registration","properties":{"originalId":"p1","player_id":"p1","first_dep_datetime":"2029-09-12","brand_id":"1"}}""",
        messages.get(0).body());

    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"first_deposit","properties":{"first_dep_datetime":"2029-09-12"}}""",
        messages.get(1).body());
  }

  @Test
  public void testProcessorPlayerFirstDepDatetimeTwice() {

    context.setRecordMetadata(Configuration.Topic.PLAYERS, 0, 0);
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","brand_id":"1","first_dep_datetime":"2029-09-12Tddd"}""".getBytes(), 0L));
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","brand_id":"1","first_dep_datetime":"2029-09-12Tddd", "country_id":"FR"}"""
            .getBytes(),
        0L));

    var messages = sqs
        .receiveMessage(ReceiveMessageRequest.builder().maxNumberOfMessages(10).queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(3, messages.size());

    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"player_registration","properties":{"originalId":"p1","player_id":"p1","first_dep_datetime":"2029-09-12","brand_id":"1"}}""",
        messages.get(0).body());

    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"first_deposit","properties":{"first_dep_datetime":"2029-09-12"}}""",
        messages.get(1).body());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"player_updated","properties":{"originalId":"p1","player_id":"p1","first_dep_datetime":"2029-09-12","country_id":"FR","brand_id":"1"}}""",
        messages.get(2).body());
  }

  @Test
  public void testProcessorPlayerFirstDepDatetimeAlready() {

    context.setRecordMetadata(Configuration.Topic.PLAYERS, 0, 0);
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","brand_id":"1"}""".getBytes(), 0L));
    processor.process(new Record<>("1-p1",
        """
            {"player_id":"p1","brand_id":"1","first_dep_datetime":"2029-09-12Tddd", "country_id":"FR"}"""
            .getBytes(),
        0L));

    var messages = sqs
        .receiveMessage(ReceiveMessageRequest.builder().maxNumberOfMessages(10).queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(3, messages.size());

    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"player_registration","properties":{"originalId":"p1","player_id":"p1","brand_id":"1"}}""",
        messages.get(0).body());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"player_updated","properties":{"originalId":"p1","player_id":"p1","first_dep_datetime":"2029-09-12","country_id":"FR","brand_id":"1"}}""",
        messages.get(1).body());

    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"first_deposit","properties":{"first_dep_datetime":"2029-09-12"}}""",
        messages.get(2).body());
  }

  @Test
  public void testProcessorPlayer_DEV() {

    context.setRecordMetadata(Configuration.Topic.PLAYERS, 0, 0);
    processor.process(new Record<>("2-p1",
        """
            {"player_id":"p1","brand_id":"2","country_id":"JP"}""".getBytes(), 0L));

    var messages = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"DEV_player_registration","properties":{"originalId":"p1","player_id":"p1","country_id":"JP","brand_id":"2"}}""",
        messages.get(0).body());
  }

  @Test
  public void testProcessorBigWin() {

    context.setRecordMetadata(Configuration.Topic.WAGERING, 0, 0);
    processor.process(new Record<>("2-p1",
        """
            {"player_id":"p1","brand_id":"2","win_amount":"1000.1"}""".getBytes(), 0L));

    var messages = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(brandQueue.get("1")).build())
        .messages();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertEquals(
        """
            {"type":"GENERIC_USER","contactId":"p1","mappingSelector":"DEV_big_win","properties":{"player_id":"p1","big_win":"1000.1"}}""",
        messages.get(0).body());
  }

  @BeforeEach
  public void beforeEach() {
    Sender sender = new Sender();
    sender.brandQueue = brandQueue;
    sender.messageGroupId = messageGroupId;
    sender.sqs = sqs;
    sender.mappingSelectorDevPrefix = mappingSelectorDevPrefix;
    ObjectMapperSerde<PlayerStore> playerStoreSerde = new ObjectMapperSerde<>(
        PlayerStore.class);
    context = new MockProcessorContext<>();

    KeyValueStore<String, PlayerStore> store = Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore(Configuration.Store.PLAYERS),
        org.apache.kafka.common.serialization.Serdes.String(),
        playerStoreSerde)
        .withLoggingDisabled()
        .withCachingDisabled()
        .build();
    store.init(context.getStateStoreContext(), store);
    context.addStateStore(store);

    processor = new SqsProcessor(sender);
    processor.init(context);

    sender.brandQueue.forEach((k, v) -> {
      try {
        sender.sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(v).build());
      } catch (QueueDoesNotExistException q) {
      }

      sender.sqs.createQueue(
          CreateQueueRequest
              .builder()
              .queueName(v.substring(v.lastIndexOf("/") + 1))
              .attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true"))
              .build());

    });

  }

}
