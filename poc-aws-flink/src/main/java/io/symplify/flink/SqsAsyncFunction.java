package io.symplify.flink;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SqsAsyncFunction extends RichAsyncFunction<SqsRequest, Void> {

    private static final Logger logger = LoggerFactory.getLogger(SqsAsyncFunction.class);

    private transient SqsAsyncClient sqsClient;
    private final String sqsEndpoint;
    private final String region;

    public SqsAsyncFunction(String sqsEndpoint, String region) {
        this.sqsEndpoint = sqsEndpoint;
        this.region = region;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        var builder = SqsAsyncClient.builder()
                .httpClientBuilder(NettyNioAsyncHttpClient.builder());

        if (sqsEndpoint != null && !sqsEndpoint.isEmpty()) {
            builder.endpointOverride(java.net.URI.create(sqsEndpoint));
        }
        if (region != null && !region.isEmpty()) {
            builder.region(software.amazon.awssdk.regions.Region.of(region));
        }

        sqsClient = builder.build();
    }

    @Override
    public void close() throws Exception {
        if (sqsClient != null) {
            sqsClient.close();
        }
    }

    @Override
    public void asyncInvoke(SqsRequest input, ResultFuture<Void> resultFuture) {
        CompletableFuture<Void> future = sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(input.getQueueUrl())
                .messageBody(input.getBody())
                .messageDeduplicationId(input.getDeduplicationId())
                .messageGroupId(input.getMessageGroupId())
                .build())
                .thenAccept(response -> {
                    logger.debug("Successfully sent message to SQS: {}", response.messageId());
                })
                .exceptionally(throwable -> {
                    logger.error("Failed to send message to SQS", throwable);
                    // Depending on requirements, we might want to fail the job or just log it
                    // Failing the job ensures at-least-once via Flink retry
                    throw new RuntimeException("Failed to send message to SQS", throwable);
                });

        // Bridge the Java CompletableFuture to Flink's ResultFuture
        future.whenComplete((v, t) -> {
            if (t != null) {
                resultFuture.completeExceptionally(t);
            } else {
                resultFuture.complete(Collections.emptyList());
            }
        });
    }
}
