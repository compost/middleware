package io.symplify.flink;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import io.symplify.streams.Configuration.Topic;

public class FlinkJob {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = params.get("bootstrap.servers", "localhost:9092");
        String groupId = params.get("group.id", "flink-sqs-processor");
        String messageGroupId = params.get("message.group.id", "default-group");
        String sqsEndpoint = params.get("sqs.endpoint");
        String region = params.get("aws.region", "eu-west-1");
        
        // Parse brand queues: brand1=url1,brand2=url2
        Map<String, String> brandQueues = new HashMap<>();
        String brandQueuesStr = params.get("brand-queues", "");
        if (!brandQueuesStr.isEmpty()) {
            for (String part : brandQueuesStr.split(",")) {
                String[] kv = part.split("=");
                if (kv.length == 2) {
                    brandQueues.put(kv[0].trim(), kv[1].trim());
                }
            }
        }
        
        Set<String> devPrefixBrands = new HashSet<>(Arrays.asList(params.get("dev-prefix-brands", "").split(",")));

        Sender sender = new Sender(brandQueues, devPrefixBrands, messageGroupId);

        DataStream<Envelope> players = createSource(env, bootstrapServers, groupId, Topic.PLAYERS);
        DataStream<Envelope> playersNew = createSource(env, bootstrapServers, groupId, Topic.PLAYERS_NEW);
        DataStream<Envelope> logins = createSource(env, bootstrapServers, groupId, Topic.LOGINS);
        DataStream<Envelope> wagering = createSource(env, bootstrapServers, groupId, Topic.WAGERING);
        DataStream<Envelope> wallet = createSource(env, bootstrapServers, groupId, Topic.WALLET);
        DataStream<Envelope> transactions = createSource(env, bootstrapServers, groupId, Topic.TRANSACTIONS);
        DataStream<Envelope> consent = createSource(env, bootstrapServers, groupId, Topic.USER_CONSENT_UPDATE);

        DataStream<Envelope> unionStream = players
                .union(playersNew)
                .union(logins)
                .union(wagering)
                .union(wallet)
                .union(transactions)
                .union(consent);

        DataStream<SqsRequest> sqsRequests = unionStream
                .keyBy(e -> e.key != null ? e.key : "unknown")
                .process(new SqsProcessFunction(sender));

        AsyncDataStream.unorderedWait(
                sqsRequests,
                new SqsAsyncFunction(sqsEndpoint, region),
                1000, TimeUnit.MILLISECONDS, // Timeout
                100 // Capacity
        ).addSink(new DiscardingSink<>());

        env.execute("Flink SQS Processor");
    }

    private static DataStream<Envelope> createSource(StreamExecutionEnvironment env, String bootstrapServers, String groupId, String topic) {
        KafkaSource<Envelope> source = KafkaSource.<Envelope>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new EnvelopeDeserializer(topic))
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source " + topic);
    }

    public static class EnvelopeDeserializer implements KafkaRecordDeserializationSchema<Envelope> {
        private final String topic;

        public EnvelopeDeserializer(String topic) {
            this.topic = topic;
        }

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Envelope> out) throws IOException {
            String key = record.key() != null ? new String(record.key()) : null;
            out.collect(new Envelope(topic, key, record.value(), record.timestamp()));
        }

        @Override
        public TypeInformation<Envelope> getProducedType() {
            return TypeInformation.of(Envelope.class);
        }
    }
}
