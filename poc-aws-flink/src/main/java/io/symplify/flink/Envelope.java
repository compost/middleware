package io.symplify.flink;

public class Envelope {
    public String topic;
    public String key;
    public byte[] value;
    public long timestamp;

    public Envelope() {}

    public Envelope(String topic, String key, byte[] value, long timestamp) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }
}
