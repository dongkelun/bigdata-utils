package com.dkl.bigdata.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;


public class KafkaConsumerDemo extends KafkaDemo {
    public static void main(String[] args) {
        new KafkaConsumerDemo().consumeRecord();
    }

    private void consumeRecord() {
        String bootstrapServers = "indata-10-110-8-199.indata.com:6667";
        KafkaConsumer<String, String> consumer = getConsumer(bootstrapServers);
        String topic = "test-kafka-demo";
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, String>
                    records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    private KafkaConsumer<String, String> getConsumer(String bootstrapServers) {
        Properties props = getSecurityProperties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }
}
