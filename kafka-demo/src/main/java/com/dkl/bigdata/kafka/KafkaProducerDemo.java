package com.dkl.bigdata.kafka;


import com.alibaba.fastjson2.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerDemo extends KafkaDemo {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerDemo.class);

    /**
     * 获取kafka的producer
     *
     * @return
     */
    private KafkaProducer getProducer() {
        String bootstrapServers = "indata-10-110-8-199.indata.com:6667";
        Properties props = getSecurityProperties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }


    private void produceOneRecord() {
//        {"id":1,"name":"hudi","price":10,"ts":1000,"dt":"2024-10-30"}
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", 1);
        jsonObject.put("name", "hudi");
        jsonObject.put("price", 10);
        jsonObject.put("ts", 1000);
        jsonObject.put("dt", "2024-10-30");
        String val = jsonObject.toJSONString();
        KafkaProducer<String, String> producer = getProducer();
        String topic = "test-kafka-demo";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, val);
        producer.send(record);
        producer.close();
    }


    public static void main(String[] args) {
        new KafkaProducerDemo().produceOneRecord();
    }

}

