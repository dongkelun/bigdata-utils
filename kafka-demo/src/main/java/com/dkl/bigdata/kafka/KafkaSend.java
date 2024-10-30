package com.dkl.bigdata.kafka;


import com.alibaba.fastjson2.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaSend {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSend.class);

    /**
     * 获取kafka的producer
     *
     * @return
     */
    private static KafkaProducer getProducer() {
        String bootstrapServers = "indata-10-110-8-199.indata.com:6667";
        Map<String, String> loginParam = getLoginParam();
        Properties props = SecurityUtils.getSecurityProperties(loginParam);
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);

//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    private static Map<String, String> getLoginParam() {
        String principal = "kafka/indata-10-110-8-199.indata.com@INDATA.COM";
        String keytab = Thread.currentThread().getContextClassLoader().getResource("kafka.service.keytab").getPath();
        String krb5 = Thread.currentThread().getContextClassLoader().getResource("krb5.conf").getPath();

        Map<String, String> loginParam = new HashMap<>();
        loginParam.put("principal", principal);
        loginParam.put("keytab", keytab);
        loginParam.put("krb5", krb5);
        return loginParam;
    }

    private static void produceOneRecord() {
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
        produceOneRecord();
    }

}

