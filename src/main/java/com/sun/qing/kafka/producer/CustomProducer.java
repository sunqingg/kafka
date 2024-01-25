package com.sun.qing.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) {

        // 0. 填写kafka需要的配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.2.22:9092,192.168.2.21:9092");
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,String.class);
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,String.class);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        // 1. 创建kafka生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 2. 发送消息
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("first","idea 发过来的" + i));

        }


        // 3.关闭kafka生产者
        producer.close();
    }
}
