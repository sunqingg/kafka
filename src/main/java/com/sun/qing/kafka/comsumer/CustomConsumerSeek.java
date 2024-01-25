package com.sun.qing.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class CustomConsumerSeek {
    public static void main(String[] args) {
        // 0.配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop101:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test3");
        // 1.创建一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        ArrayList<String> topics = new ArrayList<>();
        topics.add("atguigu");
        consumer.subscribe(topics);
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0){
            consumer.poll(Duration.ofSeconds(1));
            // 获取消费者分区分配信息（有了分区分配信息才能开始消费）
            assignment = consumer.assignment();
        }
        // 遍历所有分区，并指定 offset 从 1700 的位置开始消费
        for (TopicPartition tp: assignment) {
            consumer.seek(tp, 0);
        }
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }



        // 2.读取信息

    }
}
