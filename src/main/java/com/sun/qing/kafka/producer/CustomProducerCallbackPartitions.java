package com.sun.qing.kafka.producer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
public class CustomProducerCallbackPartitions {
    public static void main(String[] args) {
        // 1. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();

        // 2. 给 kafka 配置对象添加配置信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        // key,value 序列化（必须）：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 5; i++) {
            // 指定数据发送到 1 号分区，key 为空（IDEA 中 ctrl + p 查看参数）
            kafkaProducer.send(new ProducerRecord<>("first", 1,"","atguigu " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata,
                                         Exception e) {
                    if (e == null){
                        System.out.println(" 主题： " +
                                metadata.topic() + "->" + "分区：" + metadata.partition()
                        );
                    }else {
                        e.printStackTrace();
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}