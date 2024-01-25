package com.sun.qing.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerTransaction {
//    // 1 初始化事务
//    void initTransactions();
//    // 2 开启事务
//    void beginTransaction() throws ProducerFencedException;
//    // 3 在事务内提交已经消费的偏移量（主要用于消费者）
//    void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
//                                  String consumerGroupId) throws
//            ProducerFencedException;
//    // 4 提交事务
//    void commitTransaction() throws ProducerFencedException;
//    // 5 放弃事务（类似于回滚事务的操作）
//    void abortTransaction() throws ProducerFencedException;
    public static void main(String[] args) {
        // 1. 初始化配置,序列化
        // 2. 配置事务,以及事务ID
        // 3. 创建生产者
        // 4. 初始化事务,开启事务
        // 5. 停止生产者.
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transaction_id_0");

        KafkaProducer<Object, Object> producer = new KafkaProducer<>(properties);
        producer.initTransactions();
        producer.beginTransaction();

        try {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>("first","sunqing" + i));

            }
        } catch (Exception e) {
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }
}
