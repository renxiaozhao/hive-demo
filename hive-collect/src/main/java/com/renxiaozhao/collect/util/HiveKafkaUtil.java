package com.renxiaozhao.collect.util;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveKafkaUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveKafkaUtil.class);
    
	public static void main(String[] args) {
	    kafkaProducer("HIVE_HOOK", "hello2");
	}
	
	public static void kafkaProducer(String topicName,String value) {
	    Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.38.10:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // KafkaProducer 是线程安全的，可以多个线程使用用一个 KafkaProducer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, value);
        kafkaProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    LOGGER.error(String.format("发送数据到kafka发生异常: %s",e));
                    return;
                }
                LOGGER.info("发送数据到kafka成功, topic: " + metadata.topic() + " offset: " + metadata.offset() + " partition: "
                        + metadata.partition());
                LOGGER.info("从kafka获取数据topic数据: " + metadata.topic());
                kafkaConsumer(metadata.topic());
            }
        });
        kafkaProducer.close();
	}
	
	public static void kafkaConsumer(String topicName) {
        Properties props = new Properties();
        // 必须指定
        props.put("bootstrap.servers", "192.168.38.10:9092");
        // 必须指定
        props.put("group.id", "atlas");
        // 必须指定
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 必须指定
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 从最早的消息开始读取
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("KafkaConsumer获取"+topicName+"信息value=" +record.value());
                if(!DBMySqlUtil.testColletHiveMeta(record.value())) {
                    LOGGER.info("KafkaConsumer获取"+topicName+"信息，插入mysql库失败");
                }
            }
        } catch (ClassNotFoundException e) {
            LOGGER.error(String.format("kafkaConsumer调用DbUtil报错: %s", e));
        } catch (SQLException e) {
            LOGGER.error(String.format("kafkaConsumer调用DbUtil报错: %s", e));
        } finally {
            consumer.close();
        }
    }

}

