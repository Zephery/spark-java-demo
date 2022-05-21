package com.spark.es.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wenzhihuai
 * @since 2022/5/15 20:52
 */
public class KafkaSendTest {
    @Test
    public void send() {
        String brokers = "106.52.188.206:30437";

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        kafkaParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaParams);

        producer.send(new ProducerRecord<>("kafka-streaming-test", "joifjaoie a wefejofawjof "));
        producer.close();

    }
}
