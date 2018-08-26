package com.kartikiyer.fusion.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;


public class KafkaWriter<K, V> implements AutoCloseable {
    Logger log = Logger.getLogger(KafkaWriter.class);

    private Properties kafkaProps;
    private String topicName;
    private Producer<K, V> producer;


    public KafkaWriter(String kafkaClusterIp, String clientId, String keySerializer, String valueSerializer, String topicName) {
        kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClusterIp);
        kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        kafkaProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        this.topicName = topicName;
        producer = new KafkaProducer<>(kafkaProps);
    }

    public KafkaWriter(Properties kafkaProps, String topicName) {
        this.kafkaProps = kafkaProps;
        this.topicName = topicName;
    }

    public void writeMessage(K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topicName, key, value);
        Future<RecordMetadata> metadata = producer.send(record, (metadata1, exception) ->
        {
            if (exception != null)
                log.error(exception.toString());
        });
    }

    public void writeMessage(V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topicName, null, value);
        Future<RecordMetadata> metadata = producer.send(record, (metadata1, exception) ->
        {
            if (exception != null)
                log.error(exception.toString());
        });
    }

    @Override
    public void close() {
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
        KafkaWriter<String, String> stringStringKafkaWriter = new KafkaWriter<>("localhost:9092", "FusionTestClient", StringSerializer.class.getName(), StringSerializer.class.getName(), "FusionTestTopic");
    }
}
