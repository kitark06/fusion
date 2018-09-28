package com.kartikiyer.fusion.io;


import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;


/**
 * The class KafkaWriter is a wrapper for the KafkaProducer. Can be considered to be a data access object pattern.
 * <p>
 * NOTE :
 * <br>
 * Methods of this class write messages silently (log errors without throwing exception) in ASYNC mode.
 *
 * @author Kartik Iyer
 * @param <K>
 *             The generic key type
 * @param <V>
 *             The generic value type
 */
public class KafkaWriter<K, V> implements AutoCloseable
{
	Logger				log	= Logger.getLogger(KafkaWriter.class);

	private String			topicName;
	private Producer<K, V>	producer;


	/**
	 * Instantiates a new kafka writer using the passed arguments.
	 *
	 * @param kafkaClusterIp
	 *             the kafka cluster ip
	 * @param clientId
	 *             the client id
	 * @param keySerializer
	 *             the key serializer
	 * @param valueSerializer
	 *             the value serializer
	 * @param topicName
	 *             the topic name
	 */
	public KafkaWriter(String kafkaClusterIp, String clientId, String keySerializer, String valueSerializer, String topicName)
	{
		this(buildKafkaProperties(kafkaClusterIp, clientId, keySerializer, valueSerializer, topicName));
	}

	/**
	 * Instantiates a new kafka writer. Accepts a property object containing all the properties which is passed to the KafkaProducer
	 *
	 * @param kafkaProps
	 *             the kafka props which will be passed to the KafkaProducer
	 */
	public KafkaWriter(Properties kafkaProps)
	{
		this.topicName = kafkaProps.getProperty("topicNm");
		producer = new KafkaProducer<>(kafkaProps);
	}

	/**
	 * Builds the kafkaProperties with the bare minimum properties required for execution.
	 *
	 * @param kafkaClusterIp
	 *             the kafka cluster ip
	 * @param clientId
	 *             the client id
	 * @param keySerializer
	 *             the key serializer
	 * @param valueSerializer
	 *             the value serializer
	 * @param topicName
	 *             the topic name
	 * @return the properties
	 */
	private static Properties buildKafkaProperties(String kafkaClusterIp, String clientId, String keySerializer, String valueSerializer, String topicName)
	{
		Properties kafkaProps = new Properties();
		kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClusterIp);
		kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
		kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
		kafkaProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		kafkaProps.put("topicNm", topicName);
		return kafkaProps;
	}


	/**
	 * Writes a [Key,Value] message silently in async mode.
	 * <br>
	 * Any exception thrown during writing the msg to kafka, is caught and dealt by logging it silently.
	 *
	 * @param key
	 *             the key
	 * @param value
	 *             the value
	 */
	public void writeMessageSilently(K key, V value)
	{
		ProducerRecord<K, V> record = new ProducerRecord<>(topicName, key, value);
		Future<RecordMetadata> metadata = producer.send(record, (metadata1, exception) ->
		{
			if (exception != null)
				log.error(exception.toString());
		});
	}

	/**
	 * Writes a message with Null Key to Kafka silently.
	 * <br>
	 * Any exception thrown during writing the msg to kafka, is caught and dealt by logging it silently.
	 * Uses the overloaded {@link com.kartikiyer.fusion.io.KafkaWriter#writeMessageSilently writeMessageSilently} of this class
	 *
	 * @param value
	 *             the value
	 */
	public void writeMessageSilently(V value)
	{
		writeMessageSilently(null, value);
	}

	/* (non-Javadoc)
	 * @see java.lang.AutoCloseable#close()
	 */
	@Override
	public void close()
	{
		producer.close();
	}
}
