package com.kartikiyer.fusion.util;


import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;


/**
 * The Class containing static utility methods used throughout the project.
 *
 * @author Kartik Iyer
 */
public class CommonUtilityMethods
{

	/**
	 * Returns a new ExclusionStrategy which will be registered with the Gson to skip the primary key during serialization.
	 * This is done because we want the pk to be present only once in the final {@link EnrichedData}.
	 * <p>
	 * Without registering this custom strategy, output EnrichedData will have the field pk [which is PCN] present multiple times,
	 * once for each field which has the property nested inside.
	 *
	 * @param keyName
	 *             the key name
	 * @return the exclusion strategy
	 */
	public static ExclusionStrategy excludeKeyDuringSerializer(String keyName)
	{
		return new ExclusionStrategy()
		{
			@Override
			public boolean shouldSkipField(FieldAttributes field)
			{
				return field.getName().equalsIgnoreCase(keyName);
			}

			@Override
			public boolean shouldSkipClass(Class<?> clazz)
			{
				return false;
			}
		};
	}

	/**
	 * Uses reflection to return a Map containing the [Class object , fieldName] of all the fields of the class.
	 *
	 * @param enrichedData
	 *             the class whose fields are required
	 * @return Map containing the [Class object , fieldName]
	 */
	public static Map<Class, String> getFieldsOfDataModel(Object enrichedData)
	{
		Map<Class, String> fields = new HashMap<>();

		Class<?> clazz = enrichedData.getClass();
		Field[] declaredFields = clazz.getDeclaredFields();

		for (int i = 0; i < declaredFields.length; i++)
		{
			Field field = declaredFields[i];
			fields.put(field.getType(), declaredFields[i].getName());
		}
		return fields;
	}

	/**
	 * Gets the flink kakfa consumer.
	 *
	 * @param <T>
	 *             the generic type
	 * @param kafkaTopics
	 *             the kafka topics
	 * @param deserializer
	 *             the deserializer
	 * @return the flink kakfa consumer
	 */
	public static <T> FlinkKafkaConsumer011<T> getFlinkKakfaConsumer(List<String> kafkaTopics, DeserializationSchema<T> deserializer)
	{
		Properties properties = new Properties();
		properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_IP_PORT);
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FUSION_CONSUMER_GROUP);

		FlinkKafkaConsumer011<T> flinkKafkaConsumer011 = new FlinkKafkaConsumer011<T>(kafkaTopics, deserializer, properties); // or use Pattern.compile("\\w+-fusionStream")
		flinkKafkaConsumer011.setStartFromEarliest();
		return flinkKafkaConsumer011;
	}

	/**
	 * Builds and returns a ParameterTool with default global job parameters & values.
	 *
	 * @return the parameter tool
	 */
	public static ParameterTool buildGlobalJobParameters()
	{
		Map<String, String> parameters = getDefaults();
		return ParameterTool.fromMap(parameters);
	}

	/**
	 * Builds and returns a ParameterTool with default global job parameters & values along with the extra parameters provided.
	 *
	 * @param extraParameters
	 *             the extra parameters
	 * @return the parameter tool
	 */
	public static ParameterTool buildGlobalJobParameters(Map<String, String> extraParameters)
	{
		Map<String, String> parameters = new HashMap<>(extraParameters);
		parameters.putAll(getDefaults());
		return ParameterTool.fromMap(parameters);
	}

	/**
	 * Returns the defaults parameters.
	 *
	 * @return the defaults
	 */
	private static Map<String, String> getDefaults()
	{
		Map<String, String> parameters = new HashMap<>();
		parameters.put("mappingType", ES_DEFAULT_INDEX_TYPE);
		parameters.put("patientInfoIndexName", PATIENT_INFO);
		parameters.put("elasticSearchClusterIp", ELASTIC_SEARCH_CLUSTER_IP);
		parameters.put("elasticSearchClusterPort", String.valueOf(ELASTIC_SEARCH_CLUSTER_PORT));
		return parameters;
	}
}
