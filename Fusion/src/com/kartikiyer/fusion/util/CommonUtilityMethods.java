package com.kartikiyer.fusion.util;

import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;

public class CommonUtilityMethods
{
	public static ExclusionStrategy excludeKeyDuringSerializer(String keyName)
	{
		return new ExclusionStrategy()
		{
			@Override
			public boolean shouldSkipField(FieldAttributes field)
			{
				return field	.getName()
							.equalsIgnoreCase(keyName);
			}

			@Override
			public boolean shouldSkipClass(Class<?> clazz)
			{
				return false;
			}
		};
	}

	public static Map<Class, String> getFieldsOfDataModel(Object enrichedData)
	{
		Map<Class, String> fields = new HashMap<>();

		Class<?> clazz = enrichedData.getClass();
		for (int i = 0; i < clazz.getDeclaredFields().length; i++)
		{
			Field field = clazz.getDeclaredFields()[i];
			fields.put(field.getType(), clazz.getDeclaredFields()[i].getName());
		}
		return fields;
	}

	public static <T> FlinkKafkaConsumer011<T> getFlinkKakfaConsumer(List<String> kafkaTopics,DeserializationSchema<T> deserializer)
	{
		Properties properties = new Properties();
		properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_IP_PORT);
		//TODO why +1 ?
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FUSION_CONSUMER_GROUP/*+1*/);

		FlinkKafkaConsumer011<T> flinkKafkaConsumer011 = new FlinkKafkaConsumer011<T>(kafkaTopics, deserializer,properties); // or use Pattern.compile("\\w+-fusionStream")
		flinkKafkaConsumer011.setStartFromEarliest();
		return flinkKafkaConsumer011;
	}

	public static ParameterTool buildGlobalJobParameters()
	{
		Map<String, String> parameters = getDefaults();
		return ParameterTool.fromMap(parameters);
	}

	public static ParameterTool buildGlobalJobParameters(Map<String,String> extraParameters)
	{
		Map<String, String> parameters = new HashMap<>(extraParameters);
		parameters.putAll(getDefaults());
		return ParameterTool.fromMap(parameters);
	}

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
