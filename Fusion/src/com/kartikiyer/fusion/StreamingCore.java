package com.kartikiyer.fusion;


import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.ws.Response;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.http.HttpHost;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.kartikiyer.fusion.map.ElasticSearchBulkIndexMapper;
import com.kartikiyer.fusion.util.CommonUtilityMethods;
import com.kartikiyer.fusion.util.ProjectFusionConstants;


public class StreamingCore
{
	public static void main(String[] args)
	{
		Logger	.getRootLogger()
				.setLevel(Level.DEBUG);

		StreamingCore streamingCore = new StreamingCore();

		Map<String, String> topicsToIndex = new HashMap<>();
		topicsToIndex.put(PATIENTS_STREAM, PATIENT_INFO);
		topicsToIndex.put(BILLING_COST_FUSION_STREAM, BILLING_COST);
		topicsToIndex.put(INSURANCE_DETAILS_FUSION_STREAM, INSURANCE_DETAILS);
		topicsToIndex.put(MEDICINE_FUSION_STREAM, MEDICINE_ORDERS);
		topicsToIndex.put(TREATMENT_FUSION_STREAM, TREATMENT);

		String clusterIp = ELASTIC_SEARCH_CLUSTER_IP;
		int clusterPort = ELASTIC_SEARCH_CLUSTER_PORT;

		topicsToIndex.forEach((topic, indexName) -> new Thread(() -> streamingCore.streamRecordsToES(topic, indexName, args)).start());
	}

	private void streamRecordsToES(String topic, String indexName, String... jarfiles)
	{
		List<String> topics = new ArrayList<>();
		topics.add(topic);


		FlinkKafkaConsumerBase<String> flinkKafkaConsumer = CommonUtilityMethods.getFlinkKakfaConsumer(topics, new SimpleStringSchema());

//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(FLINK_CLUSTER_IP, FLINK_CLUSTER_PORT, jarfiles);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		DataStream<String> stream = env.addSource(flinkKafkaConsumer);

		// TODO
		stream	/*.countWindowAll(ELASTICSEARCH_BULK_INSERT_WINDOW_COUNT)*/
				.countWindowAll(2)
				.apply(new ElasticSearchBulkIndexMapper(indexName)).name("ElasticSearchBulkIndexMapper");
//				.name("StreamingCore");

		try
		{
			env	.getConfig()
				.setGlobalJobParameters(CommonUtilityMethods.buildGlobalJobParameters());
			env.setParallelism(1).execute();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
