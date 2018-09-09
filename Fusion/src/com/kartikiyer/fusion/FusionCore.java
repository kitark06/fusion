package com.kartikiyer.fusion;


import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import com.kartikiyer.fusion.map.DataEnrichmentMapper;
import com.kartikiyer.fusion.map.ElasticsearchActivityStatefulMapper;
import com.kartikiyer.fusion.map.KeyByPcnMapper;
import com.kartikiyer.fusion.model.BillingCost;
import com.kartikiyer.fusion.model.InsuranceDetails;
import com.kartikiyer.fusion.model.MedicineOrders;
import com.kartikiyer.fusion.model.Treatment;
import com.kartikiyer.fusion.util.CommonUtilityMethods;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class FusionCore
{
	public static void main(String[] args) throws Exception
	{
		FusionCore fusionCore = new FusionCore();
		Logger	.getRootLogger()
				.setLevel(Level.DEBUG);

		fusionCore.startFusion(args);
	}


	private void startFusion(String... jarfiles) throws Exception
	{
		List<String> kafkaTopics = Arrays.asList(new String[] { BILLING_COST_FUSION_STREAM, INSURANCE_DETAILS_FUSION_STREAM, MEDICINE_FUSION_STREAM, TREATMENT_FUSION_STREAM });

		// Creating a KV pair of ElasticSearch IndexNname and the Class name to be used for deserialization of json got from querying ES
		// This will be used for dynamically adding the said data in the matching entity using reflection.
		// Building a String as objects cant be added to the Config object.

		StringBuilder dataEnrichmentStream = new StringBuilder();

		dataEnrichmentStream.append(TREATMENT + KEY_VALUE_DELIM + Treatment.class.getName())
						.append(ELEMENT_DELIM)
						.append(MEDICINE_ORDERS + KEY_VALUE_DELIM + MedicineOrders.class.getName())
						.append(ELEMENT_DELIM)
						.append(BILLING_COST + KEY_VALUE_DELIM + BillingCost.class.getName())
						.append(ELEMENT_DELIM)
						.append(INSURANCE_DETAILS + KEY_VALUE_DELIM + InsuranceDetails.class.getName());

		Map<String, String> extraParameters = new HashMap<>();
		extraParameters.put("dataEnrichmentStream", dataEnrichmentStream.toString());
		extraParameters.put("fusionStreamCount", String.valueOf(kafkaTopics.size()));

		FlinkKafkaConsumerBase<String> flinkKafkaConsumer = CommonUtilityMethods.getFlinkKakfaConsumer(kafkaTopics, new SimpleStringSchema());

		// StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(FLINK_CLUSTER_IP, FLINK_CLUSTER_PORT, jarfiles);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		env	.getConfig()
			.setGlobalJobParameters(CommonUtilityMethods.buildGlobalJobParameters(extraParameters));

		DataStream<String> stream = env.addSource(flinkKafkaConsumer);

		stream	.map(new KeyByPcnMapper())
				.name("KeyByPcnMapper")
				.keyBy(0)
				.map(new ElasticsearchActivityStatefulMapper())
				.name("ElasticsearchActivityStatefulMapper")
				.filter(queryablePcn -> queryablePcn.isPresent())
				.map(new MapFunction<Optional<String>, Optional<String>>()
				{
					@Override
					public Optional<String> map(Optional<String> queryablePcn) throws Exception
					{
						System.out.println(queryablePcn.get());
						return queryablePcn;
					}
				})
				.map(new DataEnrichmentMapper())
				.writeAsText("/root/home/output", WriteMode.OVERWRITE)
				.name("DataEnrichmentMapper");

		env.setParallelism(1).execute();
	}


}
