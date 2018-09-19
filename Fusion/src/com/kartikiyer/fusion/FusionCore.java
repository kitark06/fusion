package com.kartikiyer.fusion;


import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kartikiyer.fusion.map.DataEnrichmentMapper2;
import com.kartikiyer.fusion.map.ElasticsearchActivityStatefulMapper;
import com.kartikiyer.fusion.map.KeyByPcnMapper;
import com.kartikiyer.fusion.model.BillingCost;
import com.kartikiyer.fusion.model.InsuranceDetails;
import com.kartikiyer.fusion.model.MedicineOrders;
import com.kartikiyer.fusion.model.Treatment;
import com.kartikiyer.fusion.util.CommonUtilityMethods;


public class FusionCore
{
	Logger LOG = LoggerFactory.getLogger(FusionCore.class);

	public static void main(String[] args) throws Exception
	{
		FusionCore fusionCore = new FusionCore();
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

		int parallelism = 1;
		DataStream<String> stream = env	.addSource(flinkKafkaConsumer)
									.setParallelism(parallelism);

		stream	.map(new KeyByPcnMapper())
				// parse and map input jsons create tuple2 of [jsonDocId, Value] which gets keyed by the docID  
				.keyBy(0)
				.map(new ElasticsearchActivityStatefulMapper())
				.filter(queryablePcn -> queryablePcn.isPresent())

				// done so as to parallelize the subsequent countWindow operation
				// using Math.abs to keep the resulting integer positive as String.hashcode can return negative values
				.keyBy(queryablePcn -> Math.abs(queryablePcn.get().hashCode() % parallelism))
				.countWindow(FUSION_CORE_WINDOW_COUNT)
				.apply(new DataEnrichmentMapper2())

//				map(new DataEnrichmentMapper())

				.writeAsText("D:/workspace/output", WriteMode.OVERWRITE)
				.setParallelism(parallelism);

		env	.setParallelism(parallelism)
			.execute();
	}


}
