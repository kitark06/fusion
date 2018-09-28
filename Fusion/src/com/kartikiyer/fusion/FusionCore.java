package com.kartikiyer.fusion;


import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kartikiyer.fusion.map.DataEnrichmentMapper;
import com.kartikiyer.fusion.map.ElasticsearchActivityStatefulMapper;
import com.kartikiyer.fusion.model.BillingCost;
import com.kartikiyer.fusion.model.InsuranceDetails;
import com.kartikiyer.fusion.model.MedicineOrders;
import com.kartikiyer.fusion.model.Treatment;
import com.kartikiyer.fusion.util.CommonUtilityMethods;


/**
 * The FusionCore is the main class and heart of this project with the main objective being to efficiently and effectively join streams of data in real-time.
 * <p>
 * The main challenge in any streaming project is to handle problems like out-of-order and delayed message delivery.
 * At the same time, with disparate systems handling data influx, it is vital to know the precise moment when all data [spread across multiple schemas] have been inserted onto the storage system.
 * By keeping a track of documents inserted by various components/streams , it guarantees us data completeness & eliminates the need to fire any unnecessary queries to check if data is present.
 * <p>
 * If we have n streams, a brute force implementation would mean we fire n*n queries on the database & only when the n-th/last bit of data is inserted will the data be marked as complete and viable for join.
 * <br>
 * So out of the total n*(n-1) queries are fired, only (n-1) would actually bring the complete info once all the data is inserted. The rest (n-1)^2 are wasted.
 * <p>
 * This project addresses and provides the solution to this problem by performing stateful stream processing & delivering atleast-once semantics
 *
 * @author Kartik Iyer
 */
public class FusionCore
{
	Logger log = LoggerFactory.getLogger(FusionCore.class);

	/**
	 * The main method.
	 *
	 * @param args
	 *             the arguments
	 * @throws Exception
	 *              the exception
	 */
	public static void main(String[] args) throws Exception
	{
		FusionCore fusionCore = new FusionCore();
		fusionCore.startFusion(args);
	}


	/**
	 * Start fusion.
	 *
	 * @param jarfiles
	 *             the jarfiles
	 * @throws Exception
	 *              the exception
	 */
	private void startFusion(String... jarfiles) throws Exception
	{
		// List<String> kafkaTopics = Arrays.asList(new String[] { BILLING_COST_FUSION_STREAM, INSURANCE_DETAILS_FUSION_STREAM, MEDICINE_FUSION_STREAM, TREATMENT_FUSION_STREAM });

		List<String> kafkaTopics = new ArrayList<>();
		kafkaTopics.add(BILLING_COST_FUSION_STREAM + ACKNOWLEDGED_STREAM_SUFFIX);
		kafkaTopics.add(INSURANCE_DETAILS_FUSION_STREAM + ACKNOWLEDGED_STREAM_SUFFIX);
		kafkaTopics.add(MEDICINE_FUSION_STREAM + ACKNOWLEDGED_STREAM_SUFFIX);
		kafkaTopics.add(TREATMENT_FUSION_STREAM + ACKNOWLEDGED_STREAM_SUFFIX);


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

		env.getConfig().setGlobalJobParameters(CommonUtilityMethods.buildGlobalJobParameters(extraParameters));

		int parallelism = 1;
		DataStream<String> stream = env.addSource(flinkKafkaConsumer).setParallelism(parallelism);

		stream	.map(x -> new Tuple2<String, String>(x, x))
				.name("MapToPair")
				.keyBy(0)
				.map(new ElasticsearchActivityStatefulMapper())
				.name("ElasticsearchActivityStatefulMapper")
				.filter(queryablePcn -> queryablePcn.isPresent())
				.name("queryablePcnFilter")
				.map(new DataEnrichmentMapper())
				.name("DataEnrichmentMapper")
				.writeAsText("D:/workspace/output", WriteMode.OVERWRITE)
				.setParallelism(parallelism);

		env.setParallelism(parallelism);
		System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
