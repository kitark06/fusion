package com.kartikiyer.fusion;


import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kartikiyer.fusion.io.ElasticSearchOperations;
import com.kartikiyer.fusion.io.KafkaWriter;
import com.kartikiyer.fusion.map.AckEsInsertMapper;
import com.kartikiyer.fusion.map.ElasticSearchBulkIndexMapper;
import com.kartikiyer.fusion.trigger.CountTimeOutTrigger;
import com.kartikiyer.fusion.util.CommonUtilityMethods;
import com.kartikiyer.fusion.util.CsvToJson;
import com.kartikiyer.fusion.util.ProjectFusionConstants;


/**
 * StreamingCore is a supporting class with it being responsible for generating a steady stream of input from the input files. It has various methods for loading data into Kafka and also indexing the data in ElasticSearch.
 * The logic in {@link FusionCore} relies on this class to provide it with all the input needed.
 *
 * @author Kartik Iyer
 */
public class StreamingCore
{
	Logger log = LoggerFactory.getLogger(StreamingCore.class);

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args)
	{
		StreamingCore streamingCore = new StreamingCore();

		String kafkaClusterIp = ProjectFusionConstants.KAFKA_CLUSTER_IP_PORT;
		String keySerializer = StringSerializer.class.getName();
		String valueSerializer = StringSerializer.class.getName();

		streamingCore.loadLookUpTableIntoES("inputFiles/Patients.txt", PATIENT_INFO);
		streamingCore.filesToKafka(kafkaClusterIp, keySerializer, valueSerializer, 337);
		streamingCore.kafkaToElasticSearch(args);
	}

	/**
	 * A convenience method which lists a set of files and the Kafka topic names and triggers {@link StreamingCore#streamFiletoKafkaTopic}.
	 *
	 * @param kafkaClusterIp Ip address & Port separated by colon poinitng to the location of the kafka cluster
	 * @param keySerializer the serializer used for the message key
	 * @param valueSerializer the serializer used for the message value
	 * @param dataNeeded Number of data needed. The default input files have max 14,000 transactions.
	 */
	private void filesToKafka(String kafkaClusterIp, String keySerializer, String valueSerializer, int dataNeeded)
	{
		String directory = "inputFiles/";

		Map<String, String> filesToStream = new HashMap<>();
		filesToStream.put(directory + "BillingCost.txt", BILLING_COST_FUSION_STREAM);
		filesToStream.put(directory + "InsuranceDetails.txt", INSURANCE_DETAILS_FUSION_STREAM);
		filesToStream.put(directory + "MedicineOrders.txt", MEDICINE_FUSION_STREAM);
		filesToStream.put(directory + "Treatment.txt", TREATMENT_FUSION_STREAM);

		filesToStream.forEach((fileName, topicName) -> new Thread(
					() -> streamFiletoKafkaTopic(fileName, kafkaClusterIp, topicName + "Client", keySerializer, valueSerializer, topicName, dataNeeded)).start());
	}

	/**
	 * Reads the static input files and streams/inserts it to a kafka topic.
	 *
	 * @param inputFileLocation the input file location
	 * @param kafkaClusterIp Ip address & Port separated by colon poinitng to the location of the kafka cluster
	 * @param clientId The client id used by the Kafka Producer
	 * @param keySerializer the serializer used for the message key
	 * @param valueSerializer the serializer used for the message value
	 * @param topicName the topic name onto which records from the file will be pushed
	 * @param dataNeeded Number of transactions needed. The default input files have max 14,000 transactions.
	 */
	private void streamFiletoKafkaTopic(String inputFileLocation, String kafkaClusterIp, String clientId, String keySerializer, String valueSerializer, String topicName, int dataNeeded)
	{
		try (KafkaWriter<String, String> writer = new KafkaWriter<>(kafkaClusterIp, clientId, keySerializer, valueSerializer, topicName);
			BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFileLocation));)
		{
			// get the CSV headers by calling readLine
			String line = bufferedReader.readLine();

			String[] headers = line.split(COLUMN_DELIMITER);
			CsvToJson converter = new CsvToJson(headers);

			int recordCount = 0;
			int errorRecords = 0;

			List<Integer> list = new ArrayList<>();
			for (int i = 0; i < dataNeeded; i++)
				list.add(i);
			Collections.shuffle(list);


			// for each line after the header, treating it as a separate entry.
			while ((line = bufferedReader.readLine()) != null && --dataNeeded >= 0)
			{
				String json = converter.toJson(line, String.valueOf(list.get(dataNeeded)));

				if (json.equals(CsvToJson.MALFORMED_DATA))
					errorRecords++; // keeping track of malformed records & continue;
				else
				{
					writer.writeMessageSilently(json);
					log.debug(json);
					recordCount++;
				}
			}
			log.debug("Inserted [{}] records into topic [{}] while error records are [{}]", recordCount, topicName, errorRecords);
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * A convenience method which lists a set of Kafka topic names and ElasticSearch indexes and triggers {@link StreamingCore#streamRecordsToES}
	 *
	 * @param args the args
	 */
	private void kafkaToElasticSearch(String[] args)
	{
		Map<String, String> topicsToIndex = new HashMap<>();
		// topicsToIndex.put(PATIENTS_STREAM, PATIENT_INFO);
		topicsToIndex.put(BILLING_COST_FUSION_STREAM, BILLING_COST);
		topicsToIndex.put(INSURANCE_DETAILS_FUSION_STREAM, INSURANCE_DETAILS);
		topicsToIndex.put(MEDICINE_FUSION_STREAM, MEDICINE_ORDERS);
		topicsToIndex.put(TREATMENT_FUSION_STREAM, TREATMENT);

		topicsToIndex.forEach((topic, indexName) -> new Thread(() -> streamRecordsToES(topic, indexName, args)).start());
	}

	/**
	 * Reads the input kafka topic and indexes each message into the specified output ES index.
	 *
	 * @param topic The name of the input Kafka topic
	 * @param indexName The name of the output ElasticSearch Index
	 * @param jarfiles the jarfile location if launching the job on a remote cluster
	 */
	private void streamRecordsToES(String topic, String indexName, String... jarfiles)
	{
		List<String> topics = new ArrayList<>();
		topics.add(topic);

		FlinkKafkaConsumerBase<String> flinkKafkaConsumer = CommonUtilityMethods.getFlinkKakfaConsumer(topics, new SimpleStringSchema());

		// TODO
		// StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(FLINK_CLUSTER_IP, FLINK_CLUSTER_PORT, jarfiles);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(CommonUtilityMethods.buildGlobalJobParameters());


		DataStream<String> stream = env.addSource(flinkKafkaConsumer);

		stream	.timeWindowAll(Time.seconds(20))
				// build count trigger with time based trigger too
				.trigger(PurgingTrigger.of(new CountTimeOutTrigger<>(ELASTICSEARCH_BULK_INSERT_WINDOW_COUNT)))
				.apply(new ElasticSearchBulkIndexMapper(indexName))
				.name("ElasticSearchBulkIndexMapper")
				.map(new AckEsInsertMapper(topic))
				.name("AckEsInsertMapper");

		try
		{
			System.out.println(env.getExecutionPlan()); // {"nodes":[{"id":3,"type":"Source: Custom Source","pact":"Data Source","contents":"Source: Custom Source","parallelism":4},{"id":9,"type":"ElasticSearchBulkIndexMapper","pact":"Operator","contents":"ElasticSearchBulkIndexMapper","parallelism":1,"predecessors":[{"id":3,"ship_strategy":"HASH","side":"second"}]},{"id":11,"type":"AckEsInsertMapper","pact":"Operator","contents":"AckEsInsertMapper","parallelism":4,"predecessors":[{"id":9,"ship_strategy":"REBALANCE","side":"second"}]}]}
			env.execute();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Loads static look up tables into ES from files.
	 *
	 * @param inputFileLocation the input location of the file to be loaded
	 * @param indexName the name of teh ES index onto which records from the file will be inserted
	 */
	private void loadLookUpTableIntoES(String inputFileLocation, String indexName)
	{

		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFileLocation));
			ElasticSearchOperations esOps = new ElasticSearchOperations(new HttpHost[] { new HttpHost(ELASTIC_SEARCH_CLUSTER_IP, ELASTIC_SEARCH_CLUSTER_PORT) });)
		{

			String line = bufferedReader.readLine();
			String[] headers = line.split(COLUMN_DELIMITER);
			CsvToJson converter = new CsvToJson(headers);

			BulkRequest requests = new BulkRequest();

			while ((line = bufferedReader.readLine()) != null)
			{
				String json = converter.toJson(line);
				log.debug(json);

				requests.add(new IndexRequest(indexName, ES_DEFAULT_INDEX_TYPE).source(json, XContentType.JSON));

				if (requests.numberOfActions() > 10)
				{
					esOps.performBulkInsert(requests);
					requests = new BulkRequest();
				}
			}
			// flush leftover ES bulk
			esOps.performBulkInsert(requests);
		}
		catch (Exception e)
		{
			// TODO: handle exception
			e.printStackTrace();
		}
	}
}
