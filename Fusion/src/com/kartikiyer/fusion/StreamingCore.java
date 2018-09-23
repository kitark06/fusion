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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.http.HttpHost;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kartikiyer.fusion.io.ElasticSearchOperations;
import com.kartikiyer.fusion.io.KafkaWriter;
import com.kartikiyer.fusion.map.AckEsInsertMapper;
import com.kartikiyer.fusion.map.ElasticSearchBulkIndexMapper;
import com.kartikiyer.fusion.util.CommonUtilityMethods;
import com.kartikiyer.fusion.util.CsvToJson;
import com.kartikiyer.fusion.util.ProjectFusionConstants;


public class StreamingCore
{
	Logger log = LoggerFactory.getLogger(StreamingCore.class);

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

	private void kafkaToElasticSearch(String[] args)
	{
		Map<String, String> topicsToIndex = new HashMap<>();
		topicsToIndex.put(PATIENTS_STREAM, PATIENT_INFO);
		topicsToIndex.put(BILLING_COST_FUSION_STREAM, BILLING_COST);
		topicsToIndex.put(INSURANCE_DETAILS_FUSION_STREAM, INSURANCE_DETAILS);
		topicsToIndex.put(MEDICINE_FUSION_STREAM, MEDICINE_ORDERS);
		topicsToIndex.put(TREATMENT_FUSION_STREAM, TREATMENT);

		topicsToIndex.forEach((topic, indexName) -> new Thread(() -> streamRecordsToES(topic, indexName, args)).start());
	}

	private void streamRecordsToES(String topic, String indexName, String... jarfiles)
	{
		List<String> topics = new ArrayList<>();
		topics.add(topic);

		FlinkKafkaConsumerBase<String> flinkKafkaConsumer = CommonUtilityMethods.getFlinkKakfaConsumer(topics, new SimpleStringSchema());

		// TODO
		// StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(FLINK_CLUSTER_IP, FLINK_CLUSTER_PORT, jarfiles);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.getConfig().setGlobalJobParameters(CommonUtilityMethods.buildGlobalJobParameters());


		DataStream<String> stream = env.addSource(flinkKafkaConsumer);

		stream	.countWindowAll(ELASTICSEARCH_BULK_INSERT_WINDOW_COUNT)
				.apply(new ElasticSearchBulkIndexMapper(indexName))
				.name("ElasticSearchBulkIndexMapper")
				.map(new AckEsInsertMapper(topic))
				.name("AckEsInsertMapper");

		try
		{
			env.execute();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

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
