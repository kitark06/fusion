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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import com.kartikiyer.fusion.io.KafkaWriter;
import com.kartikiyer.fusion.map.ElasticSearchBulkIndexMapper;
import com.kartikiyer.fusion.util.CommonUtilityMethods;
import com.kartikiyer.fusion.util.ProjectFusionConstants;


public class StreamingCore
{
	Logger LOG = Logger.getLogger(StreamingCore.class);

	public static void main(String[] args)
	{
		StreamingCore streamingCore = new StreamingCore();

		String kafkaClusterIp = ProjectFusionConstants.KAFKA_CLUSTER_IP_PORT;
		String keySerializer = StringSerializer.class.getName();
		String valueSerializer = StringSerializer.class.getName();

		String directory = "inputFiles/";
		Map<String, String> filesToStream = new HashMap<>();
		filesToStream.put(directory + "BillingCost.txt", BILLING_COST_FUSION_STREAM);
		filesToStream.put(directory + "InsuranceDetails.txt", INSURANCE_DETAILS_FUSION_STREAM);
		filesToStream.put(directory + "MedicineOrders.txt", MEDICINE_FUSION_STREAM);
//		filesToStream.put(directory + "Patients.txt", PATIENTS_STREAM);
		filesToStream.put(directory + "Treatment.txt", TREATMENT_FUSION_STREAM);

		int dataNeeded=337;

		filesToStream
		.forEach((fileName, topicName) -> {
			new Thread(() -> {
				streamingCore
				// Dont generate PCN numbers for the patients data
				.streamFiletoKafkaTopic(fileName, topicName.equals(PATIENTS_STREAM) ? false : true, kafkaClusterIp, topicName + "Client", keySerializer, valueSerializer, topicName, dataNeeded);
			}).start();
		});


		Map<String, String> topicsToIndex = new HashMap<>();
//		topicsToIndex.put(PATIENTS_STREAM, PATIENT_INFO);
		topicsToIndex.put(BILLING_COST_FUSION_STREAM, BILLING_COST);
		topicsToIndex.put(INSURANCE_DETAILS_FUSION_STREAM, INSURANCE_DETAILS);
		topicsToIndex.put(MEDICINE_FUSION_STREAM, MEDICINE_ORDERS);
		topicsToIndex.put(TREATMENT_FUSION_STREAM, TREATMENT);

		topicsToIndex.forEach((topic, indexName) -> new Thread(() -> streamingCore.streamRecordsToES(topic, indexName, args)).start());
	}

	private void streamFiletoKafkaTopic(String inputFileLocation, boolean generatePrimaryKey, String kafkaClusterIp, String clientId, String keySerializer, String valueSerializer, String topicName, int dataNeeded)
	{
		try (KafkaWriter<String, String> writer = new KafkaWriter<>(kafkaClusterIp, clientId, keySerializer, valueSerializer, topicName);
			BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFileLocation));)
		{
			// get the CSV headers by calling readLine
			String line = bufferedReader.readLine();
			String[] headers = line.split(COLUMN_DELIMITER);

			int recordCount = 0;
			int errorRecords = 0;

			List<Integer> list = new ArrayList<>();
			for (int i = 0; i < dataNeeded; i++)
				list.add(i);
			Collections.shuffle(list);


			// for each line after the header, treating it as a separate entry.
			while ((line = bufferedReader.readLine()) != null && --dataNeeded >= 0)
			{
				String[] data = line.split(COLUMN_DELIMITER);

				if (data.length < headers.length)
					errorRecords++; // keeping track of malformed records & continue;
				else
				{
					StringBuilder json = new StringBuilder();
					json.append("{");

					if (generatePrimaryKey)
					{
						json	.append("\"pcn\"")
							.append(":")
							.append("\"" + list.get(dataNeeded) + "\"")
							.append(",");
					}

					for (int i = 0; i < headers.length; i++)
					{
						json	.append("\"" + headers[i] + "\"")
							.append(":")
							.append("\"" + data[i] + "\"")
							.append(",");
					}
					json.setLength(json.length() - 1);
					json.append("}");

					writer.writeMessage(json.toString());

					recordCount++;
					LOG.debug(json.toString());
				}
				LOG.debug(recordCount + " -- " + topicName);
			}
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void streamRecordsToES(String topic, String indexName, String... jarfiles)
	{
		List<String> topics = new ArrayList<>();
		topics.add(topic);


		FlinkKafkaConsumerBase<String> flinkKafkaConsumer = CommonUtilityMethods.getFlinkKakfaConsumer(topics, new SimpleStringSchema());

		// TODO
		// StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(FLINK_CLUSTER_IP, FLINK_CLUSTER_PORT, jarfiles);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		DataStream<String> stream = env.addSource(flinkKafkaConsumer);

		// TODO
		stream	.countWindowAll(ELASTICSEARCH_BULK_INSERT_WINDOW_COUNT)
				.apply(new ElasticSearchBulkIndexMapper(indexName))
				.name("ElasticSearchBulkIndexMapper");

		try
		{
			env	.getConfig()
				.setGlobalJobParameters(CommonUtilityMethods.buildGlobalJobParameters());
			env	.setParallelism(1)
				.execute();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
