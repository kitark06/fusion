package com.kartikiyer.fusion.io;


import org.apache.kafka.common.serialization.StringSerializer;

import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;


public class Loader
{

	public static void main(String[] args)
	{
		new Loader().loadMessages();
	}

	private void loadMessages()
	{

		String kafkaClusterIp = "localhost:9092";

		String keySerializer = StringSerializer.class.getName();
		String valueSerializer = StringSerializer.class.getName();

		String directory = "inputFiles/";
		streamFiletoKafkaTopic(directory + "BillingCost.txt", true, kafkaClusterIp, "BillingCostClient", keySerializer, valueSerializer, BILLING_COST_FUSION_STREAM);
		streamFiletoKafkaTopic(directory + "InsuranceDetails.txt", true, kafkaClusterIp, "InsuranceDetailsClient", keySerializer, valueSerializer, INSURANCE_DETAILS_FUSION_STREAM);
		streamFiletoKafkaTopic(directory + "MedicineOrders.txt", true, kafkaClusterIp, "MedicineOrdersClient", keySerializer, valueSerializer, MEDICINE_FUSION_STREAM);
		streamFiletoKafkaTopic(directory + "Patients.txt", false, kafkaClusterIp, "PatientsClient", keySerializer, valueSerializer, PATIENTS_STREAM);
		streamFiletoKafkaTopic(directory + "Treatment.txt", true, kafkaClusterIp, "TreatmentClient", keySerializer, valueSerializer, TREATMENT_FUSION_STREAM);
	}

	private void streamFiletoKafkaTopic(String inputFileLocation, boolean generatePCN, String kafkaClusterIp, String clientId, String keySerializer, String valueSerializer, String topicName)
	{
		new Thread(() ->
		{
			try (KafkaWriter<String, String> writer = new KafkaWriter<>(kafkaClusterIp, clientId, keySerializer, valueSerializer, topicName);
				BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFileLocation));)
			{
				// get the CSV headers by calling readLine
				String line = bufferedReader.readLine();
				String[] headers = line.split(COLUMN_DELIMITER);

				int errorRecords = 0;

				List<Integer> list = new ArrayList<>();
				int dataNeeded = 10;
				for (int i = 0; i < dataNeeded; i++)
					list.add(i);
				Collections.shuffle(list);


				// for each line after the header, treating it as a separate entry.
				while ((line = bufferedReader.readLine()) != null && --dataNeeded >= 0)
				{
					String[] data = line.split(COLUMN_DELIMITER);
					if (data.length < headers.length)
					{
						// keeping track of malformed records
						errorRecords++;
						continue;
					}

					StringBuilder json = new StringBuilder();

					json.append("{");

					if (generatePCN)
					{
						json.append("\"pcn\"");
						json.append(":");
						json.append("\"" + list.get(dataNeeded) + "\"");
						json.append(",");
					}

					for (int i = 0; i < headers.length; i++)
					{
						json.append("\"" + headers[i] + "\"");
						json.append(":");
						json.append("\"" + data[i] + "\"");
						json.append(",");
					}
					json.setLength(json.length() - 1);
					json.append("}");

					System.out.println(json);

					try
					{
						Thread.sleep(500);
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}

					writer.writeMessage(json.toString());
				}
				System.exit(0);
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}).start();
	}
}
