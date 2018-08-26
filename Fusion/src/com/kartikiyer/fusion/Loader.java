package com.kartikiyer.fusion;


import com.kartikiyer.fusion.kafka.KafkaWriter;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.Random;


public class Loader
{

	public static final String COLUMN_DELIMITER = "\\t";

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
		streamFiletoKafkaTopic(directory + "BillingCost.txt", true, kafkaClusterIp, "BillingCostClient", keySerializer, valueSerializer, "billingCostTopic");
		streamFiletoKafkaTopic(directory + "InsuranceDetails.txt", true, kafkaClusterIp, "InsuranceDetailsClient", keySerializer, valueSerializer, "insuranceDetailsTopic");
		streamFiletoKafkaTopic(directory + "Medicines.txt", true, kafkaClusterIp, "MedicinesClient", keySerializer, valueSerializer, "medicinesTopic");
		streamFiletoKafkaTopic(directory + "Patients.txt", false, kafkaClusterIp, "PatientsClient", keySerializer, valueSerializer, "patientsTopic");
		streamFiletoKafkaTopic(directory + "Treatment.txt", true, kafkaClusterIp, "TreatmentClient", keySerializer, valueSerializer, "treatmentTopic");
	}

	private void streamFiletoKafkaTopic(String inputFileLocation, boolean generatePCN, String kafkaClusterIp, String clientId, String keySerializer, String valueSerializer, String topicName)
	{
		new Thread(() -> {
			try (
				KafkaWriter<String, String> writer = new KafkaWriter<>(kafkaClusterIp, clientId, keySerializer, valueSerializer, topicName);
				BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFileLocation));)
			{
				// get the CSV headers by calling readLine
				String line = bufferedReader.readLine();
				String[] headers = line.split(COLUMN_DELIMITER);

				int errorRecords = 0;

				// for each line after the header, treating it as a separate entry.
				while ((line = bufferedReader.readLine()) != null)
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
						Random ran = new Random();
						json.append("\"pcn\"");
						json.append(":");
						json.append("\"" + ran.nextInt(999) + "\"");
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

					// writer.writeMessage(json.toString());
				}
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}).start();
	}


}
