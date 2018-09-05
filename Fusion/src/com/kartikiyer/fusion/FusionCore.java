package com.kartikiyer.fusion;


import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import com.kartikiyer.fusion.mapper.KeyByPcnMapper;
import com.kartikiyer.fusion.mapper.ElasticsearchActivityStatefulMapper;


public class FusionCore
{
	public static void main(String[] args) throws Exception
	{
		new FusionCore().doAwesomeStuff();
	}

	private void doAwesomeStuff() throws Exception
	{
		List<String> kafkaTopics = Arrays.asList(new String[] { "billingCost-fusionStream", "insuranceDetails-fusionStream", "medicine-fusionStream", "treatment-fusionStream" });

		Configuration conf = new Configuration();
		conf.setInteger("fusionStreamCount", kafkaTopics.size()); // assuming that ALL the topics subscribed are required for the final enriched data
		conf.setString("elasticSearchClusterIp", "localhost");
		conf.setInteger("elasticSearchClusterPort", 9200);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, conf, "D:/Workspace/mapreducejars/Fusion.jar");

		FlinkKafkaConsumerBase<String> flinkKafkaConsumer = new FlinkKafkaConsumer011<>(kafkaTopics, new SimpleStringSchema(), getKakfaConsumerProps()); // or use Pattern.compile("\\w+-fusionStream")
		flinkKafkaConsumer.setStartFromEarliest();

		DataStream<String> stream = env.addSource(flinkKafkaConsumer);
		stream.map(new KeyByPcnMapper()).keyBy(0).map(new ElasticsearchActivityStatefulMapper()).filter(queryablePcn -> queryablePcn.isPresent()).print();

		env.execute();
	}

	private Properties getKakfaConsumerProps()
	{
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "mixer");

		return properties;
	}
}
