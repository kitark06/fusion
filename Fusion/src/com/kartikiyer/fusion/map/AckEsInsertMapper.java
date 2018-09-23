package com.kartikiyer.fusion.map;


import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;

import com.kartikiyer.fusion.io.KafkaWriter;


public class AckEsInsertMapper extends RichMapFunction<BulkItemResponse, String>
{
	private static final Logger	log	= Logger.getLogger(AckEsInsertMapper.class);
	KafkaWriter<String, String>	writer;
	private String				topic;


	public AckEsInsertMapper(String topic)
	{
		this.topic = topic;
	}

	@Override
	public void open(Configuration parameters) throws Exception
	{
		super.open(parameters);
		writer = new KafkaWriter<>(KAFKA_CLUSTER_IP_PORT, "AckMapperWriter_" + topic, StringSerializer.class.getName(), StringSerializer.class.getName(), topic + ACKNOWLEDGED_STREAM_SUFFIX);
	}

	@Override
	public String map(BulkItemResponse value) throws Exception
	{
		writer.writeMessageSilently(value.getResponse().getId());
		return null;
	}
}
