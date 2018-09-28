package com.kartikiyer.fusion.map;


import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;

import com.kartikiyer.fusion.io.KafkaWriter;


/**
 * This class is a RichMapFunction implementation which works on a single BulkItemResponse it gets from {@link com.kartikiyer.fusion.map.ElasticSearchBulkIndexMapper ElasticSearchBulkIndexMapper} ,
 * representing the result of a single operation of a bulk request.
 *
 * <p> For each BulkItemResponse, the map operation gets the ES docID of the doc which was indexed and writes it to a Kafka Topic.
 * Due to the wait_until nature of {@link com.kartikiyer.fusion.io.ElasticSearchOperations ElasticSearchOperations} ,
 * we can be sure that all the documents received by this mapper are Inserted, Indexed & Refreshed. (i.e. they can be searched immediately)
 *
 * <p> This mapper writes all the docIDs to a separate Kafka Topic whose name is
 * <p>outputTopic + {@link com.kartikiyer.fusion.util.ProjectFusionConstants#ACKNOWLEDGED_STREAM_SUFFIX ACKNOWLEDGED_STREAM_SUFFIX}<p>
 *  Where
 *  <br>outputTopic is the original topic from which the message was picked up and indexed into ES by {@link com.kartikiyer.fusion.map.ElasticSearchBulkIndexMapper ElasticSearchBulkIndexMapper}.
 *
 * @author Kartik Iyer
 */
public class AckEsInsertMapper extends RichMapFunction<BulkItemResponse, String>
{
	private static final long serialVersionUID = -1209326988358767540L;
	
	private static final Logger	log	= Logger.getLogger(AckEsInsertMapper.class);
	KafkaWriter<String, String>	writer;
	private String				outputTopic;



	/**
	 * Instantiates a new AckEsInsertMapper with an outputTopic. The Final topic onto which the docIDs of all the records inserted is written to is
	 * outputTopic + {@link com.kartikiyer.fusion.util.ProjectFusionConstants#ACKNOWLEDGED_STREAM_SUFFIX ACKNOWLEDGED_STREAM_SUFFIX}<p>
	 * 
	 * <p>NOTE : 
	 * <br>Please read the JavaDoc of {@link com.kartikiyer.fusion.map.AckEsInsertMapper this} class to find more info.
	 *
	 * @param outputTopic the topic
	 */
	public AckEsInsertMapper(String outputTopic)
	{
		this.outputTopic = outputTopic;
	}

	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.AbstractRichFunction#open(org.apache.flink.configuration.Configuration)
	 */
	@Override
	public void open(Configuration parameters) throws Exception
	{
		super.open(parameters);
		writer = new KafkaWriter<>(KAFKA_CLUSTER_IP_PORT, "AckMapperWriter_" + outputTopic, StringSerializer.class.getName(), StringSerializer.class.getName(), outputTopic + ACKNOWLEDGED_STREAM_SUFFIX);
	}

	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.RichMapFunction#map(java.lang.Object)
	 */
	@Override
	public String map(BulkItemResponse value)
	{
		writer.writeMessageSilently(value.getResponse().getId());
		return null;
	}
}
