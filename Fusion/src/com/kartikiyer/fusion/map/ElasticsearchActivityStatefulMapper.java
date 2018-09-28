package com.kartikiyer.fusion.map;


import java.util.Optional;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ElasticsearchActivityStatefulMapper is responsible for keeping track of inserts done on the ES by {@link ElasticSearchBulkIndexMapper}.
 * It accomplishes this by processing a DataStream created by subscribing to a list of topics created by {@link AckEsInsertMapper} on which primarykey is written for
 * each record indexed to any of the ES indexes part of the join operation.
 *
 * <p>
 * It maintains a ReducingState of Integer to keep track of the number of inserts done for EACH primary key on the all the ES indexes which will be used to perform the join operation.
 * The map method will emit an {@code Optional<String>} with value equal to the primaryKey of the document being indexed only when the document with this primaryKey is indexed
 * in ALL indexes which will be queried in the next step of the pipeline by the {@link DataEnrichmentMapper}.
 *
 * <p>
 * This makes sure that the project can handle out-of-oder and delayed streams and still be sure of when data from different parts of the pipeline insert documents on ES,
 * thus efficiently querying the database only when we are guaranteed to get all the data for performing the join.
 *
 * @author Kartik Iyer
 */
public class ElasticsearchActivityStatefulMapper extends RichMapFunction<Tuple2<String, String>, Optional<String>>
{
	private static final long				serialVersionUID	= -998043378187887250L;

	private static final Logger				log				= LoggerFactory.getLogger(ElasticsearchActivityStatefulMapper.class);

	int									totalStreamNumber;
	private transient ReducingState<Integer>	sumState;
	private transient Optional<String>			queryablePcn;

	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.AbstractRichFunction#open(org.apache.flink.configuration.Configuration)
	 */
	@Override
	public void open(Configuration config) throws Exception
	{
		super.open(config);

		queryablePcn = Optional.empty();
		ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		this.totalStreamNumber = parameters.getInt("fusionStreamCount");

		ReducingStateDescriptor<Integer> aggregatingReducer = new ReducingStateDescriptor<>("countTracker", (x, y) -> x + y, IntSerializer.INSTANCE);
		sumState = getRuntimeContext().getReducingState(aggregatingReducer);
	}


	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.RichMapFunction#map(java.lang.Object)
	 */
	@Override
	public Optional<String> map(Tuple2<String, String> record) throws Exception
	{
		sumState.add(1);
		log.debug("key [{}] -- sumState [{}] ", record.f0, sumState.get());
		if (sumState.get() == totalStreamNumber)
			queryablePcn = Optional.of(record.f0); // here record has a key which is the PCN (field0) and value at field1. Hence record.f0 is emitted as the optional output of this stage when the criteria is met

		return queryablePcn;
	}
}

