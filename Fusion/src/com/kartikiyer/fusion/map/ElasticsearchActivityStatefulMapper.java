package com.kartikiyer.fusion.map;


import java.util.Optional;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class ElasticsearchActivityStatefulMapper extends RichMapFunction<Tuple2<String, String>, Optional<String>>
{
	Logger								LOG	= LoggerFactory.getLogger(ElasticsearchActivityStatefulMapper.class);

	int									totalStreamNumber;
	private transient ReducingState<Integer>	sumState;
	private transient Optional<String>			queryablePcn;

	@Override
	public void open(Configuration config) throws Exception
	{
		super.open(config);

		queryablePcn = Optional.empty();
		ParameterTool parameters = (ParameterTool) getRuntimeContext()	.getExecutionConfig()
															.getGlobalJobParameters();
		this.totalStreamNumber = parameters.getInt("fusionStreamCount", 0);

		ReducingStateDescriptor<Integer> aggregatingReducer = new ReducingStateDescriptor<>("countTracker", (x, y) -> x + y, IntSerializer.INSTANCE);
		sumState = getRuntimeContext().getReducingState(aggregatingReducer);
	}


	@Override
	public Optional<String> map(Tuple2<String, String> record) throws Exception
	{
		sumState.add(1);
		LOG.debug("key [{}] -- sumState [{}] ",record.f0,sumState.get());
		if (sumState.get() == totalStreamNumber)
			queryablePcn = Optional.of(record.f0); // here record has a key which is the PCN (field0) and value at field1. Hence record.f0 is emitted as the optional output of this stage when the criteria is met

		return queryablePcn;
	}
}

