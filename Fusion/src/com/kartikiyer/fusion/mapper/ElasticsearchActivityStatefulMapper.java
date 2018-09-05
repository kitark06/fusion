package com.kartikiyer.fusion.mapper;


import java.util.Optional;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;


public class ElasticsearchActivityStatefulMapper extends RichMapFunction<Tuple2<String, String>, Optional<String>>
{
	private transient ReducingState<Integer>	sumState;
	int									totalStreamNumber;
	Optional<String>						queryablePcn	= Optional.empty();

	@Override
	public void open(Configuration parameters) throws Exception
	{
		super.open(parameters);
		this.totalStreamNumber = parameters.getInteger("fusionStreamCount", 0);
		
		ReducingStateDescriptor<Integer> aggregatingReducer = new ReducingStateDescriptor<>("countTracker", (x, y) -> x + y, IntSerializer.INSTANCE);
		sumState = getRuntimeContext().getReducingState(aggregatingReducer);
	}


	@Override
	public Optional<String> map(Tuple2<String, String> record) throws Exception
	{
		sumState.add(1);

		if (sumState.get() == totalStreamNumber)
			queryablePcn = Optional.of(record.f0); // here record has a key which is the PCN (field0) and value at field1. Hence record.f0 is emitted as the optional output of this stage when the criteria is met

		return queryablePcn;
	}
}

