package com.kartikiyer.fusion.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class CountTimeOutTrigger<W extends Window> extends Trigger<Object,W>
{
	private final long maxCount;

	public CountTimeOutTrigger(long maxCount) {
		this.maxCount = maxCount;
	}

	private final ReducingStateDescriptor<Long> stateDesc =
			new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);



	@Override
	public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
		ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
		count.add(1L);
		if (count.get() >= maxCount) {
			count.clear();
			return TriggerResult.FIRE;
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
		return TriggerResult.FIRE;
	}

	@Override
	public void clear(W window, TriggerContext ctx) throws Exception {
		ctx.getPartitionedState(stateDesc).clear();
	}

	@Override
	public boolean canMerge() {
		return true;
	}

	@Override
	public void onMerge(W window, OnMergeContext ctx) throws Exception {
		ctx.mergePartitionedState(stateDesc);
	}

	@Override
	public String toString() {
		return "CountTrigger(" +  maxCount + ")";
	}

	private static class Sum implements ReduceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return value1 + value2;
		}

	}
}
