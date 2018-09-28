package com.kartikiyer.fusion.trigger;


import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;


/**
 * The CountTimeOutTrigger is registered as a trigger class to a TimeWindow.
 * It provides a default ProcessingTime based trigger which will fire the window after the set amount of Time units as registered with the TimeWindow
 *
 * <p>
 * The main purpose however is to act as a CountWindow which will fire once a set count of elements have been inserted into the window.
 * The count is as a parameter during the Construction of this Trigger. Once created, it uses a reducing state descriptor to keep tack and aggregate the count of elements.
 * It is wrapped with the PurgingTrigger to convert its TriggerResult.FIRE to TriggerResult.FIRE_AND_PURGE
 *
 * @author Kartik Iyer
 * @param <W>
 *             the generic type
 */
public class CountTimeOutTrigger<W extends Window> extends Trigger<Object, W>
{
	private static final long	serialVersionUID	= 13727810396559287L;
	private final long			maxCount;

	/**
	 * Instantiates a new CountTimeOutTrigger which is registered as a trigger class to a TimeWindow.
	 * It provides a default ProcessingTime based trigger which will fire the window after the set amount of Time units as registered with the TimeWindow
	 *
	 * <p>
	 * The main purpose however is to act as a CountWindow which will fire once a set count of elements have been inserted into the window.
	 * The count is as a parameter during the Construction of this Trigger. Once created, it uses a reducing state descriptor to keep tack and aggregate the count of elements.
	 * It is wrapped with the PurgingTrigger to convert its TriggerResult.FIRE to TriggerResult.FIRE_AND_PURGE
	 *
	 * @param maxCount
	 *             the max count
	 *
	 * @author Kartik Iyer
	 */
	public CountTimeOutTrigger(long maxCount)
	{
		this.maxCount = maxCount;
	}

	private final ReducingStateDescriptor<Long> stateDesc = new ReducingStateDescriptor<>("count", (Long value1, Long value2) -> value1 + value2, LongSerializer.INSTANCE);


	/* (non-Javadoc)
	 * @see org.apache.flink.streaming.api.windowing.triggers.Trigger#onElement(java.lang.Object, long, org.apache.flink.streaming.api.windowing.windows.Window, org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext)
	 */
	@Override
	public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception
	{
		ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
		count.add(1L);
		if (count.get() >= maxCount)
		{
			count.clear();
			return TriggerResult.FIRE;
		}
		return TriggerResult.CONTINUE;
	}

	/* (non-Javadoc)
	 * @see org.apache.flink.streaming.api.windowing.triggers.Trigger#onEventTime(long, org.apache.flink.streaming.api.windowing.windows.Window, org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext)
	 */
	@Override
	public TriggerResult onEventTime(long time, W window, TriggerContext ctx)
	{
		return TriggerResult.CONTINUE;
	}

	/* (non-Javadoc)
	 * @see org.apache.flink.streaming.api.windowing.triggers.Trigger#onProcessingTime(long, org.apache.flink.streaming.api.windowing.windows.Window, org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext)
	 */
	@Override
	public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception
	{
		return TriggerResult.FIRE;
	}

	/* (non-Javadoc)
	 * @see org.apache.flink.streaming.api.windowing.triggers.Trigger#clear(org.apache.flink.streaming.api.windowing.windows.Window, org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext)
	 */
	@Override
	public void clear(W window, TriggerContext ctx) throws Exception
	{
		ctx.getPartitionedState(stateDesc).clear();
	}

	/* (non-Javadoc)
	 * @see org.apache.flink.streaming.api.windowing.triggers.Trigger#canMerge()
	 */
	@Override
	public boolean canMerge()
	{
		return true;
	}

	/* (non-Javadoc)
	 * @see org.apache.flink.streaming.api.windowing.triggers.Trigger#onMerge(org.apache.flink.streaming.api.windowing.windows.Window, org.apache.flink.streaming.api.windowing.triggers.Trigger.OnMergeContext)
	 */
	@Override
	public void onMerge(W window, OnMergeContext ctx) throws Exception
	{
		ctx.mergePartitionedState(stateDesc);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return "CountTimeOutTrigger(" + maxCount + ")";
	}
}
