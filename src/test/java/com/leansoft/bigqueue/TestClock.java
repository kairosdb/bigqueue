package com.leansoft.bigqueue;

import com.leansoft.bigqueue.utils.Clock;

/**
 Clock implementation that always increments the returned value
 */
public class TestClock implements Clock
{
	private long nextTime = 1L;

	@Override
	public long getTime()
	{
		return nextTime++;
	}

	public void setNextTime(long time)
	{
		nextTime = time;
	}

	public void advanceClock(long time)
	{
		nextTime += time;
	}
}
