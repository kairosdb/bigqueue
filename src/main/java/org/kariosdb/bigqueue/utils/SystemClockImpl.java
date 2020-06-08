package org.kariosdb.bigqueue.utils;

public class SystemClockImpl implements Clock
{
	@Override
	public long getTime()
	{
		return System.currentTimeMillis();
	}
}
