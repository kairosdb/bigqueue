package org.kariosdb.bigqueue.metrics;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;


public interface BigArrayStats
{
	LongCollector appendData(@Key("name") String arrayName);

	LongCollector getData(@Key("name") String arrayName);
}
