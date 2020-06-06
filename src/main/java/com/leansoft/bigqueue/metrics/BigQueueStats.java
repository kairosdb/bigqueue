package com.leansoft.bigqueue.metrics;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface BigQueueStats
{
	LongCollector gcCount(@Key("name")String queueName);
}
