package org.kairosdb.bigqueue.page;

import org.kairosdb.bigqueue.metrics.PageFactoryStats;
import org.kairosdb.bigqueue.utils.Clock;
import org.kairosdb.bigqueue.utils.FileFactory;
import org.kairosdb.metrics4j.MetricSourceManager;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 The purpose of this class is to wrap all delete calls and do a GC call only when
 all deletes have been completed.
 */
public class GCMappedPageFactoryImpl extends MappedPageFactoryImpl
{
	private static final PageFactoryStats stats = MetricSourceManager.getSource(PageFactoryStats.class);
	private final AtomicInteger m_deleteCounter = new AtomicInteger();
	private boolean m_fileDeleted = false;  //Identifies if a file was actually removed

	GCMappedPageFactoryImpl(int pageSize, String pageDir, long cacheTTL, Clock clock, FileFactory fileFactory)
	{
		super(pageSize, pageDir, cacheTTL, clock, fileFactory);
	}

	GCMappedPageFactoryImpl(int pageSize, String pageDir, long cacheTTL)
	{
		super(pageSize, pageDir, cacheTTL);
	}

	private void incrementUsage()
	{
		m_deleteCounter.incrementAndGet();
	}

	private void decrementUsage()
	{
		int count = m_deleteCounter.decrementAndGet();
		if (count == 0)
		{
			if (m_fileDeleted)
			{
				stats.forceGCCalls(pageFile).put(1);
				System.gc();
			}

			m_fileDeleted = false;
		}
	}

	@Override
	public void deleteAllPages() throws IOException
	{
		incrementUsage();
		super.deleteAllPages();
		decrementUsage();
	}

	@Override
	public void deletePages(Set<Long> indexes) throws IOException
	{
		incrementUsage();
		super.deletePages(indexes);
		decrementUsage();
	}

	@Override
	public boolean deletePage(long index) throws IOException
	{
		incrementUsage();
		boolean resp = super.deletePage(index);
		m_fileDeleted = resp;
		decrementUsage();
		return resp;
	}

	@Override
	public void deletePagesBefore(long timestamp) throws IOException
	{
		incrementUsage();
		super.deletePagesBefore(timestamp);
		decrementUsage();
	}

	@Override
	public void deletePagesBeforePageIndex(long pageIndex) throws IOException
	{
		incrementUsage();
		super.deletePagesBeforePageIndex(pageIndex);
		decrementUsage();
	}


}
