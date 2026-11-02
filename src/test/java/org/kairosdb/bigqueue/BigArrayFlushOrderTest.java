package org.kairosdb.bigqueue;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import org.kairosdb.bigqueue.page.IMappedPage;
import org.kairosdb.bigqueue.page.IMappedPageFactory;

/**
 * flush() must force the data an index item points at before the head index that publishes it.
 *
 * <p>A crash between the two leaves entries that getHeadIndex() reports but whose content never
 * reached disk, and no removal API can drop them: removeBeforeIndex/removeBefore work from the
 * tail, and removeAll() discards everything including the intact entries.
 */
public class BigArrayFlushOrderTest {

	private String testDir = TestUtil.TEST_BASE_DIR + "bigarray/flushorder";
	private IBigArray bigArray;

	@Test
	public void flushForcesDataBeforeIndexBeforeMeta() throws IOException {
		BigArrayImpl array = new BigArrayImpl(testDir, "flush_order_test");
		bigArray = array;
		array.append("hello".getBytes());

		List<String> flushed = new ArrayList<String>();
		array.dataPageFactory = new RecordingPageFactory("data", flushed, array.dataPageFactory);
		array.indexPageFactory = new RecordingPageFactory("index", flushed, array.indexPageFactory);
		array.metaPageFactory = new RecordingPageFactory("meta", flushed, array.metaPageFactory);

		array.flush();

		assertEquals(Arrays.asList("data", "index", "meta"), flushed);
	}

	@After
	public void clean() throws IOException {
		if (bigArray != null) {
			bigArray.removeAll();
		}
	}

	/** Delegates everything, recording the order in which flush() reaches each factory. */
	private static class RecordingPageFactory implements IMappedPageFactory {

		private final String name;
		private final List<String> flushed;
		private final IMappedPageFactory delegate;

		RecordingPageFactory(String name, List<String> flushed, IMappedPageFactory delegate) {
			this.name = name;
			this.flushed = flushed;
			this.delegate = delegate;
		}

		public void flush() {
			flushed.add(name);
			delegate.flush();
		}

		public IMappedPage acquirePage(long index) throws IOException { return delegate.acquirePage(index); }
		public void releasePage(long index) { delegate.releasePage(index); }
		public int getPageSize() { return delegate.getPageSize(); }
		public String getPageDir() { return delegate.getPageDir(); }
		public boolean deletePage(long index) throws IOException { return delegate.deletePage(index); }
		public void deletePages(Set<Long> indexes) throws IOException { delegate.deletePages(indexes); }
		public void deleteAllPages() throws IOException { delegate.deleteAllPages(); }
		public void releaseCachedPages() throws IOException { delegate.releaseCachedPages(); }
		public Set<Long> getPageIndexSetBefore(long timestamp) { return delegate.getPageIndexSetBefore(timestamp); }
		public void deletePagesBefore(long timestamp) throws IOException { delegate.deletePagesBefore(timestamp); }
		public void deletePagesBeforePageIndex(long pageIndex) throws IOException { delegate.deletePagesBeforePageIndex(pageIndex); }
		public long getPageFileLastModifiedTime(long index) { return delegate.getPageFileLastModifiedTime(index); }
		public long getFirstPageIndexBefore(long timestamp) { return delegate.getFirstPageIndexBefore(timestamp); }
		public Set<Long> getExistingBackFileIndexSet() { return delegate.getExistingBackFileIndexSet(); }
		public int getCacheSize() { return delegate.getCacheSize(); }
		public Set<String> getBackPageFileSet() { return delegate.getBackPageFileSet(); }
		public long getBackPageFileSize() { return delegate.getBackPageFileSize(); }
	}
}
