package org.kairosdb.bigqueue.page;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Test;

import org.kairosdb.bigqueue.TestUtil;
import org.kairosdb.bigqueue.utils.FileUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class MappedPageTest {
	
	private IMappedPageFactory mappedPageFactory;
	private String testDir = TestUtil.TEST_BASE_DIR + "bigqueue/unit/mapped_page_test";

	@Test
	public void testSingleThread() throws IOException {
		int pageSize = 1024 * 1024 * 32;
		mappedPageFactory = new MappedPageFactoryImpl(pageSize, testDir + "/test_single_thread", 2 * 1000);
		
		IMappedPage mappedPage = this.mappedPageFactory.acquirePage(0);
		assertThat(mappedPage).isNotNull();
		
		ByteBuffer buffer = mappedPage.getLocal(0);
		assertThat(buffer.limit()).isEqualTo(pageSize);
		assertThat(buffer.position()).isEqualTo(0);
		
		
		for(int i = 0; i < 10000; i++) {
			String hello = "hello world";
			int length = hello.getBytes().length;
			mappedPage.getLocal(i * 20).put(hello.getBytes());
			assertThat(mappedPage.getLocal(i * 20 , length)).isEqualTo(hello.getBytes());
		}
		
		buffer = ByteBuffer.allocateDirect(16);
		buffer.putInt(1);
		buffer.putInt(2);
		buffer.putLong(3L);
		for(int i = 0; i < 10000; i++) {
			buffer.flip();
			mappedPage.getLocal(i * 20).put(buffer);
		}
		for(int i = 0; i < 10000; i++) {
			ByteBuffer buf = mappedPage.getLocal(i * 20);
			assertThat(buf.getInt()).isEqualTo(1);
			assertThat(buf.getInt()).isEqualTo(2);
			assertThat(buf.getLong()).isEqualTo(3L);
		}
	}

	@Test
	public void testSingleThreadMultiBuffer() throws IOException {
		int pageSize = 1024 * 1024 * 32;
		mappedPageFactory = new MappedPageFactoryImpl(pageSize, testDir + "/test_single_thread", 2 * 1000);

		IMappedPage mappedPage1 = this.mappedPageFactory.acquirePage(0);
		assertThat(mappedPage1).isNotNull();

		ByteBuffer buffer = mappedPage1.getLocal(0);
		assertThat(buffer.limit()).isEqualTo(pageSize);
		assertThat(buffer.position()).isEqualTo(0);

		IMappedPage mappedPage2 = this.mappedPageFactory.acquirePage(1);
		assertThat(mappedPage2).isNotNull();

		ByteBuffer buffer2 = mappedPage2.getLocal(0);
		assertThat(buffer2.limit()).isEqualTo(pageSize);
		assertThat(buffer2.position()).isEqualTo(0);

		String hello1 = "hello world 1";
		String hello2 = "hello world 2";
		int length1 = hello1.getBytes().length;
		int length2 = hello2.getBytes().length;

		//Write to both buffers
		for(int i = 0; i < 10000; i++)
		{
			mappedPage1.getLocal(i * 20).put(hello1.getBytes());
			mappedPage2.getLocal(i * 20).put(hello2.getBytes());
		}

		//Read data from both to make sure they are right
		for (int i = 0; i < 10000; i++)
		{
			assertThat(mappedPage1.getLocal(i * 20 , length1)).isEqualTo(hello1.getBytes());
			assertThat(mappedPage2.getLocal(i * 20 , length2)).isEqualTo(hello2.getBytes());
		}

	}

	@Test
	public void testMultiThreads() {
		int pageSize = 1024 * 1024 * 32;
		mappedPageFactory = new MappedPageFactoryImpl(pageSize, testDir + "/test_multi_threads", 2 * 1000);
		
		int threadNum = 100;
		int pageNumLimit = 50;
		
		Set<IMappedPage> pageSet = Collections.newSetFromMap(new ConcurrentHashMap<IMappedPage, Boolean>());
		List<ByteBuffer> localBufferList = Collections.synchronizedList(new ArrayList<ByteBuffer>());
		
		Worker[] workers = new Worker[threadNum];
		for(int i = 0; i < threadNum; i++) {
			workers[i] = new Worker(i, mappedPageFactory, pageNumLimit, pageSet, localBufferList);
		}
		for(int i = 0; i < threadNum; i++) {
			workers[i].start();
		}
		
		for(int i = 0; i< threadNum; i++) {
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				// ignore
			}
		}
		
		assertThat(localBufferList.size()).isEqualTo(threadNum * pageNumLimit);
		assertThat(pageSet.size()).isEqualTo(pageNumLimit);
		
		// verify thread locality
		for(int i = 0; i < localBufferList.size(); i++) {
			for(int j = i + 1; j < localBufferList.size(); j++) {
				if (localBufferList.get(i) == localBufferList.get(j)) {
					fail("thread local buffer is not thread local");
				}
			}
		}
	}
	
	private static class Worker extends Thread {
		private final int id;
		private final int pageNumLimit;
		private final IMappedPageFactory pageFactory;
		private final Set<IMappedPage> sharedPageSet;
		private final List<ByteBuffer> localBufferList;
		
		public Worker(int id, IMappedPageFactory pageFactory, int pageNumLimit, 
				Set<IMappedPage> sharedPageSet, List<ByteBuffer> localBufferList) {
			this.id = id;
			this.pageFactory = pageFactory;
			this.sharedPageSet = sharedPageSet;
			this.localBufferList = localBufferList;
			this.pageNumLimit = pageNumLimit;
			
		}
		
		public void run() {
			for(int i = 0; i < pageNumLimit; i++) {
				try {
					IMappedPage page = this.pageFactory.acquirePage(i);
					sharedPageSet.add(page);
					localBufferList.add(page.getLocal(0));
					
					int startPosition = this.id * 2048;

					for(int j = 0; j < 100; j++) {
						String helloj = "hello world " + j;
						int length = helloj.getBytes().length;
						page.getLocal(startPosition + j * 20).put(helloj.getBytes());

						assertThat(page.getLocal(startPosition + j * 20, length)).isEqualTo(helloj.getBytes());

					}
					
					ByteBuffer buffer = ByteBuffer.allocateDirect(16);
					buffer.putInt(1);
					buffer.putInt(2);
					buffer.putLong(3L);
					for(int j = 0; j < 100; j++) {
						buffer.flip();
						page.getLocal(startPosition + j * 20).put(buffer);
					}
					for(int j = 0; j < 100; j++) {
						ByteBuffer buf = page.getLocal(startPosition + j * 20);
						assertThat(buf.getInt()).isEqualTo(1);
						assertThat(buf.getInt()).isEqualTo(2);
						assertThat(buf.getLong()).isEqualTo(3L);
					}
					
				} catch (IOException e) {
					fail("Got IOException when acquiring page " + i);
				}
			}
		}
		
	}
	
	@After
	public void clear() throws IOException {
		if (this.mappedPageFactory != null) {
			this.mappedPageFactory.deleteAllPages();
		}
		FileUtil.deleteDirectory(new File(testDir));
	}

}
