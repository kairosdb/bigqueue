package org.kariosdb.bigqueue;

import java.io.Closeable;
import java.io.IOException;

/**
 * FanOut queue ADT
 * 
 * @author bulldog
 *
 */
public interface IFanOutQueue extends Closeable {
	
	/**
	 * Constant represents earliest timestamp
	 */
	long EARLIEST = -1;
	/**
	 * Constant represents latest timestamp
	 */
	long LATEST = -2;
	
	/**
	 * Determines whether a fan out queue is empty
	 * 
	 * @param fanoutId the fanout identifier
	 * @param useLatest if no offset has been recorded the head of the queue is used
	 * @return true if empty, false otherwise
	 * @throws IOException exception thrown if IO error occurs
	 */
	boolean isEmpty(String fanoutId, boolean useLatest) throws IOException;

	/**
	 * Determines whether a fan out queue is empty
	 *
	 * @param fanoutId the fanout identifier
	 * @return true if empty, false otherwise
	 * @throws IOException exception thrown if IO error occurs
	 */
	boolean isEmpty(String fanoutId) throws IOException;

	/**
	 * Determines whether the queue is empty
	 *
	 * @return true if empty, false otherwise
	 */
	boolean isEmpty();

	/**
	 * Adds an item at the back of the queue
	 *
	 * @param data to be enqueued data
	 * @return index where the item was appended
	 * @throws IOException exception throws if there is any IO error during enqueue operation.
	 */
	long enqueue(byte[] data)  throws IOException;

	/**
	 * Retrieves and removes the front of a fan out queue
	 *
	 * @param fanoutId the fanout identifier
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during dequeue operation.
	 */
	byte[] dequeue(String fanoutId) throws IOException;


	/**
	 * Retrieves and removes the front of a fan out queue
	 *
	 * @param fanoutId the fanout identifier
	 * @param useLatest if no offset has been recorded the head of the queue is used
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during dequeue operation.
	 */
	byte[] dequeue(String fanoutId, boolean useLatest) throws IOException;

	/**
	 * Peek the item at the front of a fanout queue, without removing it from the queue
	 *
	 * @param fanoutId the fanout identifier
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	byte[] peek(String fanoutId)  throws IOException;

	/**
	 * Peek the item at the front of a fanout queue, without removing it from the queue
	 *
	 * @param fanoutId the fanout identifier
	 * @param useLatest if no offset has been recorded the head of the queue is used
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	byte[] peek(String fanoutId, boolean useLatest)  throws IOException;


	/**
	 * Peek the length of the item at the front of a fan out queue
	 *
	 * @param fanoutId the fanout identifier
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	int peekLength(String fanoutId) throws IOException;

	/**
	 * Peek the length of the item at the front of a fan out queue
	 *
	 * @param fanoutId the fanout identifier
	 * @param useLatest if no offset has been recorded the head of the queue is used
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	int peekLength(String fanoutId, boolean useLatest) throws IOException;

	/**
	 * Peek the timestamp of the item at the front of a fan out queue
	 *
	 * @param fanoutId the fanout identifier
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	long peekTimestamp(String fanoutId) throws IOException;

	/**
	 * Peek the timestamp of the item at the front of a fan out queue
	 *
	 * @param fanoutId the fanout identifier
	 * @param useLatest if no offset has been recorded the head of the queue is used
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	long peekTimestamp(String fanoutId, boolean useLatest) throws IOException;

	/**
	 * Retrieves data item at the specific index of the queue
	 *
	 * @param index data item index
	 * @return data at index
	 * @throws IOException exception throws if there is any IO error during fetch operation.
	 */
	byte[] get(long index) throws IOException;


	/**
	 * Get length of data item at specific index of the queue
	 *
	 * @param index data item index
	 * @return length of data item
	 * @throws IOException exception throws if there is any IO error during fetch operation.
	 */
	int getLength(long index) throws IOException;

	/**
	 * Get timestamp of data item at specific index of the queue, this is the timestamp when corresponding item was appended into the queue.
	 *
	 * @param index data item index
	 * @return timestamp of data item
	 * @throws IOException exception throws if there is any IO error during fetch operation.
	 */
	long getTimestamp(long index) throws IOException;

	/**
	 * Total number of items remaining in the fan out queue
	 *
	 * @param fanoutId the fanout identifier
	 * @return total number
	 * @throws IOException exception thrown if IO error occurs
	 */
	long size(String fanoutId) throws IOException;

	/**
	 * Total number of items remaining in the fan out queue
	 *
	 * @param fanoutId the fanout identifier
	 * @param useLatest if no offset has been recorded the head of the queue is used
	 * @return total number
	 * @throws IOException exception thrown if IO error occurs
	 */
	long size(String fanoutId, boolean useLatest) throws IOException;


	/**
	 * Total number of items remaining in the queue.
	 *
	 * @return total number
	 */
	long size();

	/**
	 * Force to persist current state of the queue,
	 *
	 * normally, you don't need to flush explicitly since:
	 * 1.) FanOutQueue will automatically flush a cached page when it is replaced out,
	 * 2.) FanOutQueue uses memory mapped file technology internally, and the OS will flush the changes even your process crashes,
	 *
	 * call this periodically only if you need transactional reliability and you are aware of the cost to performance.
	 */
	void flush();

	/**
	 * Remove all data before specific timestamp, truncate back files and advance the queue front if necessary.
	 *
	 * @param timestamp a timestamp
	 * @throws IOException exception thrown if there was any IO error during the removal operation
	 */
	void removeBefore(long timestamp) throws IOException;

	/**
	 * Limit the back file size of this queue, truncate back files and advance the queue front if necessary.
	 *
	 * Note, this is a best effort call, exact size limit can't be guaranteed
	 *
	 * @param sizeLmit size limit
	 * @throws IOException exception thrown if there was any IO error during the operation
	 */
	void limitBackFileSize(long sizeLmit) throws IOException;

	/**
	 * Current total size of the back files of this queue
	 *
	 * @return total back file size
	 * @throws IOException exception thrown if there was any IO error during the operation
	 */
	long getBackFileSize() throws IOException;


    /**
     * Find an index closest to the specific timestamp when the corresponding item was enqueued.
     * to find latest index, use {@link #LATEST} as timestamp.
     * to find earliest index, use {@link #EARLIEST} as timestamp.
     *
     * @param timestamp when the corresponding item was appended
     * @return an index
     * @throws IOException exception thrown during the operation
     */
    long findClosestIndex(long timestamp) throws IOException;

    /**
     * Reset the front index of a fanout queue.
     *
     * @param fanoutId fanout identifier
     * @param index target index
     * @throws IOException exception thrown during the operation
     */
    void resetQueueFrontIndex(String fanoutId, long index) throws IOException;

	/**
	 * Removes all items of a queue, this will empty the queue and delete all back data files.
	 *
	 * @throws IOException exception throws if there is any IO error during dequeue operation.
	 */
	void removeAll() throws IOException;

	/**
	 * Get the queue front index, this is the earliest appended index
	 *
	 * @return an index
	 */
	long getFrontIndex();

	/**
	 * Get front index of specific fanout queue
	 *
	 * @param fanoutId fanout identifier
	 * @return an index
	 * @throws IOException exception thrown if IO error occurs
	 */
	long getFrontIndex(String fanoutId) throws IOException;

	/**
	 * Get front index of specific fanout queue
	 *
	 * @param fanoutId fanout identifier
	 * @param useLatest if no offset has been recorded the head of the queue is used
	 * @return an index
	 * @throws IOException exception thrown if IO error occurs
	 */
	long getFrontIndex(String fanoutId, boolean useLatest) throws IOException;
	
	/**
	 * Get the queue rear index, this is the next to be appended index
	 * 
	 * @return an index
	 */
	long getRearIndex();

}
