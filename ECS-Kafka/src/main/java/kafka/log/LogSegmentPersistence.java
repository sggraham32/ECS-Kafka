/**
 * 
 */
package kafka.log;

import kafka.message.ByteBufferMessageSet;
import kafka.server.FetchDataInfo;

/**
 * The interface defining the pluggable persistence mechanism for Log Segments
 *
 */
public interface LogSegmentPersistence {
	//possible properties
	//startOffset: Long, indexIntervalBytes: Int, maxIndexSize: Int,
	//fileAlreadyExists: Boolean = false, initFileSize: Int = 0, preallocate: Boolean = false

   /**
	 * Return the size (in bytes) of whatever backing store mechanism is used for the LogSegment
	 */
	public long size(); 

   /**
	 * Append the given messages starting with the given offset. Add
	 * an entry to the index if needed.
	 * 
	 * It is assumed this method is being called from within a lock.
	 * 
	 * @param offset The first offset in the message set.
	 * @param messages The messages to append.
     */
	public void append(long offset, ByteBufferMessageSet messages); 

	/**
	  * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
	  * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
	  * 
	  * @param startOffset A lower bound on the first offset to include in the message set we read
	  * @param maxSize The maximum number of bytes to include in the message set we read
	  * @param maxOffset An optional maximum offset for the message set we read
	  * @param maxPosition An optional maximum position in the log segment that should be exposed for read.
	  * 
	  * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
	  *         or null if the startOffset is larger than the largest offset in this log
	  */
	public FetchDataInfo read(long startOffset, long maxOffset, int maxSize, long maxPosition);
	
	/**
	  * Flush this log segment to disk
	  */
	//@threadsafe
	public void flush();

}
