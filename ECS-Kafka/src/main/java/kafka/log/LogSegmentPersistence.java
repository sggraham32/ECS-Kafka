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

	
	public void append(long offset, ByteBufferMessageSet messages); 


	public FetchDataInfo read(long startOffset, long maxOffset, int maxSize, long maxPosition);
}
