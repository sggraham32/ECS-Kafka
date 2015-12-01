/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log;

import java.time.Instant;

import kafka.message.ByteBufferMessageSet;
import kafka.server.FetchDataInfo;

/**
 * Java version of the Kafka scala class that manages LogSegment files in Kafka.
 * Some parts copied from the original Kafka 0.9 version of the Scala code
 * 
 * Modified the code to make the persistence layer pluggable
 *
 */

public class LogSegment {
	private LogSegmentPersistence persister;
	private long rollJitterMs;
	private Instant created;
	
	public LogSegment(LogSegmentPersistence persister, long rollJitterMs){
		this(persister, rollJitterMs, Instant.now());
	}
	
	public LogSegment(LogSegmentPersistence persister, long rollJitterMs, Instant created){
		this.persister = persister;
		this.rollJitterMs = rollJitterMs;
		this.created = created;
	}
	
	public Instant getCreated(){
		return created;
	}


	/* Return the size in bytes of this log segment */
	public long size(){
		return persister.size();
	}
	
	 /**
	   * Append the given messages starting with the given offset. Add
	   * an entry to the index if needed.
	   * 
	   * It is assumed this method is being called from within a lock.
	   * 
	   * @param offset The first offset in the message set.
	   * @param messages The messages to append.
	   */
	//@nonthreadsafe
	public void append(long offset, ByteBufferMessageSet messages) {
		persister.append(offset, messages);
	}
	
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
	//@threadsafe
	public FetchDataInfo read(long startOffset, long maxOffset, int maxSize, long maxPosition){
		return persister.read(startOffset, maxOffset, maxSize, maxPosition);
	}

}