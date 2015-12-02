/**
 * 
 */
package kafka.log.s3;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import kafka.log.LogSegmentPersistence;
import kafka.message.ByteBufferBackedInputStream;
import kafka.message.ByteBufferMessageSet;
import kafka.server.FetchDataInfo;
import kafka.server.LogOffsetMetadata;

/**
 * An implementation of the pluggable log segment persistence mechanism, using AWS S3.
 * A bucket corresponds to the topic-partition.
 * Contents of the bucket are log segment objects (log segment files)
 * Writes (appends) must be serialized to a single broker that "owns" the partition -- concurrent appends to a segment object will corrupt it
 *
 */
public class S3LogSegment implements LogSegmentPersistence {
	private static final int MAX_SEGMENT_FILE_SIZE = 1024*1024;

	private static Region defaultRegion = Region.getRegion(Regions.US_EAST_1);
	
	private String segmentPath;   //corresponds to bucket name in s3
	private String segmentName;	  //corresponds to key name in S3
	
	private ByteBuffer contents = ByteBuffer.allocate(MAX_SEGMENT_FILE_SIZE);
	private ByteBufferMessageSet bbms = new ByteBufferMessageSet(contents);
	private long contentLength = 0;
	
	private AWSCredentials credentials;
	private AmazonS3 s3;
	private Region region;
	
	public S3LogSegment(String topicName, int partition, String segmentName, AWSCredentials credentials){
		this(topicName, partition, segmentName, credentials, defaultRegion);
	}
	
	public S3LogSegment(String topicName, int partition, String segmentName, AWSCredentials credentials, Region region){
		this.segmentPath = getBucketName(topicName, partition);
		this.segmentName = segmentName;
		this.credentials = credentials;
		this.region = region;
		
        s3 = new AmazonS3Client(credentials);
        s3.setRegion(region);
        
        assertBucket();
	}
	
	private String getBucketName(String topicName, int partition){
		return topicName + "-" + partition;
	}

	@Override
	public long size() {
		return contentLength;
	}

	@Override
	/*
	 * Keep an in memory ByteBuffer and use flush() to write it out to S3.  Make the appends to the in memory byte buffer
	 */
	public void append(long offset, ByteBufferMessageSet messages) {
		// TODO currently, offset param is ignored, change that!
		contents.put(messages.getBuffer());
		contentLength += messages.sizeInBytes();
	}

	@Override
	public FetchDataInfo read(long startOffset, long maxOffset, int maxSize, long maxPosition) {
		// TODO implement startOffset, maxOffset, maxSize and maxPosition, for now, just return the entire segment
		LogOffsetMetadata md = new LogOffsetMetadata(startOffset, maxPosition, maxSize); //bogus parms
		contents.rewind();
		FetchDataInfo ret = new FetchDataInfo(md, bbms);
		return ret;
	}
	
	@Override
	public void flush() {
        ObjectMetadata metadata = new ObjectMetadata();
       // metadata.setContentLength(contentLength);
        ByteBufferBackedInputStream bbbis = new ByteBufferBackedInputStream(contents);

        try {
            s3.putObject(new PutObjectRequest(segmentPath, segmentName, bbbis, metadata));
        } catch (AmazonServiceException ase) {
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Error Message: " + ace.getMessage());
        } finally {
            if (bbbis != null) {
            	try {
					bbbis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        }
	}

	private void assertBucket(){
		try {
            if(!(s3.doesBucketExist(segmentPath))){
            	s3.createBucket(new CreateBucketRequest(segmentPath));
            }
         } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
	}
}
