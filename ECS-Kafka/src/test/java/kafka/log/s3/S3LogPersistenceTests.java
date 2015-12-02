package kafka.log.s3;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

import kafka.log.s3.S3LogSegment;
import kafka.message.*;
import kafka.server.FetchDataInfo;
import scala.collection.Iterator;
import scala.collection.Seq;

public class S3LogPersistenceTests {

	private AWSCredentials getCredentials(){
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
        return credentials;
	}
	
	@Test
	public void creationTest() {
		AWSCredentials credentials = getCredentials();

        String topicName = "topic1";
        int partition = 0;
        String segmentName = "0000000000000000.log";
		S3LogSegment logSegment = new S3LogSegment(topicName, partition, segmentName, credentials);
		assertNotNull(logSegment);
	}

	@Test
	public void oneRoundTrip(){
		AWSCredentials credentials = getCredentials();

        String topicName = "topic1";
        int partition = 1;
        String segmentName = "0000000000000000.log";
		S3LogSegment logSegment = new S3LogSegment(topicName, partition, segmentName, credentials);
		
		String msg = "hello world";
		ByteBuffer b = ByteBuffer.wrap(msg.getBytes());
		Message m = new Message(msg.getBytes());

		ArrayList<Message> list = new ArrayList<Message>(Arrays.asList(m));
		Seq<Message> argumentsSeq = scala.collection.JavaConversions.asScalaBuffer(list).seq();
		ByteBufferMessageSet messages = new ByteBufferMessageSet(argumentsSeq);
		logSegment.append(0, messages);
		logSegment.flush();
		
		FetchDataInfo data = logSegment.read(0, 0, 0, 0); //get more sophisticated with the offsets
		Iterator<MessageAndOffset> readMessages = data.messageSet().iterator();
		while(readMessages.hasNext()){
			MessageAndOffset mno = readMessages.next();
			Message readMsg = mno.message();
			System.out.println(readMsg.payloadSize());
			System.out.println(readMsg.toString());
			String payload = new String(readMsg.payload().array());
			System.out.println(payload);
		}
	}
}
