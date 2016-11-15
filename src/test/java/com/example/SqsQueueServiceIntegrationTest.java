package com.example;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/*
This integration tests can be turned on if it is a separate project on its own
 */
@Ignore
public class SqsQueueServiceIntegrationTest extends BaseTestClass {

	private static AmazonSQSClient SQS;
	private SqsQueueService queueService;
	private String qUrl;

	@BeforeClass
	public static void beforeClass() {
		AWSCredentials credentials;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (~/.aws/credentials), and is in valid format.",
					e);
		}
		SQS = new AmazonSQSClient(credentials);
	}

	@Before
	public void before() {
		queueService = new SqsQueueService(SQS);
	}

	@After
	public void after() {
		if(qUrl != null) {
			System.out.println("deleting qUrl " + qUrl);
			SQS.deleteQueue(qUrl);
		}
	}

	@Test
	public void push_shouldSendMessageToQueue() {
		String qName = "IT-test-1-queue";
		String message = "Dummy Message";
		qUrl = SQS.createQueue(qName).getQueueUrl();

		queueService.push(qName, message);

		Optional<Message> messageInQueue = SQS.receiveMessage(qUrl).getMessages().stream().findFirst();
		assertThat(messageInQueue.isPresent(), is(true));
		assertThat(messageInQueue.orElse(null).getBody(), equalTo(message));
	}

	@Test
	public void pull_shouldReceiveMessageFromQueue() {
		String qName = "IT-test-2-queue";
		String expectedBody = "Dummy Message body";
		qUrl = SQS.createQueue(qName).getQueueUrl();
		queueService.push(qName, expectedBody);

		Optional<Message> message = queueService.pull(qName);

		assertThat(message.isPresent(), is(true));
		assertThat(message.orElse(null).getBody(), equalTo(expectedBody));
	}

	@Test
	public void delete_shouldDeleteMessageFromQueue() throws Exception {
		qUrl = SQS.createQueue("IT-test-3-queue--7").getQueueUrl();
		SQS.sendMessage(qUrl, "Message Body");

		Message message = queueService.pull(qUrl).orElse(null);
		queueService.delete(qUrl, message.getReceiptHandle());

		List<Message> messages = SQS.receiveMessage(qUrl).getMessages();
		assertThat(messages.size(), is(0));
	}
}