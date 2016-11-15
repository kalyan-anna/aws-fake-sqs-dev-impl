package com.example;

import com.amazonaws.services.sqs.model.Message;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@RunWith(value = Parameterized.class)
public class QueueServiceTest extends BaseTestClass {

	@Parameterized.Parameters
	public static Collection<String> getParameters() {
		return Arrays.asList("InMemoryQueueService");
	}

	private String queueServiceImplClass;
	private QueueService queueService;

	private String qName = "Test-Queue";
	private String qUrlBase = "https://sqs.amazonaws.com/373529781950/";
	private String qUrl = qUrlBase + qName;

	public QueueServiceTest(String queueServiceImplClass) {
		this.queueServiceImplClass = queueServiceImplClass;
	}

	@Before
	public void before() {
		if(queueServiceImplClass.equals("InMemoryQueueService")) {
			this.queueService = new InMemoryQueueService();
		}
	}

	@Test
	public void testSingleMessage() {
		String inputBody = "Test message body";

		queueService.push(qUrl, inputBody);

		Optional<Message> message = queueService.pull(qUrl);
		assertThat(message.isPresent(), is(true));
		assertThat(message.orElse(null).getBody(), equalTo(inputBody));

		queueService.delete(qUrl, message.orElse(null).getReceiptHandle());

		Optional<Message> msg2ndAttempt = queueService.pull(qUrl);
		assertThat(msg2ndAttempt.isPresent(), is(false));
	}

	@Test
	public void testMultipleMessage() {
		String body1 = "Test message body 1";
		queueService.push(qUrl, body1);

		String body2 = "Test message body 2";
		queueService.push(qUrl, body2);

		Optional<Message> msg1 = queueService.pull(qUrl);
		assertThat(msg1.isPresent(), is(true));
		assertThat(msg1.orElse(null).getBody(), equalTo(body1));

		String body3 = "Test Message body 3";
		queueService.push(qUrl, body3);

		Optional<Message> msg2 = queueService.pull(qUrl);
		assertThat(msg2.isPresent(), is(true));
		assertThat(msg2.orElse(null).getBody(), equalTo(body2));

		queueService.delete(qUrl, msg1.orElse(null).getReceiptHandle());

		Optional<Message> msg3 = queueService.pull(qUrl);
		assertThat(msg3.isPresent(), is(true));
		assertThat(msg3.orElse(null).getBody(), equalTo(body3));

		Optional<Message> msg4 = queueService.pull(qUrl);
		assertThat(msg4.isPresent(), is(false));

		queueService.delete(qUrl, msg2.orElse(null).getReceiptHandle());
		queueService.delete(qUrl, msg3.orElse(null).getReceiptHandle());
	}

	@Test
	public void testMultipleQueueAndMessage() {
		String qUrl1=qUrlBase + "Test-Queue-1", qUrl2=qUrlBase + "Test-Queue-2";
		String body1="body 1", body2="body 2", body3="body 3";

		queueService.push(qUrl1, body1);
		queueService.push(qUrl2, body2);

		Optional<Message> msg1 = queueService.pull(qUrl2);
		assertThat(msg1.orElse(null).getBody(), equalTo(body2));

		Optional<Message> msg2 = queueService.pull(qUrl1);
		assertThat(msg2.orElse(null).getBody(), equalTo(body1));

		queueService.delete(qUrl2, msg1.orElse(null).getReceiptHandle());
		queueService.push(qUrl2, body3);

		Optional<Message> msg3 = queueService.pull(qUrl1);
		assertThat(msg3.isPresent(), is(false));

		queueService.delete(qUrl1, msg2.orElse(null).getReceiptHandle());

		Optional<Message> msg4 = queueService.pull(qUrl2);
		assertThat(msg4.orElse(null).getBody(), equalTo(body3));

		queueService.delete(qUrl2, msg4.orElse(null).getReceiptHandle());
	}

	@Ignore
	@Test
	public void testWithMultipleThreads() throws InterruptedException {
		Runnable push = () -> queueService.push(qUrlBase + "test-queue", "test message");
		ExecutorService service = Executors.newFixedThreadPool(2);
		IntStream.rangeClosed(1, 10).parallel().forEach(i -> service.execute(push));
		service.awaitTermination(10, TimeUnit.SECONDS);
	}
}
