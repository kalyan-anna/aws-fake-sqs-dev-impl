package com.example;

import com.amazonaws.services.sqs.model.Message;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * These tests are like integration tests and common for all implementation. So placed them in separate test class
 */
@RunWith(value = Parameterized.class)
public class QueueServiceTest extends BaseTestClass {

	@Parameterized.Parameters
	public static Collection<String> getParameters() {
		return Arrays.asList("InMemoryQueueService", "FileQueueService");
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
	public void before() throws Exception {
		if(queueServiceImplClass.equals("InMemoryQueueService")) {
			this.queueService = new InMemoryQueueService();
		}
		if(queueServiceImplClass.equals("FileQueueService")) {
			this.queueService = new FileQueueService(new UniversalUniqueIdGenerator());
			FileQueueServiceTest.deleteAllSubDirectories(Paths.get(FileQueueServiceTest.BASE_PATH));
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

	@Test
	public void testWithMultipleThreads() throws InterruptedException {
		Set<Message> messages = new HashSet<>();
		Runnable sqsTask = () -> {
			String qName = "test-queue";
			String body = "Test Message body";
			queueService.push(qUrlBase + qName, body + RandomStringUtils.randomAlphabetic(10));
			Optional<Message> message = queueService.pull(qUrlBase + qName);
			messages.add(message.orElse(null));
			queueService.delete(qUrlBase + qName, message.orElse(null).getReceiptHandle());
		};
		ExecutorService service = Executors.newFixedThreadPool(3);
		IntStream.rangeClosed(1, 10).parallel().forEach(i -> service.execute(sqsTask));
		service.shutdown();
		boolean completed = service.awaitTermination(10, TimeUnit.SECONDS);
		if(!completed) {
			System.out.println("Threads couldn't complete");
			return;
		}
		assertThat(messages.size(), equalTo(10));
		Set<String> messageIds = messages.stream().map(Message::getMessageId).collect(Collectors.toSet());
		assertThat(messageIds.size(), equalTo(10));
		Set<String> handlers = messages.stream().map(Message::getReceiptHandle).collect(Collectors.toSet());
		assertThat(handlers.size(), equalTo(10));
	}
}
