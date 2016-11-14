package com.example;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.util.concurrent.testing.TestingExecutors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(value = Parameterized.class)
public class QueueServiceVisibilityTimeoutTest {

	@Parameterized.Parameters
	public static Collection<String> getParameters() {
		return Arrays.asList("InMemoryQueueService");
	}

	private String queueServiceImplClass;
	private QueueService queueService;
	private String qUrlBase = "https://sqs.amazonaws.com/373529781950/";

	public QueueServiceVisibilityTimeoutTest(String queueServiceImplClass) {
		this.queueServiceImplClass = queueServiceImplClass;
	}

	@Before
	public void before() {
		if(queueServiceImplClass.equals("InMemoryQueueService")) {
			this.queueService = new InMemoryQueueService(new ConcurrentHashMap<>(), new ConcurrentHashMap<>(),
					new ConcurrentHashMap<>(), TestingExecutors.sameThreadScheduledExecutor());
		}
	}

	@Test
	public void testMessageAddedBackToQueueHead() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);
		Optional<Message> msg1 = queueService.pull(qUrlBase + qName);
		//Timeout
		Optional<Message> msg2 = queueService.pull(qUrlBase + qName);
		assertThat(msg2.orElse(null).getMessageId(), equalTo(msg1.orElse(null).getMessageId()));
		assertThat(msg2.orElse(null).getBody(), equalTo(inputBody));
	}

	@Test
	public void testMessageAddedBackToQueueHeadWithMultipleMessages() {
		String qName = "Test-Queue";
		String body1 = "Message Body 1";
		String body2 = "Message Body 2";
		queueService.push(qUrlBase + qName, body1);
		queueService.push(qUrlBase + qName, body2);

		Optional<Message> msg1_body1 = queueService.pull(qUrlBase + qName);
		Optional<Message> msg2_body2 = queueService.pull(qUrlBase + qName);

		Optional<Message> msg3_body2 = queueService.pull(qUrlBase + qName);
		Optional<Message> msg4_body1 = queueService.pull(qUrlBase + qName);

		assertThat(msg4_body1.orElse(null).getMessageId(), equalTo(msg1_body1.orElse(null).getMessageId()));
		assertThat(msg3_body2.orElse(null).getMessageId(), equalTo(msg2_body2.orElse(null).getMessageId()));
	}

	@Test
	public void testMessageIdAndReceiptHandlerWhenReceivingSameMessage() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);

		//timeout immediately after each pull
		Optional<Message> msg1 = queueService.pull(qUrlBase + qName);
		Optional<Message> msg2 = queueService.pull(qUrlBase + qName);
		Optional<Message> msg3 = queueService.pull(qUrlBase + qName);

		assertMessagesHaveUniqueReceiptHandler(msg1.orElse(null), msg2.orElse(null), msg3.orElse(null));
		assertMessagesHaveSameMessageId(msg1.orElse(null), msg2.orElse(null), msg3.orElse(null));
	}

	private void assertMessagesHaveUniqueReceiptHandler(Message... messages) {
		Set<String> receiptHandlers = Arrays.stream(messages).map(Message::getReceiptHandle).collect(Collectors.toSet());
		assertThat(receiptHandlers.size(), equalTo(messages.length));
	}

	private void assertMessagesHaveSameMessageId(Message... messages) {
		Set<String> messageIds = Arrays.stream(messages).map(Message::getMessageId).collect(Collectors.toSet());
		assertThat(messageIds.size(), equalTo(1));
	}
}
