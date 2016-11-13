package com.example;

import com.amazonaws.services.sqs.model.Message;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class InMemoryQueueServiceTest {

	private QueueService queueService;
	private ConcurrentHashMap<String, ConcurrentLinkedDeque<Message>> queues;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> msgIdToSuppressedMsg;
	private ConcurrentHashMap<String, ScheduledFuture<?>> msgIdToschedulerMap;

	private String qUrlBase = "https://sqs.amazonaws.com/373529781950/";

	@Before
	public void before() {
		queues = new ConcurrentHashMap<>();
		msgIdToSuppressedMsg = new ConcurrentHashMap<>();
		msgIdToschedulerMap = new ConcurrentHashMap<>();
		queueService = new InMemoryQueueService(queues, msgIdToSuppressedMsg,
				msgIdToschedulerMap, Executors.newScheduledThreadPool(5));
	}

	@Test
	public void push_shouldCreateQueue_whenQueueAlreadyDoesnNotExists() {
		String qName = "Test-Queue";
		queueService.push(qUrlBase + qName, "Message Body");
		assertThat(queues.get(qName), is(notNullValue()));
	}

	@Test
	public void push_shouldAddMessageToQueue() {
		String qName = "Test-Queue";
		String inputMessageBody = "Message Body";
		queueService.push(qUrlBase + qName, inputMessageBody);
		assertThat(queues.get(qName), is(notNullValue()));
		assertThat(queues.get(qName).poll(), equalTo(inputMessageBody));
	}

	@Test(expected = IllegalArgumentException.class)
	public void push_shouldThrowException_whenQueueUrlIsInvalid() {
		queueService.push(null, "dummyMessage");
	}

	@Test(expected = IllegalArgumentException.class)
	public void push_shouldThrowException_whenMessageBodyIsInvalid() {
		queueService.push(qUrlBase + "qName", null);
	}

	@Test
	public void pull_shouldPollTheFirstMessageInQueue() {
		String qName = "Test-Queue";
		String firstMessage = "Message Body 1";
		String secondMessage = "Message Body 2";
		String thirdMessage = "Message Body 3";
		queueService.push(qUrlBase + qName, firstMessage);
		queueService.push(qUrlBase + qName, secondMessage);
		queueService.push(qUrlBase + qName, thirdMessage);

		Optional<Message> message = queueService.pull(qUrlBase + qName);
		assertThat(message.isPresent(), is(true));
		assertThat(message.get().getBody(), equalTo(firstMessage));
	}

	@Test
	public void pull_shouldReturnEmptyOptionalObject_whenQueueIsEmpty() {
		String qName = "Test-Queue";
		queues.put(qName, new ConcurrentLinkedDeque<>());
		Optional<Message> message = queueService.pull(qUrlBase + qName);
		assertThat(message.isPresent(), is(false));
	}

	@Test
	public void pull_shouldReturnEmptyOptionalObject_whenQueueIsNull() {
		String qName = "Test-Queue";
		Optional<Message> message = queueService.pull(qUrlBase + qName);
		assertThat(message.isPresent(), is(false));
	}

	@Test(expected = IllegalArgumentException.class)
	public void pull_shouldThrowException_whenQueueUrlIsInvalid() {
		queueService.pull(null);
	}

	@Test
	public void pull_shouldReturnValidMessageIdBodyAndReceiptHandler() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);

		Optional<Message> message = queueService.pull(qUrlBase + qName);

		assertThat(message.isPresent(), is(true));
		assertThat(message.get().getBody(), equalTo(inputBody));
		assertThat(message.get().getReceiptHandle(), notNullValue());
		assertThat(message.get().getMessageId(), notNullValue());
	}

	@Test
	public void pull_shouldRemoveTheMessageFromQueueAndMarkItSuppressed() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);

		Optional<Message> message = queueService.pull(qUrlBase + qName);

		assertThat(queues.get(qName).isEmpty(), is(true));
		assertThat(msgIdToSuppressedMsg.get(qName).get(message.get().getReceiptHandle()), notNullValue());
	}

	@Test
	public void pull_shouldScheduleVisibilityTimeoutTasks() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);

		Optional<Message> message = queueService.pull(qUrlBase + qName);
		assertThat(msgIdToschedulerMap.get(message.get().getReceiptHandle()), notNullValue());
	}

	@Test
	public void delete_shouldRemoveMessageFromSuppressedListAndCancelFutureTask() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);

		Optional<Message> message = queueService.pull(qUrlBase + qName);
		ScheduledFuture future = msgIdToschedulerMap.get(message.get().getReceiptHandle());
		queueService.delete(qUrlBase + qName, message.get().getReceiptHandle());

		assertThat(queues.get(qName).isEmpty(), is(true));
		assertThat(msgIdToSuppressedMsg.get(qName).isEmpty(), is(true));
		assertThat(msgIdToschedulerMap.get(message.get().getReceiptHandle()), nullValue());
		assertThat(future.isCancelled(), is(true));
	}

	@Test(expected = IllegalArgumentException.class)
	public void delete_shouldThrowException_whenQueueUrlIsInvalid() {
		queueService.delete(null, "receiptHandler");
	}

	@Test(expected = IllegalArgumentException.class)
	public void delete_shouldThrowException_whenReceiptHandlerIsInvalid() {
		queueService.delete("qUrl", null);
	}
}
