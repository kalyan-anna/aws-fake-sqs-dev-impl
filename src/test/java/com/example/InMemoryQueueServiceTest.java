package com.example;

import com.amazonaws.services.sqs.model.Message;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class InMemoryQueueServiceTest extends BaseTestClass {

	private QueueService queueService;
	private ConcurrentHashMap<String, ConcurrentLinkedDeque<Message>> messageStore;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> invisibleMessageStore;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, ScheduledFuture<?>>> scheduledTaskStore;

	private String qUrlBase = "https://sqs.amazonaws.com/373529781950/";

	@Before
	public void before() {
		messageStore = new ConcurrentHashMap<>();
		invisibleMessageStore = new ConcurrentHashMap<>();
		scheduledTaskStore = new ConcurrentHashMap<>();
		queueService = new InMemoryQueueService(messageStore, invisibleMessageStore,
				scheduledTaskStore, Executors.newScheduledThreadPool(3));
	}

	@Test
	public void push_shouldCreateQueue_whenDoesNotExists() {
		String qName = "Test-Queue";
		queueService.push(qUrlBase + qName, "Message Body");
		assertThat(messageStore.get(qName), is(notNullValue()));
	}

	@Test
	public void push_shouldAddMessageToQueue() {
		String qName = "Test-Queue";
		String inputMessageBody = "Message Body";
		queueService.push(qUrlBase + qName, inputMessageBody);
		assertThat(messageStore.get(qName).peek().getBody(), equalTo(inputMessageBody));
		assertThat(messageStore.get(qName).peek().getMessageId(), not(isEmptyOrNullString()));
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
		assertThat(message.orElse(null).getBody(), equalTo(firstMessage));
	}

	@Test
	public void pull_shouldReturnEmptyOptionalObject_whenQueueIsEmpty() {
		String qName = "Test-Queue";
		messageStore.put(qName, new ConcurrentLinkedDeque<>());
		Optional<Message> message = queueService.pull(qUrlBase + qName);
		assertThat(message.isPresent(), is(false));
	}

	@Test
	public void pull_shouldReturnEmptyOptionalObject_whenQueueIsNull() {
		String qName = "Test-Queue";
		Optional<Message> message = queueService.pull(qUrlBase + qName);
		assertThat(message.isPresent(), is(false));
	}

	@Test
	public void pull_shouldReturnValidMessageIdReceiptHandlerAndBody() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);

		Optional<Message> message = queueService.pull(qUrlBase + qName);

		assertThat(message.isPresent(), is(true));
		assertThat(message.orElse(null).getBody(), equalTo(inputBody));
		assertThat(message.orElse(null).getReceiptHandle(), not(isEmptyOrNullString()));
		assertThat(message.orElse(null).getMessageId(), not(isEmptyOrNullString()));
	}

	@Test
	public void pull_shouldMoveTheMessageFromQueueToSuppressed() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);

		Optional<Message> message = queueService.pull(qUrlBase + qName);

		assertThat(messageStore.get(qName).isEmpty(), is(true));
		Message invisibleMessage = invisibleMessageStore.get(qName).get(message.orElse(null).getMessageId());
		assertThat(invisibleMessage, notNullValue());
	}

	@Test
	public void pull_shouldScheduleVisibilityTimeoutTasks() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);

		Optional<Message> message = queueService.pull(qUrlBase + qName);
		ScheduledFuture scheduledFuture = scheduledTaskStore.get(qName).get(message.orElse(null).getMessageId());
		assertThat(scheduledFuture, notNullValue());
	}

	@Test
	public void delete_shouldRemoveMessageFromSuppressedListAndCancelScheduleTask() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);
		Optional<Message> message = queueService.pull(qUrlBase + qName);
		ScheduledFuture future = scheduledTaskStore.get(qName).get(message.orElse(null).getMessageId());

		queueService.delete(qUrlBase + qName, message.orElse(null).getReceiptHandle());

		assertThat(messageStore.get(qName).isEmpty(), is(true));
		assertThat(invisibleMessageStore.get(qName).isEmpty(), is(true));
		assertThat(scheduledTaskStore.get(message.orElse(null).getMessageId()), nullValue());
		assertThat(future.isCancelled(), is(true));
	}
}
