package com.example;

import com.amazonaws.services.sqs.model.Message;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class InMemoryQueueServiceTest extends BaseTestClass {

	private QueueService queueService;
	private ConcurrentHashMap<String, DelayQueue<Record>> messageStore;
	private String qUrlBase = "https://sqs.amazonaws.com/373529781950/";

	@Before
	public void before() {
		messageStore = new ConcurrentHashMap<>();
		queueService = new InMemoryQueueService(messageStore);
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
		assertThat(messageStore.get(qName).peek().getMessage().getBody(), equalTo(inputMessageBody));
		assertThat(messageStore.get(qName).peek().getMessage().getMessageId(), not(isEmptyOrNullString()));
	}

	@Test
	public void pull_shouldReturnMessageInOrderOfInsertion() {
		String qName = "Test-Queue";
		String firstMessage = "Message Body 1";
		String secondMessage = "Message Body 2";
		String thirdMessage = "Message Body 3";
		queueService.push(qUrlBase + qName, firstMessage);
		queueService.push(qUrlBase + qName, secondMessage);

		Optional<Message> msg1 = queueService.pull(qUrlBase + qName);
		assertThat(msg1.isPresent(), is(true));
		assertThat(msg1.orElse(null).getBody(), equalTo(firstMessage));

		Optional<Message> msg2 = queueService.pull(qUrlBase + qName);
		assertThat(msg2.isPresent(), is(true));
		assertThat(msg2.orElse(null).getBody(), equalTo(secondMessage));

		queueService.push(qUrlBase + qName, thirdMessage);
		Optional<Message> msg3 = queueService.pull(qUrlBase + qName);
		assertThat(msg3.isPresent(), is(true));
		assertThat(msg3.orElse(null).getBody(), equalTo(thirdMessage));
	}

	@Test
	public void pull_shouldReturnEmptyOptionalObject_whenQueueIsEmpty() {
		String qName = "Test-Queue";
		messageStore.put(qName, new DelayQueue<>());
		Optional<Message> message = queueService.pull(qUrlBase + qName);
		assertThat(message.isPresent(), is(false));

		String body = "Message Body 1";
		queueService.push(qUrlBase + qName, body);
		queueService.pull(qUrlBase + qName);
		Optional<Message> msg2 = queueService.pull(qUrlBase + qName);
		assertThat(msg2.isPresent(), is(false));
	}

	@Test
	public void pull_shouldNotRemoveMessageFromQueueButShouldMakeItInvisible() {
		String qName = "Test-Queue";
		String body = "Message Body 1";
		queueService.push(qUrlBase + qName, body);

		Optional<Message> message = queueService.pull(qUrlBase + qName);

		assertThat(messageStore.get(qName).size(), is(1));
		assertThat(messageStore.get(qName).peek().getMessage(), equalTo(message.orElse(null)));
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
	public void delete_shouldRemoveTheMessage() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);
		Optional<Message> message = queueService.pull(qUrlBase + qName);

		queueService.delete(qUrlBase + qName, message.orElse(null).getReceiptHandle());
		assertThat(messageStore.get(qName).isEmpty(), is(true));
	}

	@Test
	public void pull_shouldReturnSameMessage_whenVisibilityTimeout() {
		String qName = "Test-Queue";
		String body1 = "Message Body 1";
		queueService.push(qUrlBase + qName, body1);
		queueService.pull(qUrlBase + qName);

		String body2 = "Message Body 1";
		queueService.push(qUrlBase + qName, body2);
		Optional<Message> msg1 = ((InMemoryQueueService)queueService).pull(qUrlBase + qName, 0);
		Optional<Message> msg2 = queueService.pull(qUrlBase + qName);

		assertThat(msg2.isPresent(), is(true));
		assertThat(msg2.orElse(null).getMessageId(), equalTo(msg1.orElse(null).getMessageId()));
		assertThat(msg2.orElse(null).getBody(), equalTo(msg1.orElse(null).getBody()));
		assertThat(msg2.orElse(null).getReceiptHandle(), not(equalTo(msg1.orElse(null).getReceiptHandle())));
	}
}
