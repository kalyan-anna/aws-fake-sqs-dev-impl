package com.example;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.util.concurrent.testing.TestingExecutors;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledFuture;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class InMemoryQueueServiceVisibilityTimeoutTest {

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
				msgIdToschedulerMap, TestingExecutors.sameThreadScheduledExecutor());
	}

	@Test
	public void pull_shouldAddMessageBackToQueueHead_whenVisbilityTimeout() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);

		Optional<Message> message = queueService.pull(qUrlBase + qName);
		assertThat(queues.get(qName).poll().getBody(), equalTo(inputBody));
	}

	@Test
	public void pull_shouldMaintainOrderForvisibilityTimeout_multipleMessageScenario() {
		String qName = "Test-Queue";
		String firstMessage = "Message Body 1";
		String secondMessage = "Message Body 2";
		String thirdMessage = "Message Body 3";
		queueService.push(qUrlBase + qName, firstMessage);
		queueService.push(qUrlBase + qName, secondMessage);
		queueService.push(qUrlBase + qName, thirdMessage);

		queueService.pull(qUrlBase + qName);
		queueService.pull(qUrlBase + qName);
		queueService.pull(qUrlBase + qName);

		assertThat(queues.get(qName).poll().getBody(), equalTo(firstMessage));
		assertThat(queues.get(qName).poll().getBody(), equalTo(secondMessage));
		assertThat(queues.get(qName).poll().getBody(), equalTo(thirdMessage));
	}

	@Test
	public void pull_shouldReceiveDifferentReceiptHandler_whenSameMessageReceivedMoreThanOnce() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		queueService.push(qUrlBase + qName, inputBody);

		Optional<Message> msg1 = queueService.pull(qUrlBase + qName);
		Optional<Message> msg2 = queueService.pull(qUrlBase + qName);
		Optional<Message> msg3 = queueService.pull(qUrlBase + qName);
		HashSet<String> receiptHandlers = new HashSet<>(Arrays.asList(msg1.get().getReceiptHandle(),
				msg2.get().getReceiptHandle(), msg3.get().getReceiptHandle()));
		assertThat(receiptHandlers.size(), equalTo(3));
	}
}
