package com.example;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.util.concurrent.testing.TestingExecutors;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledFuture;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class InMemoryQueueServiceVisibilityTimeoutTest {

	private QueueService queueService;
	private ConcurrentHashMap<String, ConcurrentLinkedDeque<String>> queues;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> suppressedMessages;
	private ConcurrentHashMap<String, ScheduledFuture<?>> handlerToscheduledTasksMap;

	private String qUrlBase = "https://sqs.amazonaws.com/373529781950/";

	@Before
	public void before() {
		queues = new ConcurrentHashMap<>();
		suppressedMessages = new ConcurrentHashMap<>();
		handlerToscheduledTasksMap = new ConcurrentHashMap<>();
		queueService = new InMemoryQueueService(queues, suppressedMessages,
				handlerToscheduledTasksMap, TestingExecutors.sameThreadScheduledExecutor());
	}

	@Test
	public void pull_shouldAddMessageBackToQueueHead_whenVisbilityTimeout() {
		String qName = "Test-Queue";
		String inputBody = "Message Body 1";
		ConcurrentLinkedDeque<String> testQueue = new ConcurrentLinkedDeque<>(Arrays.asList(inputBody));
		queues.put(qName, testQueue);

		Optional<Message> message = queueService.pull(qUrlBase + qName);
		assertThat(testQueue.poll(), equalTo(inputBody));
	}

	@Test
	public void pull_shouldMaintainOrderForvisibilityTimeout_multipleMessageScenario() {
		String qName = "Test-Queue";
		String firstMessage = "Message Body 1";
		String secondMessage = "Message Body 2";
		String thirdMessage = "Message Body 3";
		ConcurrentLinkedDeque<String> testQueue =
				new ConcurrentLinkedDeque<String>(Arrays.asList(firstMessage, secondMessage, thirdMessage));
		queues.put(qName, testQueue);

		queueService.pull(qUrlBase + qName);
		queueService.pull(qUrlBase + qName);
		queueService.pull(qUrlBase + qName);

		assertThat(queues.get(qName).poll(), equalTo(firstMessage));
		assertThat(queues.get(qName).poll(), equalTo(secondMessage));
		assertThat(queues.get(qName).poll(), equalTo(thirdMessage));
	}
}
