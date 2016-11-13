package com.example;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class FileQueueServiceTest {

	private QueueService queueService;
	private String qUrlBase = "https://sqs.amazonaws.com/373529781950/";

	@Before
	public void before() {
		this.queueService = new FileQueueService();
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
	public void push_shouldSetUpQueueDirectoryIfDoesNotExists() {
		queueService.push(qUrlBase + "test-queue", "test message");
		queueService.push(qUrlBase + "test-queue", "test message1");
		queueService.push(qUrlBase + "test-queue", "test message2");
		queueService.push(qUrlBase + "test-queue", "test message3");
		queueService.push(qUrlBase + "test-queue", "test message4");
		queueService.push(qUrlBase + "test-queue", "test message5");
	}

	@Test
	public void push_test() throws InterruptedException {
		Runnable task = () -> queueService.push(qUrlBase + "qUrl", "test message");
		ExecutorService service = Executors.newFixedThreadPool(5);
		IntStream.rangeClosed(1, 10).parallel().forEach(i -> {
			service.execute(task);
		});
		service.awaitTermination(10, TimeUnit.SECONDS);
	}
}
