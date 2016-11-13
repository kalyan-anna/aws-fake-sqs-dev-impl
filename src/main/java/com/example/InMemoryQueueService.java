package com.example;

import com.amazonaws.services.sqs.model.Message;
import static org.apache.commons.lang.StringUtils.*;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class InMemoryQueueService implements QueueService {

	private static int VISIBILITY_TIMEOUT_SEC = 30;

	private ConcurrentHashMap<String, ConcurrentLinkedDeque<String>> queues;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> suppressedMessages;
	private ConcurrentHashMap<String, ScheduledFuture<?>> handlerToscheduledTasksMap;

	private ScheduledExecutorService executorService;

	public InMemoryQueueService() {
		this(new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), Executors.newScheduledThreadPool(5));
	}

	InMemoryQueueService(ConcurrentHashMap<String, ConcurrentLinkedDeque<String>> queues,
			ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> suppressedMessages,
			ConcurrentHashMap<String, ScheduledFuture<?>> handlerToscheduledTasksMap,
			ScheduledExecutorService executorService) {
		this.queues = queues;
		this.suppressedMessages = suppressedMessages;
		this.executorService = executorService;
		this.handlerToscheduledTasksMap = handlerToscheduledTasksMap;
	}

	@Override
	public void push(String qUrl, String messageBody) {
		if(isBlank(qUrl) || isBlank(messageBody)) {
			throw new IllegalArgumentException("Invalid qName or messageBody");
		}
		String qName = fromQueueUrl(qUrl);

		if(queues.get(qName) == null) {
			queues.put(qName, new ConcurrentLinkedDeque<>());
		}
		queues.get(qName).add(messageBody);
	}

	@Override
	public Optional<Message> pull(String qUrl) {
		if(isBlank(qUrl)) {
			throw new IllegalArgumentException("Invalid qName or messageBody");
		}
		String qName = fromQueueUrl(qUrl);
		if(queues.get(qName) == null || queues.get(qName).isEmpty()) {
			return Optional.empty();
		}

		String body = queues.get(qName).poll();
		String uniqueId = UUID.randomUUID().toString();
		Message message = new Message().withMessageId(uniqueId).withBody(body).withReceiptHandle("RH-" + uniqueId);
		suppressMessage(qName, message);

		ScheduledFuture future = executorService.schedule(() -> {
			Message suppressedMessage = suppressedMessages.get(qName).get(message.getReceiptHandle());
			queues.get(qName).addFirst(suppressedMessage.getBody());
			suppressedMessages.get(qName).remove(message.getReceiptHandle());
			handlerToscheduledTasksMap.remove(message.getReceiptHandle());
		}, VISIBILITY_TIMEOUT_SEC, TimeUnit.SECONDS);
		handlerToscheduledTasksMap.put(message.getReceiptHandle(), future);

		return Optional.of(message);
	}

	private void suppressMessage(String qName, Message message) {
		if(suppressedMessages.get(qName) == null) {
			suppressedMessages.put(qName, new ConcurrentHashMap<>());
		}
		suppressedMessages.get(qName).put(message.getReceiptHandle(), message);
	}

	@Override
	public void delete(String qUrl, String receiptHandler) {
		if(isBlank(qUrl) || isBlank(receiptHandler)) {
			throw new IllegalArgumentException("Invalid qName or receiptHandler");
		}
		String qName = fromQueueUrl(qUrl);

		boolean success = handlerToscheduledTasksMap.get(receiptHandler).cancel(false);
		if(success) {
			suppressedMessages.get(qName).remove(receiptHandler);
			handlerToscheduledTasksMap.remove(receiptHandler);
		}
	}

	private String fromQueueUrl(String queueUrl) {
		return Paths.get(queueUrl).getFileName().toString();
	}
}
