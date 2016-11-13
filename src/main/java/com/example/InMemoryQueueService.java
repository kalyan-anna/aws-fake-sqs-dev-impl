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

	private ConcurrentHashMap<String, ConcurrentLinkedDeque<Message>> queues;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> msgIdToSuppressedMsgQueue;
	private ConcurrentHashMap<String, String> handlerToMsgIdMap;
	private ConcurrentHashMap<String, ScheduledFuture<?>> msgIdToschedulerMap;

	private ScheduledExecutorService executorService;

	public InMemoryQueueService() {
		this(new ConcurrentHashMap<>(), new ConcurrentHashMap<>(),
						new ConcurrentHashMap<>(), Executors.newScheduledThreadPool(5));
	}

	InMemoryQueueService(ConcurrentHashMap<String, ConcurrentLinkedDeque<Message>> queues,
			ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> msgIdToSuppressedMsgQueue,
			ConcurrentHashMap<String, ScheduledFuture<?>> msgIdToschedulerMap,
			ScheduledExecutorService executorService) {
		this.queues = queues;
		this.msgIdToSuppressedMsgQueue = msgIdToSuppressedMsgQueue;
		this.executorService = executorService;
		this.msgIdToschedulerMap = msgIdToschedulerMap;
		this.handlerToMsgIdMap = new ConcurrentHashMap<>();
	}

	@Override
	public void push(String qUrl, String body) {
		if(isBlank(qUrl) || isBlank(body)) {
			throw new IllegalArgumentException("Invalid qName or messageBody");
		}
		String qName = fromQueueUrl(qUrl);

		if(queues.get(qName) == null) {
			queues.put(qName, new ConcurrentLinkedDeque<>());
		}
		Message newMessage = new Message()
								.withMessageId(UUID.randomUUID().toString())
								.withBody(body);
		queues.get(qName).add(newMessage);
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

		Message message = queues.get(qName).poll().clone();
		String receiptHandle = "RH-" + UUID.randomUUID().toString();
		message.setReceiptHandle(receiptHandle);
		suppressMessage(qName, message);
		handlerToMsgIdMap.put(message.getReceiptHandle(), message.getMessageId());

		ScheduledFuture future = executorService.schedule(() -> {
			msgIdToSuppressedMsgQueue.get(qName).remove(message.getMessageId());
			queues.get(qName).addFirst(message);
			msgIdToschedulerMap.remove(message.getMessageId());
		}, VISIBILITY_TIMEOUT_SEC, TimeUnit.SECONDS);
		msgIdToschedulerMap.put(message.getMessageId(), future);

		return Optional.of(message);
	}

	private void suppressMessage(String qName, Message message) {
		if(msgIdToSuppressedMsgQueue.get(qName) == null) {
			msgIdToSuppressedMsgQueue.put(qName, new ConcurrentHashMap<>());
		}
		msgIdToSuppressedMsgQueue.get(qName).put(message.getMessageId(), message);
	}

	@Override
	public void delete(String qUrl, String receiptHandler) {
		if(isBlank(qUrl) || isBlank(receiptHandler)) {
			throw new IllegalArgumentException("Invalid qName or receiptHandler");
		}
		String qName = fromQueueUrl(qUrl);

		String messageId = handlerToMsgIdMap.get(receiptHandler);
		msgIdToschedulerMap.get(messageId).cancel(false);
		msgIdToschedulerMap.remove(messageId);
		msgIdToSuppressedMsgQueue.get(qName).remove(messageId);
	}

	private String fromQueueUrl(String queueUrl) {
		return Paths.get(queueUrl).getFileName().toString();
	}
}
