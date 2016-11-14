package com.example;

import com.amazonaws.services.sqs.model.Message;
import org.apache.commons.lang3.StringUtils;

import static org.apache.commons.lang3.StringUtils.*;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class InMemoryQueueService implements QueueService {

	private static final int VISIBILITY_TIMEOUT_SEC = 30;

	private ConcurrentHashMap<String, ConcurrentLinkedDeque<Message>> queues;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> msgIdToSuppressedMsgQueue;
	private ConcurrentHashMap<String, ScheduledFuture<?>> msgIdToSchedulerMap;

	private ScheduledExecutorService executorService;

	InMemoryQueueService() {
		this(new ConcurrentHashMap<>(), new ConcurrentHashMap<>(),
						new ConcurrentHashMap<>(), Executors.newScheduledThreadPool(5));
	}

	InMemoryQueueService(ConcurrentHashMap<String, ConcurrentLinkedDeque<Message>> queues,
			ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> msgIdToSuppressedMsgQueue,
			ConcurrentHashMap<String, ScheduledFuture<?>> msgIdToSchedulerMap,
			ScheduledExecutorService executorService) {
		this.queues = queues;
		this.msgIdToSuppressedMsgQueue = msgIdToSuppressedMsgQueue;
		this.executorService = executorService;
		this.msgIdToSchedulerMap = msgIdToSchedulerMap;
	}

	@Override
	public void push(String qUrl, String body) {
		if(isBlank(qUrl) || isBlank(body)) {
			throw new IllegalArgumentException("Invalid qName or messageBody");
		}
		String qName = fromQueueUrl(qUrl);
		queues.putIfAbsent(qName, new ConcurrentLinkedDeque<>());

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
		String receiptHandle = "MSG-ID-" + message.getMessageId() + "-RH-" + UUID.randomUUID().toString();
		message.setReceiptHandle(receiptHandle);
		suppressMessage(qName, message);

		ScheduledFuture future = executorService.schedule(() -> {
			msgIdToSuppressedMsgQueue.get(qName).remove(message.getMessageId());
			queues.get(qName).addFirst(message);
			msgIdToSchedulerMap.remove(message.getMessageId());
		}, VISIBILITY_TIMEOUT_SEC, TimeUnit.SECONDS);
		msgIdToSchedulerMap.put(message.getMessageId(), future);

		return Optional.of(message);
	}

	private void suppressMessage(String qName, Message message) {
		msgIdToSuppressedMsgQueue.putIfAbsent(qName,  new ConcurrentHashMap<>());
		msgIdToSuppressedMsgQueue.get(qName).put(message.getMessageId(), message);
	}

	@Override
	public void delete(String qUrl, String receiptHandler) {
		if(isBlank(qUrl) || isBlank(receiptHandler)) {
			throw new IllegalArgumentException("Invalid qName or receiptHandler");
		}
		String qName = fromQueueUrl(qUrl);

		String messageId = fromReceiptHandler(receiptHandler);
		msgIdToSchedulerMap.get(messageId).cancel(false);
		msgIdToSchedulerMap.remove(messageId);
		msgIdToSuppressedMsgQueue.get(qName).remove(messageId);
	}

	private String fromQueueUrl(String queueUrl) {
		return Paths.get(queueUrl).getFileName().toString();
	}

	private String fromReceiptHandler(String receiptHandler) {
		return StringUtils.substringBetween(receiptHandler, "MSG-ID-", "-RH-");
	}
}
