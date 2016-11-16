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

/**
 * Each queue maintains 3 concurrent HashMap stores and queueName is the parent-key for all of them
 *		- messageStore holds the incoming messages
 *		- invisibleMessageStore holds the messages that are pulled and waiting to be deleted
 *	    - scheduledTaskStore holds the visibility timeout scheduled tasks
 *
 * Push
 * When a message is pushed a unique messageId is generated and the message is added to messageStore
 *
 * Pull
 * The first message in the queue is pulled and added to invisibleMessageStore. invisibleMessageStore uses queueName as parent-key
 * and messageId as key for quick retrieval.
 * A unique receiptHandler is generated for each pulled message and a scheduled task is submitted to check for visibility timeout.
 *
 * Delete
 * When a delete request is received, the message is removed from invisibleMessageStore and associated scheduled task
 * for visibility timeout is cancelled.
 *
 * I thought of using DelayQueue but that would have affected concurrency. I'll try to create another branch with DelayQueue impl
 */
class InMemoryQueueService implements QueueService {

	private static final int VISIBILITY_TIMEOUT = Integer.valueOf(System.getProperty("visibility.timeout.sec"));
	private static final int POOL_SIZE = Integer.valueOf(System.getProperty("scheduled.task.thread.pool.size"));

	private ConcurrentHashMap<String, ConcurrentLinkedDeque<Message>> messageStore;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> invisibleMessageStore;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, ScheduledFuture<?>>> scheduledTaskStore;

	private ScheduledExecutorService executorService;

	InMemoryQueueService() {
		this(new ConcurrentHashMap<>(), new ConcurrentHashMap<>(),
						new ConcurrentHashMap<>(), Executors.newScheduledThreadPool(POOL_SIZE));
	}

	InMemoryQueueService(ConcurrentHashMap<String, ConcurrentLinkedDeque<Message>> messageStore,
			ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> invisibleMessageStore,
			ConcurrentHashMap<String, ConcurrentHashMap<String, ScheduledFuture<?>>> scheduledTaskStore,
			ScheduledExecutorService executorService) {
		this.messageStore = messageStore;
		this.invisibleMessageStore = invisibleMessageStore;
		this.executorService = executorService;
		this.scheduledTaskStore = scheduledTaskStore;
	}

	@Override
	public void push(String qUrl, String body) {
		String qName = fromQueueUrl(qUrl);
		messageStore.putIfAbsent(qName, new ConcurrentLinkedDeque<>());

		Message newMessage = new Message()
								.withMessageId(UUID.randomUUID().toString())
								.withBody(body);
		messageStore.get(qName).add(newMessage);
	}

	@Override
	public Optional<Message> pull(String qUrl) {
		String qName = fromQueueUrl(qUrl);
		if(messageStore.get(qName) == null || messageStore.get(qName).isEmpty()) {
			return Optional.empty();
		}

		Message message = messageStore.get(qName).poll().clone();
		String receiptHandle = "MSG-ID-" + message.getMessageId() + "-RH-" + UUID.randomUUID().toString();
		message.setReceiptHandle(receiptHandle);
		addToInvisibleStore(qName, message);

		ScheduledFuture visibilityTimeoutTask = executorService.schedule(() -> {
			invisibleMessageStore.get(qName).remove(message.getMessageId());
			messageStore.get(qName).addFirst(message);
			scheduledTaskStore.get(qName).remove(message.getMessageId());
		}, VISIBILITY_TIMEOUT, TimeUnit.SECONDS);

		addToScheduledTaskStore(qName, message.getMessageId(), visibilityTimeoutTask);
		return Optional.of(message);
	}

	private void addToInvisibleStore(String qName, Message message) {
		invisibleMessageStore.putIfAbsent(qName,  new ConcurrentHashMap<>());
		invisibleMessageStore.get(qName).put(message.getMessageId(), message);
	}

	private void addToScheduledTaskStore(String qName, String messageId, ScheduledFuture scheduledFuture) {
		scheduledTaskStore.putIfAbsent(qName, new ConcurrentHashMap<>());
		scheduledTaskStore.get(qName).put(messageId, scheduledFuture);
	}

	@Override
	public void delete(String qUrl, String receiptHandler) {
		String qName = fromQueueUrl(qUrl);

		String messageId = fromReceiptHandler(receiptHandler);
		if(scheduledTaskStore.get(qName).get(messageId) == null) {
			System.out.println("Scheduled task unavailable. Visibility timeout might have been executed. "
					+ "Message with handler " + receiptHandler + " may not be available for deletion");
			return;
		}
		scheduledTaskStore.get(qName).get(messageId).cancel(false);
		scheduledTaskStore.get(qName).remove(messageId);
		invisibleMessageStore.get(qName).remove(messageId);
	}

	private String fromQueueUrl(String queueUrl) {
		return Paths.get(queueUrl).getFileName().toString();
	}

	private String fromReceiptHandler(String receiptHandler) {
		return StringUtils.substringBetween(receiptHandler, "MSG-ID-", "-RH-");
	}
}
