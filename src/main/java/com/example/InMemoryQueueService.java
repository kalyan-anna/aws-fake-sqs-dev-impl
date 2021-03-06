package com.example;

import com.amazonaws.services.sqs.model.Message;
import static org.apache.commons.lang3.StringUtils.*;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

class InMemoryQueueService implements QueueService {

	private static final int DEFAULT_VISIBILITY_TIMEOUT = Integer.valueOf(System.getProperty("visibility.timeout.sec"));

	private ConcurrentHashMap<String, DelayQueue<Record>> messageStore;

	InMemoryQueueService(ConcurrentHashMap<String, DelayQueue<Record>> messageStore) {
		this.messageStore = messageStore;
	}

	@Override
	public void push(String qUrl, String body) {
		String qName = fromQueueUrl(qUrl);
		synchronized(InMemoryQueueService.class) {
			messageStore.putIfAbsent(qName, new DelayQueue<>());
		}

		Message newMessage = new Message()
				.withMessageId(UUID.randomUUID().toString())
				.withBody(body);
		Record record = Record.toRecord(newMessage);
		messageStore.get(qName).add(record);
	}

	@Override
	public Optional<Message> pull(String qUrl) {
		return pull(qUrl, DEFAULT_VISIBILITY_TIMEOUT);
	}

	Optional<Message> pull(String qUrl, int visibilityTimeout) {
		String qName = fromQueueUrl(qUrl);
		if(messageStore.get(qName) == null) {
			return Optional.empty();
		}

		Record nextVisibleMessage = messageStore.get(qName).poll();
		if(nextVisibleMessage == null) {
			return Optional.empty();
		}
		nextVisibleMessage.getMessage().setReceiptHandle("RH-" + UUID.randomUUID().toString());
		nextVisibleMessage.setDelayInSec(visibilityTimeout);
		//Adding record back to the queue; but it will preserve the order coz ordering is based on creationTime
		messageStore.get(qName).add(nextVisibleMessage);

		return Optional.of(nextVisibleMessage.getMessage().clone());
	}

	@Override
	public void delete(String qUrl, String receiptHandler) {
		String qName = fromQueueUrl(qUrl);
		Record messageToDelete = messageStore.get(qName).stream()
				.filter(msg -> isNoneBlank(msg.getMessage().getReceiptHandle()) && msg.getMessage().getReceiptHandle().equals(receiptHandler))
				.findFirst().orElse(null);
		messageStore.get(qName).remove(messageToDelete);
	}

	private String fromQueueUrl(String queueUrl) {
		return Paths.get(queueUrl).getFileName().toString();
	}

}
