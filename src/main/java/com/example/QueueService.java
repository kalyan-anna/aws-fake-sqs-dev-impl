package com.example;

import com.amazonaws.services.sqs.model.Message;

import java.util.Optional;

/**
 * Implementation classes are package private.
 *
 * Not sure about dependency injection framework used in-house. But most open source frameworks like Spring
 * can autowire package private beans
 */
public interface QueueService {

	void push(String qUrl, String messageBody);

	Optional<Message> pull(String qUrl);

	void delete(String qUrl, String receiptHandler);
}
