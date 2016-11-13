package com.example;

import com.amazonaws.services.sqs.model.Message;

import java.util.Optional;

public interface QueueService {

	void push(String qUrl, String messageBody);

	Optional<Message> pull(String qUrl);

	void delete(String qUrl, String receiptHandler);
}
