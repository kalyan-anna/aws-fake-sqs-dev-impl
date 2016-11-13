package com.example;

import com.example.model.CanvaMessage;

import java.util.Optional;

public interface QueueService {

	void push(String queueUrl, String messageBody);

	Optional<CanvaMessage> pull(String queueUrl);

	void delete(String queueUrl, String receiptHandler);
}
