package com.example;

import com.example.model.CanvaMessage;

import java.util.Optional;

public interface QueueService {

	void push(String qName, String messageBody);

	Optional<CanvaMessage> pull(String qName);

	void delete(String qName, String receiptHandler);
}
