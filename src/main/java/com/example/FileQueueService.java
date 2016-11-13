package com.example;

import com.amazonaws.services.sqs.model.Message;

import java.util.Optional;

public class FileQueueService implements QueueService {

	@Override
	public void push(String qUrl, String messageBody) {

	}

	@Override
	public Optional<Message> pull(String qUrl) {
		return null;
	}

	@Override
	public void delete(String qUrl, String receiptHandler) {

	}

}
