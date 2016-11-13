package com.example;

import com.amazonaws.services.sqs.model.Message;

import java.util.Optional;

class FileQueueService implements QueueService {

	@Override
	public void push(String queueUrl, String messageBody) {

	}

	@Override
	public Optional<Message> pull(String queueUrl) {
		return null;
	}

	@Override
	public void delete(String queueUrl, String receiptHandler) {

	}

}
