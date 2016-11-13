package com.example;

import com.example.model.CanvaMessage;

import java.util.Optional;

public class FileQueueService implements QueueService {
	@Override
	public void push(String queueUrl, String messageBody) {

	}

	@Override
	public Optional<CanvaMessage> pull(String queueUrl) {
		return null;
	}

	@Override
	public void delete(String queueUrl, String receiptHandler) {

	}

}
