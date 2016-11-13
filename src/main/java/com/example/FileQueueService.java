package com.example;

import com.example.model.CanvaMessage;

import java.util.Optional;

public class FileQueueService implements QueueService {
	@Override
	public void push(String qName, String messageBody) {

	}

	@Override
	public Optional<CanvaMessage> pull(String qName) {
		return null;
	}

	@Override
	public void delete(String qName, String receiptHandler) {

	}

}
