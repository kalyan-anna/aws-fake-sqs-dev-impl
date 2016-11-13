package com.example.model;

public class CanvaMessage {

	private String messageId;
	private String receiptHandle;
	private String body;

	public CanvaMessage(String messageId, String receiptHandle, String body) {
		this.messageId = messageId;
		this.receiptHandle = receiptHandle;
		this.body = body;
	}

	public String getMessageId() {
		return messageId;
	}

	public String getReceiptHandle() {
		return receiptHandle;
	}

	public String getBody() {
		return body;
	}
}
