package com.example;

import com.amazonaws.services.sqs.model.Message;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;

class Record implements Delayed {

	private static final String NEW_LINE = System.getProperty("line.separator");

	private Message message;
	private long creationTime;
	private long visibleFromTime;

	private Record(Message message) {
		this.message = message;
		this.creationTime = System.nanoTime();
		this.visibleFromTime = currentTimeMillis();
	}

	private Record(String messageId, String body) {
		this.message = new Message().withMessageId(messageId).withBody(body);
		this.creationTime = System.nanoTime();
		this.visibleFromTime = currentTimeMillis();
	}

	private Record(Message message, long creationTime, long visibleFromTime) {
		this.message = message;
		this.creationTime = creationTime;
		this.visibleFromTime = visibleFromTime;
	}

	static Record toRecord(String messageId, String body) {
		return new Record(messageId, body);
	}

	static Record toRecord(Message message) {
		return new Record(message);
	}

	static Record fromLine(String line) {
		String[] fields = line.split("::");
		Message message = new Message().withMessageId(fields[0]).withReceiptHandle(fields[3]).withBody(fields[4]);
		return new Record(message, Long.parseLong(fields[2]), Long.parseLong(fields[1]));
	}

	String toLine() {
		return message.getMessageId() + "::" + visibleFromTime + "::" + creationTime + "::" + message.getReceiptHandle() + "::" + message.getBody() + NEW_LINE;
	}

	/**
	 * Sort by visibility and creationTime to preserve FIFO order
	 */
	@Override
	public int compareTo(Delayed other) {
		if(this.isVisible() && !((Record) other).isVisible()) {
			return -1;
		}
		if(!this.isVisible() && ((Record) other).isVisible()) {
			return 1;
		}
		if(this.creationTime < ((Record) other).creationTime) {
			return -1;
		}
		if(this.creationTime > ((Record) other).creationTime) {
			return 1;
		}
		return 0;
	}

	@Override
	public long getDelay(TimeUnit unit) {
		return unit.convert(visibleFromTime - currentTimeMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof Record && this.message.equals(((Record) obj).getMessage());
	}

	Message getMessage() {
		return message;
	}

	void setDelayInSec(int delayInSec) {
		this.visibleFromTime = currentTimeMillis() + (delayInSec * 1000);
	}

	boolean isVisible() {
		return this.visibleFromTime - currentTimeMillis() <= 0;
	}

}
