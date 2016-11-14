package com.example;

import com.amazonaws.services.sqs.model.Message;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static org.apache.commons.lang3.StringUtils.*;

class FileQueueService implements QueueService {

	static final String BASE_PATH = "canva-test/sqs";
	private static final String UNIVERSAL_LOCK = BASE_PATH + "/universalLock";

	static {
		if(Files.notExists(Paths.get(BASE_PATH))) {
			try {
				Files.createDirectories(Paths.get(BASE_PATH));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private UniversalSequenceGenerator sequence;

	FileQueueService(UniversalSequenceGenerator sequence) {
		this.sequence = sequence;
	}

	@Override
	public void push(String qUrl, String messageBody) {
		if(isBlank(qUrl) || isBlank(messageBody)) {
			throw new IllegalArgumentException("Invalid qName or messageBody");
		}
		String qName = fromQueueUrl(qUrl);

		setupQueueDirectoryIfAbsent(qName);
		String uniqueId = sequence.nextValue();
		writeMessageToQueue(qName, uniqueId, messageBody);
	}

	private void setupQueueDirectoryIfAbsent(String qName) {
		Path qPath = Paths.get(BASE_PATH, qName);
		if(Files.notExists(qPath)) {
			lock(UNIVERSAL_LOCK);
			if(Files.exists(qPath)) {
				return;
			}
			try {
				Files.createDirectories(qPath);
				Files.createFile(Paths.get(qPath.toString(), "messages"));
				Files.createFile(Paths.get(qPath.toString(), "invisibleMessages"));
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				unLock(UNIVERSAL_LOCK);
			}
		}
	}

	private void writeMessageToQueue(String qName, String uniqueId, String body) {
		String record = uniqueId + ":::" + body;
		Path messagePath = Paths.get(BASE_PATH, qName, "messages");
		Path lock = Paths.get(BASE_PATH, qName, "lock");
		lock(lock.toString());
		try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(messagePath.toFile(), true)))) {
			writer.append(record);
			writer.println();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			unLock(lock.toString());
		}
	}

	@Override
	public Optional<Message> pull(String qUrl) {
		return null;
	}

	@Override
	public void delete(String qUrl, String receiptHandler) {

	}

	private String fromQueueUrl(String queueUrl) {
		return Paths.get(queueUrl).getFileName().toString();
	}

	private void lock(String lockPath) {
		File file = new File(lockPath);
		while(!file.mkdirs()) { }
	}

	private void unLock(String lockPath) {
		File file = new File(lockPath);
		file.delete();
	}
}
