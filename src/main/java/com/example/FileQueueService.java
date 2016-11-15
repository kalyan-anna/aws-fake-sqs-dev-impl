package com.example;

import com.amazonaws.services.sqs.model.Message;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static java.nio.file.StandardOpenOption.*;
import static org.apache.commons.lang3.StringUtils.*;

class FileQueueService implements QueueService {

	private final String BASE_PATH;
	private final String UNIVERSAL_LOCK;
	private final String NEW_LINE = System.getProperty("line.separator");

	private UniversalUniqueIdGenerator idGenerator;

	FileQueueService(UniversalUniqueIdGenerator idGenerator) {
		this.idGenerator = idGenerator;
		this.BASE_PATH = System.getProperty("fileQueueService.basePath");
		this.UNIVERSAL_LOCK = BASE_PATH + "/universal-lock";
		setupBaseDirIfAbsent(BASE_PATH);
	}

	static void setupBaseDirIfAbsent(String basePath) {
		if(Files.notExists(Paths.get(basePath))) {
			try {
				Files.createDirectories(Paths.get(basePath));
				Path sequenceFilePath = Paths.get(basePath, "sequence");
				Files.createFile(sequenceFilePath);
				Files.write(sequenceFilePath, "1".getBytes());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void push(String qUrl, String messageBody) {
		if(isBlank(qUrl) || isBlank(messageBody)) {
			throw new IllegalArgumentException("Invalid qName or messageBody");
		}
		String qName = fromQueueUrl(qUrl);

		setupQueueDirectoryIfAbsent(qName);
		String messageId = idGenerator.nextValue();
		addMessageToQueue(qName, messageId, messageBody);
	}

	private void addMessageToQueue(String qName, String messageId, String body) {
		String record = toRecord(messageId, "", body) + NEW_LINE;
		Path messagePath = Paths.get(BASE_PATH, qName, "messages");
		lockQ(qName);
		try {
			Files.write(messagePath, record.getBytes(), APPEND);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			unlockQ(qName);
		}
	}

	@Override
	public Optional<Message> pull(String qUrl) {
		if(isBlank(qUrl)) {
			throw new IllegalArgumentException("Invalid qUrl");
		}
		String qName = fromQueueUrl(qUrl);

		Optional<Message> message = pollQueue(qName);
		if(!message.isPresent()) {
			return Optional.empty();
		}
		String receiptHandle = "MSG-ID-" + message.get().getMessageId() + "-RH-" + idGenerator.nextValue();
		message.get().setReceiptHandle(receiptHandle);

		addToInvisible(qName, message.get());
		return message;
	}

	private Optional<Message> pollQueue(String qName) {
		Path messagePath = Paths.get(BASE_PATH, qName, "messages");
		lockQ(qName);
		try (Stream<String> stream = Files.lines(messagePath)) {
			List<String> lines = stream.collect(Collectors.toList());
			if(lines.isEmpty()) {
				return Optional.empty();
			}
			String record = lines.remove(0);
			Files.write(messagePath, lines);
			return Optional.of(toMessage(record));
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			unlockQ(qName);
		}
	}

	private void addToInvisible(String qName, Message message) {
		Path invisiblePath = Paths.get(BASE_PATH, qName, "invisibleMessages");
		String record = toRecord(message.getMessageId(), message.getReceiptHandle(), message.getBody()) + NEW_LINE;
		lockQ(qName);
		try  {
			Files.write(invisiblePath, record.getBytes(), APPEND);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			unlockQ(qName);
		}
	}

	private Message toMessage(String record) {
		String[] fields = record.split(":::");
		return new Message().withMessageId(fields[0]).withReceiptHandle(fields[1]).withBody(fields[2]);
	}

	private String toRecord(String messageId, String receiptHandler, String body) {
		return messageId + ":::" + receiptHandler + ":::" + body;
	}

	@Override
	public void delete(String qUrl, String receiptHandler) {

	}

	private void setupQueueDirectoryIfAbsent(String qName) {
		Path qPath = Paths.get(BASE_PATH, qName);
		if(Files.notExists(qPath)) {
			lock(UNIVERSAL_LOCK);
			if(Files.exists(qPath)) {
				unlock(UNIVERSAL_LOCK);
				return;
			}
			try {
				Files.createDirectories(qPath);
				Files.createFile(Paths.get(qPath.toString(), "messages"));
				Files.createFile(Paths.get(qPath.toString(), "invisibleMessages"));
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				unlock(UNIVERSAL_LOCK);
			}
		}
	}

	private String fromQueueUrl(String queueUrl) {
		return Paths.get(queueUrl).getFileName().toString();
	}

	private void lockQ(String qName) {
		Path lock = Paths.get(BASE_PATH, qName, "lock");
		lock(lock.toString());
	}

	private void unlockQ(String qName) {
		Path lock = Paths.get(BASE_PATH, qName, "lock");
		unlock(lock.toString());
	}

	private void lock(String lockPath) {
		File file = new File(lockPath);
		while(!file.mkdirs()) { }
	}

	private void unlock(String lockPath) {
		File file = new File(lockPath);
		file.delete();
	}

}
