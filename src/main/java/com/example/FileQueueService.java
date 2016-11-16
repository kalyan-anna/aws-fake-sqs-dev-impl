package com.example;

import com.amazonaws.services.sqs.model.Message;
import static org.apache.commons.lang3.StringUtils.*;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static java.nio.file.StandardOpenOption.*;

class FileQueueService implements QueueService {

	private static final String BASE_PATH = System.getProperty("fileQueueService.basePath");
	private static final String UNIVERSAL_LOCK = BASE_PATH + "/universal-lock";
	private static final int DEFAULT_VISIBILITY_TIMEOUT = Integer.valueOf(System.getProperty("visibility.timeout.sec"));

	static {
		setupBaseDirIfAbsent(BASE_PATH);
	}

	private UniversalUniqueIdGenerator idGenerator;

	FileQueueService(UniversalUniqueIdGenerator idGenerator) {
		this.idGenerator = idGenerator;
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
	public void push(String qUrl, String body) {
		String qName = fromQueueUrl(qUrl);

		setupQueueDirectoryIfAbsent(qName);
		String messageId = idGenerator.nextValue();
		lockQ(qName);
		try {
			appendRecordToFile(qName, Record.toRecord(messageId, body));
		} finally {
			unlockQ(qName);
		}
	}

	private void appendRecordToFile(String qName, Record record) {
		Path messagePath = Paths.get(BASE_PATH, qName, "messages");
		try {
			Files.write(messagePath, record.toLine().getBytes(), APPEND);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Optional<Message> pull(String qUrl) {
		return pull(qUrl, DEFAULT_VISIBILITY_TIMEOUT);
	}

	Optional<Message> pull(String qUrl, int visibilityTimeout) {
		String qName = fromQueueUrl(qUrl);
		lockQ(qName);
		try {
			List<Record> records = readAllRecordsFromFile(qName);
			Optional<Record> nextVisibleRecord = records.stream().filter(Record::isVisible).findFirst();
			if (!nextVisibleRecord.isPresent()) {
				return Optional.empty();
			}

			nextVisibleRecord.get().setDelayInSec(visibilityTimeout);
			String receiptHandle = "RH-" + idGenerator.nextValue();
			nextVisibleRecord.get().getMessage().setReceiptHandle(receiptHandle);
			writeRecordsToFile(qName, records);

			return Optional.of(nextVisibleRecord.get().getMessage().clone());
		} finally {
			unlockQ(qName);
		}
	}

	@Override
	public void delete(String qUrl, String receiptHandler) {
		String qName = fromQueueUrl(qUrl);
		lockQ(qName);
		try {
			List<Record> records = readAllRecordsFromFile(qName);
			Optional<Record> recordToDelete = records.stream()
					.filter(r -> isNotBlank(r.getMessage().getReceiptHandle()) && r.getMessage().getReceiptHandle().equals(receiptHandler))
					.findFirst();
			if(!recordToDelete.isPresent()) {
				System.out.println("Message with receiptHandler " + receiptHandler + " is not available for deletion. Visibility timeout might have been executed");
				return;
			}
			records.remove(recordToDelete.get());
			writeRecordsToFile(qName, records);
		} finally {
			unlockQ(qName);
		}
	}

	private List<Record> readAllRecordsFromFile(String qName) {
		Path messagePath = Paths.get(BASE_PATH, qName, "messages");
		if(Files.notExists(messagePath)) {
			return Collections.emptyList();
		}
		try(Stream<String> stream = Files.lines(messagePath)) {
			return stream.filter(StringUtils::isNotBlank).map(Record::fromLine).collect(Collectors.toList());
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void writeRecordsToFile(String qName, List<Record> records) {
		Path messagePath = Paths.get(BASE_PATH, qName, "messages");
		List<String> lines = records.stream().map(Record::toLine).collect(Collectors.toList());
		try {
			Files.write(messagePath, lines);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
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
		while(!file.mkdirs()) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void unlock(String lockPath) {
		File file = new File(lockPath);
		file.delete();
	}

}
