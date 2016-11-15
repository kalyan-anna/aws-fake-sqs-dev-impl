package com.example;

import com.amazonaws.services.sqs.model.Message;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static java.nio.file.StandardOpenOption.*;
import static org.apache.commons.lang3.StringUtils.*;

class FileQueueService implements QueueService {

	private static final String BASE_PATH = System.getProperty("fileQueueService.basePath");
	private static final String UNIVERSAL_LOCK = BASE_PATH + "/universal-lock";
	private static final String NEW_LINE = System.getProperty("line.separator");
	private static final int VISIBILITY_TIMEOUT = Integer.valueOf(System.getProperty("visibility.timeout.sec"));
	private static final int POOL_SIZE = Integer.valueOf(System.getProperty("scheduled.task.thread.pool.size"));

	static {
		setupBaseDirIfAbsent(BASE_PATH);
	}

	private UniversalUniqueIdGenerator idGenerator;
	private ScheduledExecutorService executorService;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, ScheduledFuture<?>>> scheduledTaskStore;

	FileQueueService(UniversalUniqueIdGenerator idGenerator) {
		this(idGenerator, Executors.newScheduledThreadPool(POOL_SIZE), new ConcurrentHashMap<>());
	}

	FileQueueService(UniversalUniqueIdGenerator idGenerator, ScheduledExecutorService executorService,
			ConcurrentHashMap<String, ConcurrentHashMap<String, ScheduledFuture<?>>> scheduledTaskStore) {
		this.idGenerator = idGenerator;
		this.executorService = executorService;
		this.scheduledTaskStore = scheduledTaskStore;
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
		addMessageToQueueFile(qName, messageId, messageBody);
	}

	private void addMessageToQueueFile(String qName, String messageId, String body) {
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

		Optional<Message> message = pollMessageFromQueueFile(qName);
		if(!message.isPresent()) {
			return Optional.empty();
		}
		String receiptHandle = "MSG-ID-" + message.get().getMessageId() + "-RH-" + idGenerator.nextValue();
		message.get().setReceiptHandle(receiptHandle);
		addMessageToInvisibleFile(qName, message.get());

		ScheduledFuture future = executorService.schedule(() -> {
			Message msg = pullFromInvisibleFile(qName, message.get().getMessageId());
			addFirstToQueueFile(qName, msg);
			scheduledTaskStore.get(qName).remove(message.get().getMessageId());
		}, VISIBILITY_TIMEOUT, TimeUnit.SECONDS);

		addToScheduledTaskStore(qName, message.get().getMessageId(), future);
		return message;
	}

	private Optional<Message> pollMessageFromQueueFile(String qName) {
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

	private void addMessageToInvisibleFile(String qName, Message message) {
		Path invisiblePath = Paths.get(BASE_PATH, qName, "invisibleMessages");
		String record = toRecord(message) + NEW_LINE;
		lockQ(qName);
		try  {
			Files.write(invisiblePath, record.getBytes(), APPEND);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			unlockQ(qName);
		}
	}

	private void addToScheduledTaskStore(String qName, String messageId, ScheduledFuture scheduledFuture) {
		scheduledTaskStore.putIfAbsent(qName, new ConcurrentHashMap<>());
		scheduledTaskStore.get(qName).put(messageId, scheduledFuture);
	}

	private void addFirstToQueueFile(String qName, Message message) {
		Path messagePath = Paths.get(BASE_PATH, qName, "messages");
		lockQ(qName);
		try (Stream<String> stream = Files.lines(messagePath)) {
			List<String> lines = stream.collect(Collectors.toList());
			lines.add(0, toRecord(message));
			Files.write(messagePath, lines);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			unlockQ(qName);
		}
	}

	@Override
	public void delete(String qUrl, String receiptHandler) {
		if(isBlank(qUrl) || isBlank(receiptHandler)) {
			throw new IllegalArgumentException("Invalid qUrl or receiptHandler");
		}
		String qName = fromQueueUrl(qUrl);

		String messageId = fromReceiptHandler(receiptHandler);
		if(scheduledTaskStore.get(qName).get(messageId) == null) {
			System.out.println("Scheduled task unavailable. Visibility timeout might have been executed. "
					+ "Message with handler " + receiptHandler + " may not be available for deletion");
			return;
		}
		scheduledTaskStore.get(qName).get(messageId).cancel(false);
		scheduledTaskStore.get(qName).remove(messageId);
		pullFromInvisibleFile(qName, messageId);
	}

	private String fromReceiptHandler(String receiptHandler) {
		return StringUtils.substringBetween(receiptHandler, "MSG-ID-", "-RH-");
	}

	private Message pullFromInvisibleFile(String qName, String messageId) {
		Path invisiblePath = Paths.get(BASE_PATH, qName, "invisibleMessages");
		lockQ(qName);
		try (Stream<String> stream = Files.lines(invisiblePath)) {
			Set<String> lines = stream.collect(Collectors.toSet());
			Optional<String> record = lines.stream().filter(line -> line.split(":::")[0].equals(messageId)).findFirst();
			Set<String> remainingLines = lines.stream().filter(line -> !line.split(":::")[0].equals(messageId))
					.collect(Collectors.toSet());
			Files.write(invisiblePath, remainingLines);
			return toMessage(record.orElse(""));
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

	private String toRecord(Message message) {
		return toRecord(message.getMessageId(), message.getReceiptHandle(), message.getBody());
	}

	private String toRecord(String messageId, String receiptHandler, String body) {
		return messageId + ":::" + receiptHandler + ":::" + body;
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
