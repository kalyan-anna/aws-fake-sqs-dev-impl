package com.example;

import org.apache.commons.lang3.RandomStringUtils;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * UniversalUniqueIdGenerator is a process independent file based unique id generator. However a batch of sequences are cached to reduce
 * I/O operation.
 *
 * This is used to create unique messageId and receiptHandler.
 */
class UniversalUniqueIdGenerator {

	static final int TOTAL_CACHE = 100;
	private final Path SEQUENCE_FILE_PATH;

	private int remainingCacheCount = 0;
	private long currentValue;

	UniversalUniqueIdGenerator() {
		String basePath = System.getProperty("fileQueueService.basePath");
		SEQUENCE_FILE_PATH = Paths.get(basePath, "sequence");
	}

	synchronized String nextValue() {
		if(remainingCacheCount == 0) {
			fetchAndCacheNextSequenceBatch();
		}
		remainingCacheCount--;
		return currentValue++ + "-sq-" + RandomStringUtils.randomAlphanumeric(4);
	}

	private void fetchAndCacheNextSequenceBatch() {
		try (RandomAccessFile file = new RandomAccessFile(SEQUENCE_FILE_PATH.toFile(), "rw")) {
			FileLock lock = file.getChannel().lock();
			currentValue = Long.parseLong(file.readLine());
			remainingCacheCount = TOTAL_CACHE;
			Files.write(SEQUENCE_FILE_PATH, Long.toString(currentValue + TOTAL_CACHE).getBytes());
			lock.release();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
