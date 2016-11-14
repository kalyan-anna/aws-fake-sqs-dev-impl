package com.example;

import org.apache.commons.lang3.RandomStringUtils;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * UniversalSequenceGenerator is a process independent file based unique id generator.
 *
 * This is used to create unique messageId and receiptHandler
 */
class UniversalSequenceGenerator {

	static int TOTAL_CACHE = 1000;
	static Path SEQUENCE_FILE_PATH = Paths.get(FileQueueService.BASE_PATH, "sequence");

	static {
		if(Files.notExists(SEQUENCE_FILE_PATH)) {
			try {
				Files.createDirectories(SEQUENCE_FILE_PATH.getParent());
				Files.createFile(SEQUENCE_FILE_PATH);
				Files.write(SEQUENCE_FILE_PATH, "1".getBytes());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private int remainingCacheCount = 0;
	private long currentValue;

	synchronized String nextValue() {
		if(remainingCacheCount == 0) {
			fetchAndCacheNextSequenceBatch();
		}
		remainingCacheCount--;
		return String.valueOf(currentValue++) + "-sq-" + RandomStringUtils.randomAlphanumeric(4);
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
