package com.example;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.util.Collections.*;

class UniversalSequenceGenerator {

	private static UniversalSequenceGenerator INSTANCE = new UniversalSequenceGenerator();

	private int remainingCache = 0;
	private Integer currentSequenceNumber;

	private UniversalSequenceGenerator() {

	}

	public static UniversalSequenceGenerator getInstance() {
		return INSTANCE;
	}

	public String nextSequence(String qName) {
		if(remainingCache == 0) {
			fetchAndCacheNextSequences(qName);
		}
		remainingCache--;
		currentSequenceNumber++;
		return String.valueOf(currentSequenceNumber);
	}

	private void fetchAndCacheNextSequences(String qName) {
		Path sequenceFilePath = Paths.get(FileQueueService.BASE_PATH, qName, "config", "sequence");
		try (RandomAccessFile file = new RandomAccessFile(sequenceFilePath.toFile(), "rw")) {
			FileLock lock = file.getChannel().lock();
			currentSequenceNumber = Integer.parseInt(Files.readAllLines(sequenceFilePath).get(0));
			remainingCache = 1000;
			Files.write(sequenceFilePath, singleton(Integer.toString(currentSequenceNumber + 1000)) , Charset.defaultCharset());
			lock.release();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
