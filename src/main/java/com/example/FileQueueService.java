package com.example;

import com.amazonaws.services.sqs.model.Message;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;

import static org.apache.commons.lang3.StringUtils.*;

class FileQueueService implements QueueService {

	static final String BASE_PATH = "canva-test/sqs";

	private final UniversalSequenceGenerator sequence = UniversalSequenceGenerator.getInstance();

	@Override
	public void push(String qUrl, String messageBody) {
		if(isBlank(qUrl) || isBlank(messageBody)) {
			throw new IllegalArgumentException("Invalid qName or messageBody");
		}
		String qName = fromQueueUrl(qUrl);

		setupQueueDirectoriesIfDoesNotExists(qName);
		String uniqueId = sequence.nextSequence(qName);
		writeToFile(qName, uniqueId, messageBody);
	}

	private void setupQueueDirectoriesIfDoesNotExists(String qName) {
		if(Files.notExists(Paths.get(BASE_PATH, qName))) {
			try {
				Files.createDirectories(Paths.get(BASE_PATH, qName));
				Files.createDirectories(Paths.get(BASE_PATH, qName, "polled"));
				Files.createDirectories(Paths.get(BASE_PATH, qName, "config"));
				Files.createDirectories(Paths.get(BASE_PATH, qName, "config", ".lock"));
				Path sequenceFilePath = Files.createFile(Paths.get(BASE_PATH, qName, "config", "sequence"));
				Files.write(sequenceFilePath, Collections.singleton("0"), Charset.defaultCharset());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void writeToFile(String qName, String uniqueId, String body) {
		try {
			Path messageFilePath = Files.createFile(Paths.get(BASE_PATH, qName, "MSG-" + uniqueId));
			Files.write(messageFilePath, Collections.singleton(body), Charset.defaultCharset());
		} catch (IOException e) {
			throw new RuntimeException(e);
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
}
