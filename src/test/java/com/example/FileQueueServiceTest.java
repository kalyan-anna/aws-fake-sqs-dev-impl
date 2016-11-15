package com.example;

import com.amazonaws.services.sqs.model.Message;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class FileQueueServiceTest extends BaseTestClass {

	static String BASE_PATH = System.getProperty("fileQueueService.basePath");
	private UniversalUniqueIdGenerator sequence = new UniversalUniqueIdGenerator();
	private QueueService queueService;
	private final String qUrlBase = "https://sqs.amazonaws.com/373529781950/";
	private ConcurrentHashMap<String, ConcurrentHashMap<String, ScheduledFuture<?>>> scheduledTaskStore;

	@Before
	public void before() throws Exception {
		this.scheduledTaskStore = new ConcurrentHashMap<>();
		this.queueService = new FileQueueService(sequence, Executors.newScheduledThreadPool(3), scheduledTaskStore);
		deleteAllSubDirectories(Paths.get(BASE_PATH));
	}

	@Test(expected = IllegalArgumentException.class)
	public void push_shouldThrowException_whenQueueUrlIsInvalid() {
		queueService.push(null, "dummyMessage");
	}

	@Test(expected = IllegalArgumentException.class)
	public void push_shouldThrowException_whenMessageBodyIsInvalid() {
		queueService.push(qUrlBase + "qName", null);
	}

	@Test
	public void push_shouldSetupQueueDirectory_whenDoesNotExists() throws Exception {
		String qName = "test-queue";
		deleteQueueDirectory(qName);
		queueService.push(qUrlBase + qName, "Test Message body");
		assertThat(Files.exists(Paths.get(BASE_PATH, qName)), is(true));
	}

	@Test
	public void push_shouldAddMessageToFile() {
		String qName = "test-queue";
		String body = "Test Message body";

		queueService.push(qUrlBase + qName, body);

		List<String> lines = readAllLines(Paths.get(BASE_PATH, qName, "messages"));
		assertThat(lines.size(), equalTo(1));
		assertThat(lines.get(0), containsString(body));
	}

	@Test
	public void push_shouldAddMessageToFile_multipleMessages() {
		String qName = "test-queue";
		String body1 = "Test Message body 1";
		String body2 = "Test Message body 2";

		queueService.push(qUrlBase + qName, body1);
		queueService.push(qUrlBase + qName, body2);

		List<String> lines = readAllLines(Paths.get(BASE_PATH, qName, "messages"));
		assertThat(lines.size(), equalTo(2));
		assertThat(lines.get(0), containsString(body1));
		assertThat(lines.get(1), containsString(body2));
	}

	@Test
	public void pull_shouldRemoveTheRecordFromMessageAndAddToInvisibleMessage() {
		String qName = "test-queue";
		String body = "test message body";
		queueService.push(qUrlBase + qName, body);

		Optional<Message> message = queueService.pull(qUrlBase + qName);

		assertThat(message.isPresent(), is(true));
		assertThat(message.orElse(null).getBody(), equalTo(body));
		assertThat(message.orElse(null).getMessageId(), not(isEmptyOrNullString()));
		assertThat(message.orElse(null).getReceiptHandle(), not(isEmptyOrNullString()));
		List<String> lines = readAllLines(Paths.get(BASE_PATH, qName, "invisibleMessages"));
		assertThat(lines.size(), equalTo(1));
		assertThat(lines.get(0), containsString(body));
	}

	@Test
	public void pull_shouldReturnEmptyOptionalMessage_whenNoMessageInQueue() {
		String qName = "test-queue";
		String body = "test message body";
		queueService.push(qUrlBase + qName, body);
		queueService.pull(qUrlBase + qName);

		Optional<Message> message = queueService.pull(qUrlBase + qName);

		assertThat(message.isPresent(), equalTo(false));
	}

	@Test
	public void pull_multipleMessage() {
		String qName = "test-queue-1";
		String body1 = "Im body 1";
		String body2 = "I'm body 2";
		queueService.push(qUrlBase + qName, body1);
		queueService.push(qUrlBase + qName, body2);

		Optional<Message> msg1 = queueService.pull(qUrlBase + qName);
		Optional<Message> msg2 = queueService.pull(qUrlBase + qName);
		Optional<Message> msg3 = queueService.pull(qUrlBase + qName);

		assertThat(msg1.isPresent(), equalTo(true));
		assertThat(msg1.orElse(null).getBody(), equalTo(body1));
		assertThat(msg2.isPresent(), equalTo(true));
		assertThat(msg2.orElse(null).getBody(), equalTo(body2));
		assertThat(msg3.isPresent(), is(false));
	}

	@Test
	public void pull_shouldSubmitScheduledTaskForVisibilityTimeout() {
		String qName = "test-queue";
		String body = "test message body";
		queueService.push(qUrlBase + qName, body);
		Optional<Message> message = queueService.pull(qUrlBase + qName);

		ScheduledFuture task = scheduledTaskStore.get(qName).get(message.orElse(null).getMessageId());
		assertThat(task, notNullValue());
		assertThat(task.isCancelled(), is(false));
	}

	@Test(expected = IllegalArgumentException.class)
	public void delete_shouldThrowException_whenQueueUrlIsInvalid() {
		queueService.delete(null, "receiptHandler");
	}

	@Test(expected = IllegalArgumentException.class)
	public void delete_shouldThrowException_whenReceiptHandlerIsInvalid() {
		queueService.delete("qUrl", null);
	}

	@Test
	public void delete_shouldRemoveRecordFromInvisibleFile(){
		String qName = "test-queue";
		String body = "test message body";
		queueService.push(qUrlBase + qName, body);
		Optional<Message> message = queueService.pull(qUrlBase + qName);
		queueService.delete(qUrlBase + qName, message.orElse(null).getReceiptHandle());
		List<String> lines = readAllLines(Paths.get(BASE_PATH, qName, "invisibleMessages"));
		assertThat(lines.isEmpty(), is(true));
	}

	static void deleteAllSubDirectories(Path dirPath) throws Exception {
		Files.list(dirPath)
				.map(Path::toFile)
				.filter(File::isDirectory)
				.forEach(FileUtils::deleteQuietly);
	}

	private void deleteQueueDirectory(String qName) throws IOException {
		Path qPath = Paths.get(BASE_PATH, qName);
		FileUtils.deleteDirectory(qPath.toFile());
	}

	private List<String> readAllLines(Path path) {
		try {
			return Files.lines(path).filter(StringUtils::isNotBlank).collect(Collectors.toList());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
