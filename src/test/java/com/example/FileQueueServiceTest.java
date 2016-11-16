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
import java.util.stream.Collectors;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class FileQueueServiceTest extends BaseTestClass {

	static String BASE_PATH = System.getProperty("fileQueueService.basePath");
	private UniversalUniqueIdGenerator sequence = new UniversalUniqueIdGenerator();
	private QueueService queueService;
	private final String qUrlBase = "https://sqs.amazonaws.com/373529781950/";

	@Before
	public void before() throws Exception {
		this.queueService = new FileQueueService(sequence);
		deleteAllSubDirectories(Paths.get(BASE_PATH));
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

		List<String> lines = readAllLinesFromQueue(qName);
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

		List<String> lines = readAllLinesFromQueue(qName);
		assertThat(lines.size(), equalTo(2));
		assertThat(lines.get(0), containsString(body1));
		assertThat(lines.get(1), containsString(body2));
	}

	@Test
	public void pull_shouldReturnMessageInFile() {
		String qName = "test-queue";
		String body1 = "Test Message body 1";
		queueService.push(qUrlBase + qName, body1);
		Message message = queueService.pull(qUrlBase + qName).orElse(null);
		String[] fields = readAllLinesFromQueue(qName).get(0).split("::");
		assertThat(fields[0], equalTo(message.getMessageId()));
		assertThat(fields[3], equalTo(message.getReceiptHandle()));
		assertThat(fields[4], equalTo(message.getBody()));
	}

	@Test
	public void pull_shouldIncreaseVisibilityFieldInFile() {
		String qName = "test-queue";
		String body1 = "Test Message body 1";
		queueService.push(qUrlBase + qName, body1);
		String[] fieldsBeforePush = readAllLinesFromQueue(qName).get(0).split("::");

		queueService.pull(qUrlBase + qName).orElse(null);
		String[] fieldsAfterPush = readAllLinesFromQueue(qName).get(0).split("::");

		assertThat(Long.parseLong(fieldsBeforePush[1]), lessThan(Long.parseLong(fieldsAfterPush[1])));
	}

	@Test
	public void pull_shouldIgnoreInvisibleMessages() {
		String qName = "test-queue";
		String body1 = "Test Message body 1";
		String body2 = "Test Message body 2";
		queueService.push(qUrlBase + qName, body1);
		queueService.pull(qUrlBase + qName);
		queueService.push(qUrlBase + qName, body2);
		Message message = queueService.pull(qUrlBase + qName).orElse(null);
		assertThat(message.getBody(), equalTo(body2));
	}

	@Test
	public void pull_shouldReturnEmptyMessage_whenNoMessage() {
		String qName = "test-queue";
		Optional<Message> message = queueService.pull(qUrlBase + qName);
		assertThat(message.isPresent(), equalTo(false));
	}

	@Test
	public void pull_shouldReturnEmpty_whenNoVisibleMessage() {
		String qName = "test-queue";
		String body1 = "Test Message body 1";
		String body2 = "Test Message body 2";
		queueService.push(qUrlBase + qName, body1);
		queueService.push(qUrlBase + qName, body2);
		queueService.pull(qUrlBase + qName);
		queueService.pull(qUrlBase + qName);
		Optional<Message> message = queueService.pull(qUrlBase + qName);
		assertThat(message.isPresent(), equalTo(false));
	}

	@Test
	public void delete_shouldRemoveRecordFromFile(){
		String qName = "test-queue";
		String body = "test message body";
		queueService.push(qUrlBase + qName, body);
		Optional<Message> message = queueService.pull(qUrlBase + qName);
		queueService.delete(qUrlBase + qName, message.orElse(null).getReceiptHandle());

		List<String> lines = readAllLinesFromQueue(qName);
		assertThat(lines.isEmpty(), is(true));
	}

	@Test
	public void delete_shouldRemoveRecordFromFile_multipleMessageScenario(){
		String qName = "test-queue";
		String body1 = "test message body";
		String body2 = "test message body 2";

		queueService.push(qUrlBase + qName, body1);
		Message msg1 = queueService.pull(qUrlBase + qName).orElse(null);

		queueService.push(qUrlBase + qName, body2);
		Optional<Message> msg2 = queueService.pull(qUrlBase + qName);

		queueService.delete(qUrlBase + qName, msg2.orElse(null).getReceiptHandle());

		List<String> lines = readAllLinesFromQueue(qName);
		assertThat(lines.size(), is(1));
		String[] fields = lines.get(0).split("::");
		assertThat(fields[0], equalTo(msg1.getMessageId()));
		assertThat(fields[3], equalTo(msg1.getReceiptHandle()));
		assertThat(fields[4], equalTo(msg1.getBody()));
	}

	@Test
	public void pull_visibilityTimeoutRecord_shouldBeAvailable() {
		String qName = "test-queue";
		String body1 = "test message body";
		String body2 = "test message body 2";
		String body3 = "test message body 3";
		queueService.push(qUrlBase + qName, body1);
		queueService.push(qUrlBase + qName, body2);
		queueService.push(qUrlBase + qName, body3);

		queueService.pull(qUrlBase + qName);
		Message msg2 = ((FileQueueService)queueService).pull(qUrlBase + qName, 0).orElse(null);
		Message msg2_2nd = queueService.pull(qUrlBase + qName).orElse(null);
		assertThat(msg2_2nd.getMessageId(), equalTo(msg2.getMessageId()));
		assertThat(msg2_2nd.getBody(), equalTo(msg2.getBody()));
		assertThat(msg2_2nd.getBody(), equalTo(body2));
		assertThat(msg2_2nd.getReceiptHandle(), not(equalTo(msg2.getReceiptHandle())));

		Message msg3 = queueService.pull(qUrlBase + qName).orElse(null);
		assertThat(msg3.getBody(), equalTo(body3));

		Optional<Message> msg4 = queueService.pull(qUrlBase + qName);
		assertThat(msg4.isPresent(), equalTo(false));
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

	private List<String> readAllLinesFromQueue(String qName) {
		Path messagePath = Paths.get(BASE_PATH, qName, "messages");
		try {
			return Files.lines(messagePath).filter(StringUtils::isNotBlank).collect(Collectors.toList());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
