package com.example;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static com.example.FileQueueService.BASE_PATH;

public class FileQueueServiceTest {

	private UniversalSequenceGenerator sequence = new UniversalSequenceGenerator();
	private QueueService queueService;
	private final String qUrlBase = "https://sqs.amazonaws.com/373529781950/";

	@Before
	public void before() throws Exception {
		this.queueService = new FileQueueService(sequence);
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

	private void deleteAllSubDirectories(Path dirPath) throws Exception {
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

	@Ignore
	@Test
	public void push_test() throws InterruptedException {
		Runnable fileLockTest = () -> {

			queueService.push(qUrlBase + "qUrl", "test message");
		};
		ExecutorService service = Executors.newFixedThreadPool(5);
		IntStream.rangeClosed(1, 10).parallel().forEach(i -> service.execute(fileLockTest));
		service.awaitTermination(10, TimeUnit.SECONDS);
	}
}
