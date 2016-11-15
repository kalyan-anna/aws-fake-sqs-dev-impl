package com.example;

import org.apache.commons.lang3.StringUtils;
import static org.apache.commons.lang3.StringUtils.*;
import org.apache.commons.lang3.math.NumberUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.Matchers.*;
import static com.example.UniversalUniqueIdGenerator.*;

import static org.junit.Assert.*;

public class UniversalUniqueIdGeneratorTest extends BaseTestClass {

	private UniversalUniqueIdGenerator idGenerator;
	private static Path SEQUENCE_FILE_PATH;

	@BeforeClass
	public static void beforeAll() {
		String basePath = System.getProperty("fileQueueService.basePath");
		SEQUENCE_FILE_PATH = Paths.get(basePath, "sequence");
	}

	@Before
	public void before() {
		idGenerator = new UniversalUniqueIdGenerator();
	}

	@Test
	public void nextValue_shouldReturnTheValueInSequenceFile() throws Exception {
		Long currentSequenceNumberInFile = currentSequenceNumberInFile();
		String nextValue = idGenerator.nextValue();
		assertThat(substringBefore(nextValue, "-sq-"), equalTo(currentSequenceNumberInFile.toString()));
	}

	@Test
	public void nextValue_shouldIncrementFileSequenceAfterReading() throws Exception {
		Long firstRead = currentSequenceNumberInFile();
		idGenerator.nextValue();
		Long secondRead = currentSequenceNumberInFile();
		assertThat(secondRead, equalTo(firstRead + TOTAL_CACHE));
	}

	private Long currentSequenceNumberInFile() throws Exception {
		return Files.lines(SEQUENCE_FILE_PATH).filter(StringUtils::isNotBlank)
												.filter(NumberUtils::isParsable)
												.map(Long::parseLong)
												.findFirst().orElse(null);

	}
}