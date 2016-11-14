package com.example;

import org.apache.commons.lang3.StringUtils;
import static org.apache.commons.lang3.StringUtils.*;
import org.apache.commons.lang3.math.NumberUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import static org.hamcrest.Matchers.*;
import static com.example.UniversalSequenceGenerator.*;

import static org.junit.Assert.*;

public class UniversalSequenceGeneratorTest {

	private UniversalSequenceGenerator sequence;

	@Before
	public void before() {
		sequence = new UniversalSequenceGenerator();
	}

	@Test
	public void nextValue_shouldReturnTheValueInSequenceFile() throws Exception {
		Long currentSequenceNumberInFile = currentSequenceNumberInFile();
		String nextValue = sequence.nextValue();
		assertThat(substringBefore(nextValue, "-sq-"), equalTo(currentSequenceNumberInFile.toString()));
	}

	@Test
	public void nextValue_shouldIncrementFileSequenceAfterReading() throws Exception {
		Long firstRead = currentSequenceNumberInFile();
		sequence.nextValue();
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