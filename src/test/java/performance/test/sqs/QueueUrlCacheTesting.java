package performance.test.sqs;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.google.common.base.Stopwatch;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Optional;
import java.util.stream.IntStream;

import static com.example.SqsUtil.createSQS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/*
The common interface QueueService accepts queueName and not queueUrl for portability across different implementations (In-memory, File, SQS).

This one off test is to check if caching queueUrl is efficient than accessing SQS every time
 */
@Ignore
public class QueueUrlCacheTesting {

	private static AmazonSQSClient SQS;

	@BeforeClass
	public static void beforeClass() {
		SQS = createSQS();
	}

	/**
	 * On my MAC it saves approx 250ms for each call to find qUrl. Therefore for each message it would save approx 1sec (push, pull, delete)
	 */
	@Test
	public void findQueueUrl() {
		String testQName = "testCacheUrlQueue";
		String testQUrl = SQS.createQueue(testQName).getQueueUrl();

		Stopwatch timer = Stopwatch.createStarted();
		getQueueUrlFromAws(testQName);
		timer.stop();
		System.out.println("Time taken to access queueUrl once:" + timer.elapsed(MILLISECONDS));

		timer = Stopwatch.createStarted();
		IntStream.range(0, 10).parallel().forEach($ -> {
			getQueueUrlFromAws(testQName);
		});
		timer.stop();
		System.out.println("Time taken to access queueUrl 100 times:" + timer.elapsed(MILLISECONDS));

		timer = Stopwatch.createStarted();
		IntStream.range(0, 10).forEach($ -> {
			getQueueUrlFromAws(testQName);
		});
		timer.stop();
		System.out.println("Time taken to access queueUrl 100 times without parallel stream:" + timer.elapsed(MILLISECONDS));

		SQS.deleteQueue(testQUrl);
	}

	private Optional<String> getQueueUrlFromAws(String qName) {
		return SQS.listQueues(qName).getQueueUrls().stream().filter(qUrl -> qUrl.endsWith(qName)).findFirst();
	}
}
