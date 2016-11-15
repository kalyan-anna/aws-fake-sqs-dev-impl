package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SqsQueueServiceTest extends BaseTestClass {

	private SqsQueueService queueService;

	@Mock
	private AmazonSQSClient sqs;

	@Before
	public void before() {
		queueService = new SqsQueueService(sqs);
		when(sqs.createQueue(anyString())).thenReturn(new CreateQueueResult().withQueueUrl("dummyQUrl"));
	}

	@Test
	public void push_shouldInvokeSendMessageOnSQS() {
		queueService.push("dummyQ", "dummyMessage");
		verify(sqs, times(1)).sendMessage(anyString(), anyString());
	}

	@Test
	public void pull_shouldInvokeReceiveMessageOnSQS() {
		when(sqs.receiveMessage(anyString())).thenReturn(new ReceiveMessageResult().withMessages(new Message()));
		when(sqs.listQueues(anyString())).thenReturn(new ListQueuesResult().withQueueUrls("qUrl"));

		queueService.pull("qUrl");

		verify(sqs, times(1)).receiveMessage(anyString());
	}

	@Test(expected = IllegalArgumentException.class)
	public void push_shouldThrowException_whenQueueUrlIsInvalid() {
		queueService.push(null, "dummyMessage");
	}

	@Test(expected = IllegalArgumentException.class)
	public void push_shouldThrowException_whenMessageBodyIsInvalid() {
		queueService.push("qUrl", null);
	}

	@Test
	public void pull_shouldReturnEmptyOptional_whenThereIsNoMessageInQueue() {
		when(sqs.listQueues(anyString())).thenReturn(new ListQueuesResult().withQueueUrls("qUrl"));
		when(sqs.receiveMessage(anyString())).thenReturn(new ReceiveMessageResult().withMessages(Collections.emptyList()));

		Optional<Message> message = queueService.pull("qUrl");

		assertThat(message.isPresent(), is(false));
	}

	@Test(expected = IllegalArgumentException.class)
	public void pull_shouldThrowException_whenQueueUrlIsInvalid() {
		queueService.pull(null);
	}

	@Test
	public void delete_shouldInvokeDeleteMessageOnSQS() {
		queueService.delete("qUrl", "receiptHandler");
		verify(sqs, times(1)).deleteMessage(anyString(), anyString());
	}

	@Test(expected = IllegalArgumentException.class)
	public void delete_shouldThrowException_whenQueueUrlIsInvalid() {
		queueService.delete(null, "receiptHandler");
	}

	@Test(expected = IllegalArgumentException.class)
	public void delete_shouldThrowException_whenReceiptHandlerIsInvalid() {
		queueService.delete("qUrl", null);
	}
}
