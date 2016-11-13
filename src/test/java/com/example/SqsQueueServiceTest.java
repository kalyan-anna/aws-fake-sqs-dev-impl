package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.example.model.CanvaMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SqsQueueServiceTest {

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
	public void push_shouldCacheQueueUrl() {
		String qName="dummyQ";
		queueService.push(qName, "dummyMessage1");
		queueService.push(qName, "dummyMessage2");
		queueService.push(qName, "dummyMessage3");
		verify(sqs, times(1)).createQueue(qName);
	}

	@Test
	public void push_shouldCreateNewQueue_whenQueueDoesNotExists() throws Exception {
		String qName="dummyQ";
		queueService.push(qName, "dummyMessage");
		verify(sqs, times(1)).createQueue(qName);
	}

	@Test
	public void pull_shouldInvokeReceiveMessageOnSQS() {
		when(sqs.receiveMessage(anyString())).thenReturn(new ReceiveMessageResult().withMessages(new Message()));
		when(sqs.listQueues(anyString())).thenReturn(new ListQueuesResult().withQueueUrls("qName"));

		queueService.pull("qName");

		verify(sqs, times(1)).receiveMessage(anyString());
	}

	@Test(expected = IllegalArgumentException.class)
	public void push_shouldThrowException_whenQueueNameIsInvalid() {
		queueService.push(null, "dummyMessage");
	}

	@Test(expected = IllegalArgumentException.class)
	public void push_shouldThrowException_whenMessageBodyIsInvalid() {
		queueService.push("qName", null);
	}

	@Test
	public void pull_shouldReturnEmptyOptional_whenThereIsNoMessageInQueue() {
		when(sqs.listQueues(anyString())).thenReturn(new ListQueuesResult().withQueueUrls("qName"));
		when(sqs.receiveMessage(anyString())).thenReturn(new ReceiveMessageResult().withMessages(Collections.emptyList()));

		Optional<CanvaMessage> message = queueService.pull("qName");

		assertThat(message.isPresent(), is(false));
	}

	@Test(expected = IllegalArgumentException.class)
	public void pull_shouldThrowException_whenQNameIsNotValid() {
		when(sqs.listQueues(anyString())).thenReturn(new ListQueuesResult().withQueueUrls(Collections.emptyList()));
		queueService.pull("qName");
	}

	@Test
	public void pull_shouldCacheQueueUrl() {
		String qName = "qName";
		when(sqs.listQueues(anyString())).thenReturn(new ListQueuesResult().withQueueUrls(qName));
		when(sqs.receiveMessage(anyString())).thenReturn(new ReceiveMessageResult().withMessages(new Message()));

		queueService.pull(qName);
		queueService.pull(qName);
		queueService.pull(qName);
		verify(sqs, times(1)).listQueues(qName);
	}


	@Test(expected = IllegalArgumentException.class)
	public void pull_shouldThrowException_whenQueueNameIsInvalid() {
		queueService.pull(null);
	}

	@Test
	public void delete_shouldInvokeDeleteMessageOnSQS() {
		queueService.delete("qName", "receiptHandler");
		verify(sqs, times(1)).deleteMessage(anyString(), anyString());
	}
}
