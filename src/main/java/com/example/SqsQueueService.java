package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;

import static org.apache.commons.lang.StringUtils.*;

import java.util.List;
import java.util.Optional;

public class SqsQueueService implements QueueService {

  private AmazonSQSClient sqs;

  public SqsQueueService(AmazonSQSClient sqs) {
    this.sqs = sqs;
  }

  @Override
  public void push(String queueUrl, String messageBody) {
    if(isBlank(queueUrl) || isBlank(messageBody)) {
      throw new IllegalArgumentException("Invalid qName or messageBody");
    }

    sqs.sendMessage(queueUrl, messageBody);
  }

  @Override
  public Optional<Message> pull(String queueUrl) {
    if(isBlank(queueUrl)) {
      throw new IllegalArgumentException("Invalid qName");
    }

    return sqs.receiveMessage(queueUrl).getMessages().stream().findFirst();
  }

  @Override
  public void delete(String queueUrl, String receiptHandler) {
    if(isBlank(queueUrl) || isBlank(receiptHandler)) {
      throw new IllegalArgumentException("Invalid qName or receiptHandler");
    }

    sqs.deleteMessage(queueUrl, receiptHandler);
  }
}
