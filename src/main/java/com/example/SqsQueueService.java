package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import java.util.Optional;

class SqsQueueService implements QueueService {

  private AmazonSQSClient sqs;

  SqsQueueService(AmazonSQSClient sqs) {
    this.sqs = sqs;
  }

  @Override
  public void push(String qUrl, String messageBody) {
    sqs.sendMessage(qUrl, messageBody);
  }

  @Override
  public Optional<Message> pull(String qUrl) {
    return sqs.receiveMessage(qUrl).getMessages().stream().findFirst();
  }

  @Override
  public void delete(String qUrl, String receiptHandler) {
    sqs.deleteMessage(qUrl, receiptHandler);
  }
}
