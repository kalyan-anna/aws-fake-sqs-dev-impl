package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import static org.apache.commons.lang3.StringUtils.*;
import java.util.Optional;

class SqsQueueService implements QueueService {

  private AmazonSQSClient sqs;

  SqsQueueService(AmazonSQSClient sqs) {
    this.sqs = sqs;
  }

  @Override
  public void push(String qUrl, String messageBody) {
    if(isBlank(qUrl) || isBlank(messageBody)) {
      throw new IllegalArgumentException("Invalid qUrl or messageBody");
    }

    sqs.sendMessage(qUrl, messageBody);
  }

  @Override
  public Optional<Message> pull(String qUrl) {
    if(isBlank(qUrl)) {
      throw new IllegalArgumentException("Invalid qUrl");
    }

    return sqs.receiveMessage(qUrl).getMessages().stream().findFirst();
  }

  @Override
  public void delete(String qUrl, String receiptHandler) {
    if(isBlank(qUrl) || isBlank(receiptHandler)) {
      throw new IllegalArgumentException("Invalid qUrl or receiptHandler");
    }

    sqs.deleteMessage(qUrl, receiptHandler);
  }
}
