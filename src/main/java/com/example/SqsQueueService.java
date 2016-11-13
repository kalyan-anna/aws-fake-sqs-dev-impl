package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.example.model.CanvaMessage;
import static org.apache.commons.lang.StringUtils.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SqsQueueService implements QueueService {

  private Map<String, String> qNameToUrlMap = new HashMap<>();
  private AmazonSQSClient sqs;

  public SqsQueueService(AmazonSQSClient sqs) {
    this.sqs = sqs;
  }

  @Override
  public void push(String qName, String messageBody) {
    if(isBlank(qName) || isBlank(messageBody)) {
      throw new IllegalArgumentException("Invalid qName or messageBody");
    }

    String qUrl = qNameToUrlMap.get(qName);
    if(qUrl == null) {
      qUrl = createNewQueue(qName);
    }

    sqs.sendMessage(qUrl, messageBody);
  }

  private String createNewQueue(String qName) {
    String qUrl = sqs.createQueue(qName).getQueueUrl();
    qNameToUrlMap.put(qName, qUrl);
    return qUrl;
  }

  @Override
  public Optional<CanvaMessage> pull(String qName) {
    if(isBlank(qName)) {
      throw new IllegalArgumentException("Invalid qName");
    }

    String  qUrl = findQueueUrlOrThrowException(qName);

    List<Message> messages = sqs.receiveMessage(qUrl).getMessages();
    return messages.stream().findFirst().map(msg -> new CanvaMessage(msg.getMessageId(), msg.getReceiptHandle(), msg.getBody()));
  }

  private String findQueueUrlOrThrowException(String qName) {
    String qUrl = qNameToUrlMap.get(qName);
    if(qUrl != null) {
      return qUrl;
    }

    Optional<String> mayBeQUrl = findQueueUrlInSqs(qName);
    if(mayBeQUrl.isPresent()) {
      qNameToUrlMap.put(qName, mayBeQUrl.get());
      return mayBeQUrl.get();
    } else {
      throw new IllegalArgumentException("Couldn't find QueueUrl. Invalid qName " + qName);
    }
  }

  public Optional<String> findQueueUrlInSqs(String qName) {
    return sqs.listQueues(qName).getQueueUrls().stream().filter(url -> url.endsWith(qName)).findFirst();
  }

  @Override
  public void delete(String qName, String receiptHandler) {
    String qUrl = qNameToUrlMap.get(qName);
    sqs.deleteMessage(qUrl, receiptHandler);
  }
}
