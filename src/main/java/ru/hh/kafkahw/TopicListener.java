package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;
  private final Set<String> cashTopic1 = ConcurrentHashMap.newKeySet();
  private final Set<String> cashTopic3 = ConcurrentHashMap.newKeySet();

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    if (!cashTopic1.contains(consumerRecord.timestamp() + consumerRecord.value())) {
      cashTopic1.add(consumerRecord.timestamp() + consumerRecord.value());
      LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
      service.handle("topic1", consumerRecord.value());
      ack.acknowledge();
    }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    for (int i = 0; i < 10; i++) {
      try {
        LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
        service.handle("topic2", consumerRecord.value());
      } catch (RuntimeException e) {
      }
    }
    ack.acknowledge();
  }

  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    while (!cashTopic3.contains(consumerRecord.timestamp() + consumerRecord.value())) {
      try {
        LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
        service.handle("topic3", consumerRecord.value());
        cashTopic3.add(consumerRecord.timestamp() + consumerRecord.value());
      } catch (RuntimeException e) {
      }
    }
    ack.acknowledge();
  }
}
