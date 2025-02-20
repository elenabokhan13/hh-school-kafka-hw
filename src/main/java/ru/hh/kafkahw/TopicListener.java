package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;
  private final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> counters = new ConcurrentHashMap<>();

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    if (!counters.getOrDefault("topic1", new ConcurrentHashMap<>()).containsKey(consumerRecord.value())) {
      counters.computeIfAbsent(consumerRecord.topic(), key -> new ConcurrentHashMap<>())
          .computeIfAbsent(consumerRecord.value(), key -> new AtomicInteger(0)).incrementAndGet();
      LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
      service.handle("topic1", consumerRecord.value());
      ack.acknowledge();
    }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    try {
      LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
      service.handle("topic2", consumerRecord.value());
    } catch (RuntimeException e) {
      atLeastOnce(consumerRecord, ack);
    }
    ack.acknowledge();
  }

  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    if (!counters.getOrDefault("topic3", new ConcurrentHashMap<>()).containsKey(consumerRecord.value())) {
      try {
        LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
        service.handle("topic3", consumerRecord.value());
        counters.computeIfAbsent(consumerRecord.topic(), key -> new ConcurrentHashMap<>())
            .computeIfAbsent(consumerRecord.value(), key -> new AtomicInteger(0)).incrementAndGet();
      } catch (RuntimeException e) {
        if (Objects.equals(e.getMessage(), "Error after saving")) {
          counters.computeIfAbsent(consumerRecord.topic(), key -> new ConcurrentHashMap<>())
              .computeIfAbsent(consumerRecord.value(), key -> new AtomicInteger(0)).incrementAndGet();
          ack.acknowledge();
        } else {
          exactlyOnce(consumerRecord, ack);
        }
      }
      ack.acknowledge();

    }
  }
}
