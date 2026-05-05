package com.kafka.demo.producer;

import com.kafka.demo.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaOrderProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaOrderProducer.class);

    private final KafkaTemplate<String, Order> kafkaTemplate;

    @Value("${kafka.topic.demo.name}")
    private String topic;

    public KafkaOrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Order order) {
        kafkaTemplate.executeInTransaction(kt -> {
            CompletableFuture<SendResult<String, Order>> future =
                    kt.send(topic, order.getOrderId(), order);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send order [{}]: {}", order.getOrderId(), ex.getMessage());
                } else {
                    log.info("Sent order [{}] to partition {} at offset {}",
                            order.getOrderId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });
            return null;
        });
    }
}
