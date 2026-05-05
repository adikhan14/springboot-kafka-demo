package com.kafka.demo.producer;

import com.kafka.demo.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private final KafkaTemplate<String, Message> kafkaTemplate;

    @Value("${kafka.topic.demo.name}")
    private String topic;

    public KafkaMessageProducer(KafkaTemplate<String, Message> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Message message) {
        kafkaTemplate.executeInTransaction(kt -> {
            CompletableFuture<SendResult<String, Message>> future =
                    kt.send(topic, message.getId(), message);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send message [{}]: {}", message.getId(), ex.getMessage());
                } else {
                    log.info("Sent message [{}] to partition {} at offset {}",
                            message.getId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });
            return null;
        });
    }
}
