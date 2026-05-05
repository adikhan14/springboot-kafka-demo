package com.kafka.demo.consumer;

import com.kafka.demo.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class KafkaOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaOrderConsumer.class);

    @KafkaListener(topics = "${kafka.topic.demo.name}")
    public void consume(@Payload Order order,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset,
                        Acknowledgment ack) {
        log.info("Received order [{}] from partition {} at offset {} | item: {}, qty: {}",
                order.getOrderId(), partition, offset, order.getItemName(), order.getQuantity());
        ack.acknowledge();
    }
}
