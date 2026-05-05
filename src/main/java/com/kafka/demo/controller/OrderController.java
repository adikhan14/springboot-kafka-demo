package com.kafka.demo.controller;

import com.kafka.demo.model.Order;
import com.kafka.demo.producer.KafkaOrderProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final KafkaOrderProducer producer;

    public OrderController(KafkaOrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<Order> publish(@RequestBody Order order) {
        if (order.getOrderId() == null || order.getOrderId().isBlank()) {
            order.setOrderId(UUID.randomUUID().toString());
        }
        producer.send(order);
        return ResponseEntity.accepted().body(order);
    }
}
