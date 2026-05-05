package com.kafka.demo.controller;

import com.kafka.demo.model.Message;
import com.kafka.demo.producer.KafkaMessageProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/messages")
public class MessageController {

    private final KafkaMessageProducer producer;

    public MessageController(KafkaMessageProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<Message> publish(@RequestBody Message message) {
        if (message.getId() == null || message.getId().isBlank()) {
            message.setId(UUID.randomUUID().toString());
        }
        producer.send(message);
        return ResponseEntity.accepted().body(message);
    }
}
