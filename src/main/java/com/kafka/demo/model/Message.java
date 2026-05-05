package com.kafka.demo.model;

import java.time.LocalDateTime;

public class Message {

    private String id;
    private String content;
    private LocalDateTime timestamp;

    public Message() {}

    public Message(String id, String content) {
        this.id = id;
        this.content = content;
        this.timestamp = LocalDateTime.now();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }

    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "Message{id='" + id + "', content='" + content + "', timestamp=" + timestamp + "}";
    }
}
