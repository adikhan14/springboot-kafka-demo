# Spring Boot Kafka Demo

A Spring Boot application demonstrating Kafka producer/consumer integration with a 3-node KRaft cluster.

---

## Tech Stack

- Java 17
- Spring Boot 4.0.6
- Spring Kafka 4.0.5
- Apache Kafka (KRaft mode, 3-broker cluster)
- Docker & Docker Compose
- Kafka UI

---

## Project Structure

```
springboot-kafka-demo/
├── docker/
│   └── docker-compose.yml                  # 3-broker Kafka cluster + Kafka UI
├── postman collection/
│   └── springboot-kafka-demo.postman_collection.json
├── src/main/java/com/kafka/demo/
│   ├── config/
│   │   ├── KafkaProducerConfig.java        # Producer factory & KafkaTemplate
│   │   ├── KafkaConsumerConfig.java        # Consumer factory & listener container
│   │   └── KafkaTopicConfig.java           # KafkaAdmin & topic creation
│   ├── controller/
│   │   └── MessageController.java          # REST endpoint
│   ├── producer/
│   │   └── KafkaMessageProducer.java       # Sends messages to Kafka
│   ├── consumer/
│   │   └── KafkaMessageConsumer.java       # Listens to messages from Kafka
│   ├── serializer/
│   │   ├── MessageSerializer.java          # Custom Kafka serializer (Jackson)
│   │   └── MessageDeserializer.java        # Custom Kafka deserializer (Jackson)
│   └── model/
│       └── Message.java                    # Message payload model
└── src/main/resources/
    └── application.yml
```

---

## Kafka Cluster

The Docker Compose setup runs a **3-node Kafka cluster in KRaft mode** (no ZooKeeper).

| Service    | Internal (Docker) | External (Host) |
|------------|-------------------|-----------------|
| kafka1     | kafka1:29092      | localhost:9092  |
| kafka2     | kafka2:29092      | localhost:9094  |
| kafka3     | kafka3:29092      | localhost:9095  |
| Kafka UI   | —                 | localhost:8081  |

### Start the cluster

```bash
cd docker
docker compose up -d
```

### Stop the cluster

```bash
docker compose down
```

---

## Topic

`demo-topic` is automatically created by `KafkaTopicConfig` on application startup.

| Property           | Value      |
|--------------------|------------|
| Name               | demo-topic |
| Partitions         | 3          |
| Replication Factor | 3          |

---

## Configuration

All Kafka settings are in `src/main/resources/application.yml`:

```yaml
logging:
  level:
    org.apache.kafka.clients.producer.internals.TransactionManager: DEBUG
    org.springframework.kafka.core.KafkaTemplate: DEBUG

kafka:
  bootstrap-servers: localhost:9092,localhost:9094,localhost:9095
  producer:
    acks: all
    retries: 3
    enable-idempotence: true
    transaction-id-prefix: demo-tx-
  consumer:
    group-id: demo-group
    auto-offset-reset: earliest
    isolation-level: read_committed
  topic:
    demo:
      name: demo-topic
      partitions: 3
      replication-factor: 3
```

---

## Running Multiple Instances

IntelliJ run configurations are pre-configured under `.idea/runConfigurations/`:

| Configuration  | Port | Role     |
|----------------|------|----------|
| Instance-8080  | 8080 | Producer |
| Instance-8082  | 8082 | Consumer |
| Instance-8083  | 8083 | Consumer |
| Instance-8084  | 8084 | Consumer |
| Instance-8085  | 8085 | Consumer |

Each instance runs the same application. All consumer instances share `demo-group`, so Kafka distributes the 3 partitions across them.

To run via Maven on a custom port:

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments=--server.port=8082
```

---

## API

### Publish a Message

```
POST /api/messages
Content-Type: application/json
```

**Request body with custom ID:**
```json
{
  "id": "msg-001",
  "content": "Hello Kafka!"
}
```

**Request body with auto-generated ID:**
```json
{
  "content": "Hello Kafka!"
}
```

**Response (202 Accepted):**
```json
{
  "id": "msg-001",
  "content": "Hello Kafka!",
  "timestamp": "2026-05-05T03:50:00"
}
```

Import `postman collection/springboot-kafka-demo.postman_collection.json` into Postman for ready-made requests.

---

## Exactly-Once Semantics

The application is configured for exactly-once delivery end to end.

**Producer — Transactions + Idempotence**

| Setting | Value |
|---|---|
| `enable.idempotence` | `true` |
| `transaction-id-prefix` | `demo-tx-` |
| `acks` | `all` |

Each send is wrapped in `kafkaTemplate.executeInTransaction()`. Spring Kafka assigns actual transaction IDs as `demo-tx-0`, `demo-tx-1`, etc. per producer instance. If the send fails, the transaction is aborted automatically.

**Consumer — Read Committed**

| Setting | Value |
|---|---|
| `isolation.level` | `read_committed` |

The consumer only reads messages from **committed** transactions — aborted or in-flight transactional messages are invisible to it.

---

## Offset Management

The consumer uses **manual offset committing** — offsets are only committed after a message is successfully processed.

| Setting | Value |
|---|---|
| `enable.auto.commit` | `false` |
| Ack Mode | `MANUAL` |

`ack.acknowledge()` is called explicitly in the listener after processing. If an exception occurs before that point, the offset is not committed and the message will be redelivered.

---

## Debug Logging

Transaction begin/commit logs are enabled via targeted DEBUG loggers in `application.yml`:

| Logger | What it shows |
|---|---|
| `TransactionManager` | `beginTransaction`, `commitTransaction`, `abortTransaction`, producer epoch |
| `KafkaTemplate` | `executeInTransaction` entry/exit, send calls within the transaction |

---

## Kafka UI

Access the Kafka UI at [http://localhost:8081](http://localhost:8081) to monitor topics, partitions, consumer groups, and messages.
