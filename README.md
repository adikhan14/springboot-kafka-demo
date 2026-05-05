# Spring Boot Kafka Demo

A Spring Boot application demonstrating Kafka producer/consumer integration with a 3-node KRaft cluster, plus Kafka Streams for real-time order quantity aggregation.

---

## Tech Stack

- Java 17
- Spring Boot 4.0.6
- Spring Kafka 4.0.5
- Apache Kafka (KRaft mode, 3-broker cluster)
- Kafka Streams
- Docker & Docker Compose
- Kafka UI

---

## Project Structure

```
springboot-kafka-demo/
├── docker/
│   └── docker-compose.yml                      # 3-broker Kafka cluster + Kafka UI
├── postman collection/
│   └── springboot-kafka-demo.postman_collection.json
├── src/main/java/com/kafka/demo/
│   ├── config/
│   │   ├── KafkaProducerConfig.java            # Producer factory & KafkaTemplate
│   │   ├── KafkaConsumerConfig.java            # Consumer factory & listener container
│   │   ├── KafkaStreamsConfig.java             # Kafka Streams configuration
│   │   └── KafkaTopicConfig.java              # KafkaAdmin & topic creation
│   ├── controller/
│   │   └── OrderController.java               # REST endpoint
│   ├── producer/
│   │   └── KafkaOrderProducer.java            # Sends orders to Kafka
│   ├── consumer/
│   │   └── KafkaOrderConsumer.java            # Listens to orders from Kafka
│   ├── streams/
│   │   └── OrderQuantityStream.java           # Kafka Streams topology (quantity per item)
│   ├── serializer/
│   │   ├── OrderSerializer.java               # Custom Kafka serializer (Jackson)
│   │   ├── OrderDeserializer.java             # Custom Kafka deserializer (Jackson)
│   │   └── OrderSerde.java                    # Serde wrapping serializer + deserializer
│   └── model/
│       └── Order.java                         # Order payload model
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
  streams:
    application-id: order-quantity-streams
  topic:
    demo:
      name: demo-topic
      partitions: 3
      replication-factor: 3
```

---

## Running Multiple Instances

IntelliJ run configurations are pre-configured under `.idea/runConfigurations/`:

| Configuration  | Port |
|----------------|------|
| Instance-8080  | 8080 |
| Instance-8082  | 8082 |
| Instance-8083  | 8083 |
| Instance-8084  | 8084 |
| Instance-8085  | 8085 |

Each instance has producer, consumer, and Kafka Streams functionality. All instances share `demo-group` for the regular consumer, so Kafka distributes the 3 partitions across running instances. Each instance also runs an independent Kafka Streams pipeline under `order-quantity-streams`.

To run via Maven on a custom port:

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments=--server.port=8082
```

---

## API

### Publish an Order

```
POST /api/orders
Content-Type: application/json
```

**Request body with custom ID:**
```json
{
  "orderId": "ord-001",
  "itemName": "Laptop",
  "quantity": 2
}
```

**Request body with auto-generated ID:**
```json
{
  "itemName": "Laptop",
  "quantity": 2
}
```

**Response (202 Accepted):**
```json
{
  "orderId": "ord-001",
  "itemName": "Laptop",
  "quantity": 2
}
```

Import `postman collection/springboot-kafka-demo.postman_collection.json` into Postman for ready-made requests.

---

## Kafka Streams — Order Quantity Aggregation

`OrderQuantityStream` builds a topology that reads every order from `demo-topic` and maintains a **running total quantity per item name**.

```
demo-topic
    └── KStream<orderId, Order>
            └── groupBy(itemName)
                    └── aggregate(sum of quantity)
                            └── KTable<itemName, Long>
                                    └── toStream → log
```

Each time a new order arrives, the log prints the updated running total for that item:

```
[Streams] Item [Laptop] running total quantity: 5
```

The aggregated state is stored in a local state store named `item-quantity-store`.

**Consumer groups in play:**

| Component         | Consumer Group          | Purpose                         |
|-------------------|-------------------------|---------------------------------|
| KafkaOrderConsumer | demo-group             | Per-message logging             |
| OrderQuantityStream | order-quantity-streams | Running quantity sum per item   |

Both consume from `demo-topic` independently — every order is processed by both.

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
