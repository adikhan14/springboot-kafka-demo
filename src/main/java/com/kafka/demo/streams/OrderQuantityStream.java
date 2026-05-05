package com.kafka.demo.streams;

import com.kafka.demo.model.Order;
import com.kafka.demo.serializer.OrderSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class OrderQuantityStream {

    private static final Logger log = LoggerFactory.getLogger(OrderQuantityStream.class);

    @Value("${kafka.topic.demo.name}")
    private String topic;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        StoreBuilder<KeyValueStore<String, String>> dedupeStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("order-dedupe-store"),
                        Serdes.String(),
                        Serdes.String()
                );
        builder.addStateStore(dedupeStore);

        KStream<String, Order> orders = builder.stream(
                topic, Consumed.with(Serdes.String(), new OrderSerde()));

        KStream<String, Order> dedupedOrders = orders.process(
                () -> new ContextualProcessor<String, Order, String, Order>() {
                    private KeyValueStore<String, String> store;

                    @Override
                    public void init(ProcessorContext<String, Order> context) {
                        super.init(context);
                        store = context.getStateStore("order-dedupe-store");
                    }

                    @Override
                    public void process(Record<String, Order> record) {
                        Order order = record.value();
                        if (order == null) return;
                        String fingerprint = order.getItemName() + ":" + order.getQuantity();
                        String existing = store.get(order.getOrderId());
                        if (existing == null || !existing.equals(fingerprint)) {
                            store.put(order.getOrderId(), fingerprint);
                            context().forward(record);
                        } else {
                            log.info("[Streams] Duplicate ignored [{}]: item={}, qty={}",
                                    order.getOrderId(), order.getItemName(), order.getQuantity());
                        }
                    }
                }, "order-dedupe-store");

        dedupedOrders
                .groupBy((key, order) -> order.getItemName(),
                        Grouped.with(Serdes.String(), new OrderSerde()))
                .aggregate(
                        () -> 0L,
                        (itemName, order, total) -> total + order.getQuantity(),
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("item-quantity-store")
                                .withValueSerde(Serdes.Long())
                )
                .toStream()
                .foreach((itemName, totalQuantity) ->
                        log.info("[Streams] Item [{}] running total quantity: {}", itemName, totalQuantity));
    }
}
