package com.kafka.demo.serializer;

import com.kafka.demo.model.Order;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class OrderSerde implements Serde<Order> {

    private final OrderSerializer serializer = new OrderSerializer();
    private final OrderDeserializer deserializer = new OrderDeserializer();

    @Override
    public Serializer<Order> serializer() { return serializer; }

    @Override
    public Deserializer<Order> deserializer() { return deserializer; }
}
