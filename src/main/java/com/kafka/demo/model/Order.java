package com.kafka.demo.model;

public class Order {

    private String orderId;
    private String itemName;
    private int quantity;

    public Order() {}

    public Order(String orderId, String itemName, int quantity) {
        this.orderId = orderId;
        this.itemName = itemName;
        this.quantity = quantity;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getItemName() { return itemName; }
    public void setItemName(String itemName) { this.itemName = itemName; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    @Override
    public String toString() {
        return "Order{orderId='" + orderId + "', itemName='" + itemName + "', quantity=" + quantity + "}";
    }
}
