package org.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Order {
    @JsonProperty("id")
    private int id;
    @JsonProperty("client")
    private String client;
    @JsonProperty("total")
    private double total;
    @JsonProperty("status")
    private String status;
    @JsonProperty("timestamp")
    private long timestamp;

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    public String getClient() { return client; }
    public void setClient(String client) { this.client = client; }

    public double getTotal() { return total; }
    public void setTotal(double total) { this.total = total; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
