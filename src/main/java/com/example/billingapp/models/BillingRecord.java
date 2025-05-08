package com.example.billingapp.models;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a billing record stored in DynamoDB.
 */
public class BillingRecord {
    private String id;
    private String customerId;
    private String productId;
    private double amount;
    private String currency;
    private String status;
    private Instant createdAt;
    private Instant updatedAt;
    private int retryCount;
    private String errorMessage;
    private Map<String, String> metadata;

    // Status constants
    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_PROCESSING = "PROCESSING";
    public static final String STATUS_COMPLETED = "COMPLETED";
    public static final String STATUS_FAILED = "FAILED";

    public BillingRecord() {
        this.id = UUID.randomUUID().toString();
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
        this.retryCount = 0;
        this.status = STATUS_PENDING;
        this.metadata = new HashMap<>();
    }

    // Constructor with required fields
    public BillingRecord(String customerId, String productId, double amount, String currency) {
        this();
        this.customerId = customerId;
        this.productId = productId;
        this.amount = amount;
        this.currency = currency;
    }

    // Convert to DynamoDB item
    public Map<String, Object> toDynamoDBItem() {
        Map<String, Object> item = new HashMap<>();
        item.put("id", id);
        item.put("customerId", customerId);
        item.put("productId", productId);
        item.put("amount", amount);
        item.put("currency", currency);
        item.put("status", status);
        item.put("createdAt", createdAt.toString());
        item.put("updatedAt", updatedAt.toString());
        item.put("retryCount", retryCount);
        
        if (errorMessage != null) {
            item.put("errorMessage", errorMessage);
        }
        
        if (metadata != null && !metadata.isEmpty()) {
            item.put("metadata", metadata);
        }
        
        return item;
    }

    // Create from DynamoDB item
    public static BillingRecord fromDynamoDBItem(Map<String, Object> item) {
        BillingRecord record = new BillingRecord();
        
        record.setId((String) item.get("id"));
        record.setCustomerId((String) item.get("customerId"));
        record.setProductId((String) item.get("productId"));
        record.setAmount((Double) item.get("amount"));
        record.setCurrency((String) item.get("currency"));
        record.setStatus((String) item.get("status"));
        record.setCreatedAt(Instant.parse((String) item.get("createdAt")));
        record.setUpdatedAt(Instant.parse((String) item.get("updatedAt")));
        record.setRetryCount((Integer) item.get("retryCount"));
        
        if (item.containsKey("errorMessage")) {
            record.setErrorMessage((String) item.get("errorMessage"));
        }
        
        if (item.containsKey("metadata")) {
            @SuppressWarnings("unchecked")
            Map<String, String> metadata = (Map<String, String>) item.get("metadata");
            record.setMetadata(metadata);
        }
        
        return record;
    }

    // Getters and setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        this.updatedAt = Instant.now();
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public void incrementRetryCount() {
        this.retryCount++;
        this.updatedAt = Instant.now();
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        this.updatedAt = Instant.now();
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public void addMetadata(String key, String value) {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
        }
        this.metadata.put(key, value);
    }

    @Override
    public String toString() {
        return "BillingRecord{" +
                "id='" + id + '\'' +
                ", customerId='" + customerId + '\'' +
                ", productId='" + productId + '\'' +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                ", retryCount=" + retryCount +
                ", errorMessage='" + errorMessage + '\'' +
                ", metadata=" + metadata +
                '}';
    }
}
