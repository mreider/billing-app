package com.example.billingapp.models;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Represents an invoice that aggregates multiple billing records.
 */
public class Invoice {
    private String id;
    private String customerId;
    private double totalAmount;
    private String currency;
    private String status;
    private Instant createdAt;
    private Instant updatedAt;
    private List<String> billingRecordIds;
    private String batchId;
    private String windowId;
    private Map<String, String> metadata;

    // Status constants
    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_PROCESSING = "PROCESSING";
    public static final String STATUS_COMPLETED = "COMPLETED";
    public static final String STATUS_FAILED = "FAILED";

    public Invoice() {
        this.id = UUID.randomUUID().toString();
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
        this.status = STATUS_PENDING;
        this.billingRecordIds = new ArrayList<>();
        this.metadata = new HashMap<>();
    }

    // Constructor with required fields
    public Invoice(String customerId, String currency, String batchId, String windowId) {
        this();
        this.customerId = customerId;
        this.currency = currency;
        this.batchId = batchId;
        this.windowId = windowId;
        this.totalAmount = 0.0;
    }

    // Add a billing record to this invoice
    public void addBillingRecord(BillingRecord billingRecord) {
        if (billingRecord == null) {
            return;
        }
        
        // Ensure the currency matches
        if (!this.currency.equals(billingRecord.getCurrency())) {
            throw new IllegalArgumentException("Currency mismatch: Invoice currency is " + 
                this.currency + " but billing record currency is " + billingRecord.getCurrency());
        }
        
        // Add the billing record ID to the list
        this.billingRecordIds.add(billingRecord.getId());
        
        // Update the total amount
        this.totalAmount += billingRecord.getAmount();
        
        // Update the timestamp
        this.updatedAt = Instant.now();
    }

    // Convert to DynamoDB item
    public Map<String, Object> toDynamoDBItem() {
        Map<String, Object> item = new HashMap<>();
        item.put("id", id);
        item.put("customerId", customerId);
        item.put("totalAmount", totalAmount);
        item.put("currency", currency);
        item.put("status", status);
        item.put("createdAt", createdAt.toString());
        item.put("updatedAt", updatedAt.toString());
        item.put("billingRecordIds", billingRecordIds);
        item.put("batchId", batchId);
        item.put("windowId", windowId);
        
        if (metadata != null && !metadata.isEmpty()) {
            item.put("metadata", metadata);
        }
        
        return item;
    }

    // Create from DynamoDB item
    public static Invoice fromDynamoDBItem(Map<String, Object> item) {
        Invoice invoice = new Invoice();
        
        invoice.setId((String) item.get("id"));
        invoice.setCustomerId((String) item.get("customerId"));
        invoice.setTotalAmount((Double) item.get("totalAmount"));
        invoice.setCurrency((String) item.get("currency"));
        invoice.setStatus((String) item.get("status"));
        invoice.setCreatedAt(Instant.parse((String) item.get("createdAt")));
        invoice.setUpdatedAt(Instant.parse((String) item.get("updatedAt")));
        invoice.setBatchId((String) item.get("batchId"));
        invoice.setWindowId((String) item.get("windowId"));
        
        @SuppressWarnings("unchecked")
        List<String> billingRecordIds = (List<String>) item.get("billingRecordIds");
        invoice.setBillingRecordIds(billingRecordIds);
        
        if (item.containsKey("metadata")) {
            @SuppressWarnings("unchecked")
            Map<String, String> metadata = (Map<String, String>) item.get("metadata");
            invoice.setMetadata(metadata);
        }
        
        return invoice;
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

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
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

    public List<String> getBillingRecordIds() {
        return billingRecordIds;
    }

    public void setBillingRecordIds(List<String> billingRecordIds) {
        this.billingRecordIds = billingRecordIds;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getWindowId() {
        return windowId;
    }

    public void setWindowId(String windowId) {
        this.windowId = windowId;
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
        return "Invoice{" +
                "id='" + id + '\'' +
                ", customerId='" + customerId + '\'' +
                ", totalAmount=" + totalAmount +
                ", currency='" + currency + '\'' +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                ", billingRecordIds=" + billingRecordIds +
                ", batchId='" + batchId + '\'' +
                ", windowId='" + windowId + '\'' +
                ", metadata=" + metadata +
                '}';
    }
}
