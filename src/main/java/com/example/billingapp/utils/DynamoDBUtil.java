package com.example.billingapp.utils;

import com.example.billingapp.models.BillingRecord;
import com.example.billingapp.models.Invoice;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for DynamoDB operations.
 */
public class DynamoDBUtil {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBUtil.class);
    // Table name prefix for feature flagging
    private static final String TABLE_PREFIX = Boolean.parseBoolean(System.getenv("OTEL_ENABLED")) ? "Otel" : "";
    
    private static final String BILLING_TABLE_NAME = System.getenv("BILLING_TABLE_NAME");
    private static final String INVOICE_TABLE_NAME = System.getenv("INVOICE_TABLE_NAME");
    
    // Get the actual table names with prefix
    private static final String ACTUAL_BILLING_TABLE_NAME = TABLE_PREFIX + BILLING_TABLE_NAME;
    private static final String ACTUAL_INVOICE_TABLE_NAME = TABLE_PREFIX + INVOICE_TABLE_NAME;
    
    private static DynamoDbClient dynamoDbClient;
    
    // Get the DynamoDB client (singleton pattern)
    public static synchronized DynamoDbClient getDynamoDbClient() {
        if (dynamoDbClient == null) {
            // Get the region from environment variable or use a default
            String regionName = Optional.ofNullable(System.getenv("AWS_REGION"))
                    .orElse("us-east-1");
            
            // Create the client builder
            DynamoDbClientBuilder builder = DynamoDbClient.builder()
                    .region(Region.of(regionName));
            
            // Instrument with OpenTelemetry if enabled
            builder = OpenTelemetryUtil.instrumentDynamoDbClientBuilder(builder);
            
            // Build the client
            dynamoDbClient = builder.build();
            
            logger.info("DynamoDB client initialized with table prefix: '{}', billing table: {}, invoice table: {}", 
                    TABLE_PREFIX, ACTUAL_BILLING_TABLE_NAME, ACTUAL_INVOICE_TABLE_NAME);
        }
        return dynamoDbClient;
    }
    
    /**
     * Save a billing record to DynamoDB.
     * 
     * @param billingRecord The billing record to save
     * @return true if successful, false otherwise
     */
    public static boolean saveBillingRecord(BillingRecord billingRecord) {
        return OpenTelemetryUtil.runWithSpan("DynamoDB.PutItem.BillingRecord", () -> {
            try {
                Map<String, AttributeValue> item = convertToAttributeValues(billingRecord.toDynamoDBItem());
                
                // Add OpenTelemetry attributes
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("db.system", "dynamodb");
                    OpenTelemetryUtil.addAttribute("db.name", ACTUAL_BILLING_TABLE_NAME);
                    OpenTelemetryUtil.addAttribute("db.operation", "PutItem");
                    OpenTelemetryUtil.addAttribute("billing.record.id", billingRecord.getId());
                }
                
                PutItemRequest request = PutItemRequest.builder()
                        .tableName(ACTUAL_BILLING_TABLE_NAME)
                        .item(item)
                        .build();
                
                getDynamoDbClient().putItem(request);
                logger.info("Successfully saved billing record: {}", billingRecord.getId());
                return true;
            } catch (SdkException e) {
                logger.error("Error saving billing record: {}", e.getMessage(), e);
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.recordException(e);
                }
                return false;
            }
        });
    }
    
    /**
     * Get a billing record from DynamoDB by ID.
     * 
     * @param id The ID of the billing record
     * @return The billing record, or null if not found
     */
    public static BillingRecord getBillingRecord(String id) {
        return OpenTelemetryUtil.runWithSpan("DynamoDB.GetItem.BillingRecord", () -> {
            try {
                // Add OpenTelemetry attributes
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("db.system", "dynamodb");
                    OpenTelemetryUtil.addAttribute("db.name", ACTUAL_BILLING_TABLE_NAME);
                    OpenTelemetryUtil.addAttribute("db.operation", "GetItem");
                    OpenTelemetryUtil.addAttribute("billing.record.id", id);
                }
                
                Map<String, AttributeValue> key = new HashMap<>();
                key.put("id", AttributeValue.builder().s(id).build());
                
                GetItemRequest request = GetItemRequest.builder()
                        .tableName(ACTUAL_BILLING_TABLE_NAME)
                        .key(key)
                        .build();
                
                GetItemResponse response = getDynamoDbClient().getItem(request);
                
                if (response.hasItem()) {
                    Map<String, Object> item = convertFromAttributeValues(response.item());
                    return BillingRecord.fromDynamoDBItem(item);
                } else {
                    logger.warn("Billing record not found: {}", id);
                    return null;
                }
            } catch (SdkException e) {
                logger.error("Error getting billing record: {}", e.getMessage(), e);
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.recordException(e);
                }
                return null;
            }
        });
    }
    
    /**
     * Update a billing record in DynamoDB.
     * 
     * @param billingRecord The billing record to update
     * @return true if successful, false otherwise
     */
    public static boolean updateBillingRecord(BillingRecord billingRecord) {
        return saveBillingRecord(billingRecord); // Uses the same method as save
    }
    
    /**
     * Save an invoice to DynamoDB.
     * 
     * @param invoice The invoice to save
     * @return true if successful, false otherwise
     */
    public static boolean saveInvoice(Invoice invoice) {
        return OpenTelemetryUtil.runWithSpan("DynamoDB.PutItem.Invoice", () -> {
            try {
                Map<String, AttributeValue> item = convertToAttributeValues(invoice.toDynamoDBItem());
                
                // Add OpenTelemetry attributes
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("db.system", "dynamodb");
                    OpenTelemetryUtil.addAttribute("db.name", ACTUAL_INVOICE_TABLE_NAME);
                    OpenTelemetryUtil.addAttribute("db.operation", "PutItem");
                    OpenTelemetryUtil.addAttribute("invoice.id", invoice.getId());
                    OpenTelemetryUtil.addAttribute("invoice.customer_id", invoice.getCustomerId());
                    OpenTelemetryUtil.addAttribute("invoice.billing_records_count", invoice.getBillingRecordIds().size());
                }
                
                PutItemRequest request = PutItemRequest.builder()
                        .tableName(ACTUAL_INVOICE_TABLE_NAME)
                        .item(item)
                        .build();
                
                getDynamoDbClient().putItem(request);
                logger.info("Successfully saved invoice: {}", invoice.getId());
                return true;
            } catch (SdkException e) {
                logger.error("Error saving invoice: {}", e.getMessage(), e);
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.recordException(e);
                }
                return false;
            }
        });
    }
    
    /**
     * Get an invoice from DynamoDB by ID.
     * 
     * @param id The ID of the invoice
     * @return The invoice, or null if not found
     */
    public static Invoice getInvoice(String id) {
        return OpenTelemetryUtil.runWithSpan("DynamoDB.GetItem.Invoice", () -> {
            try {
                // Add OpenTelemetry attributes
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("db.system", "dynamodb");
                    OpenTelemetryUtil.addAttribute("db.name", ACTUAL_INVOICE_TABLE_NAME);
                    OpenTelemetryUtil.addAttribute("db.operation", "GetItem");
                    OpenTelemetryUtil.addAttribute("invoice.id", id);
                }
                
                Map<String, AttributeValue> key = new HashMap<>();
                key.put("id", AttributeValue.builder().s(id).build());
                
                GetItemRequest request = GetItemRequest.builder()
                        .tableName(ACTUAL_INVOICE_TABLE_NAME)
                        .key(key)
                        .build();
                
                GetItemResponse response = getDynamoDbClient().getItem(request);
                
                if (response.hasItem()) {
                    Map<String, Object> item = convertFromAttributeValues(response.item());
                    return Invoice.fromDynamoDBItem(item);
                } else {
                    logger.warn("Invoice not found: {}", id);
                    return null;
                }
            } catch (SdkException e) {
                logger.error("Error getting invoice: {}", e.getMessage(), e);
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.recordException(e);
                }
                return null;
            }
        });
    }
    
    /**
     * Update an invoice in DynamoDB.
     * 
     * @param invoice The invoice to update
     * @return true if successful, false otherwise
     */
    public static boolean updateInvoice(Invoice invoice) {
        return saveInvoice(invoice); // Uses the same method as save
    }
    
    /**
     * Convert a map of objects to a map of AttributeValue objects for DynamoDB.
     * 
     * @param item The map of objects
     * @return The map of AttributeValue objects
     */
    @SuppressWarnings("unchecked")
    private static Map<String, AttributeValue> convertToAttributeValues(Map<String, Object> item) {
        Map<String, AttributeValue> attributeValues = new HashMap<>();
        
        for (Map.Entry<String, Object> entry : item.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            if (value == null) {
                continue;
            }
            
            if (value instanceof String) {
                attributeValues.put(key, AttributeValue.builder().s((String) value).build());
            } else if (value instanceof Number) {
                attributeValues.put(key, AttributeValue.builder().n(value.toString()).build());
            } else if (value instanceof Boolean) {
                attributeValues.put(key, AttributeValue.builder().bool((Boolean) value).build());
            } else if (value instanceof Map) {
                Map<String, AttributeValue> mapValue = convertToAttributeValues((Map<String, Object>) value);
                attributeValues.put(key, AttributeValue.builder().m(mapValue).build());
            } else if (value instanceof Iterable) {
                // Handle lists - assuming all elements are strings for simplicity
                // In a real application, you would need to handle different types of list elements
                attributeValues.put(key, AttributeValue.builder()
                        .l(((Iterable<?>) value).stream()
                                .map(Object::toString)
                                .map(s -> AttributeValue.builder().s(s).build())
                                .toList())
                        .build());
            }
        }
        
        return attributeValues;
    }
    
    /**
     * Convert a map of AttributeValue objects from DynamoDB to a map of objects.
     * 
     * @param item The map of AttributeValue objects
     * @return The map of objects
     */
    private static Map<String, Object> convertFromAttributeValues(Map<String, AttributeValue> item) {
        Map<String, Object> result = new HashMap<>();
        
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            String key = entry.getKey();
            AttributeValue value = entry.getValue();
            
            if (value.s() != null) {
                result.put(key, value.s());
            } else if (value.n() != null) {
                // Try to parse as integer first, then as double
                try {
                    result.put(key, Integer.parseInt(value.n()));
                } catch (NumberFormatException e) {
                    result.put(key, Double.parseDouble(value.n()));
                }
            } else if (value.bool() != null) {
                result.put(key, value.bool());
            } else if (value.m() != null) {
                result.put(key, convertFromAttributeValues(value.m()));
            } else if (value.l() != null) {
                // Handle lists - assuming all elements are strings for simplicity
                result.put(key, value.l().stream()
                        .map(AttributeValue::s)
                        .toList());
            }
        }
        
        return result;
    }
}
