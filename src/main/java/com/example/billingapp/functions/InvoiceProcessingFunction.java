package com.example.billingapp.functions;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.example.billingapp.models.BillingRecord;
import com.example.billingapp.models.Invoice;
import com.example.billingapp.utils.DynamoDBUtil;
import com.example.billingapp.utils.SQSUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Lambda function that processes invoices in configurable batch sizes,
 * implements a windowing mechanism to group related invoices,
 * uses SQS batch processing capabilities, and stores final aggregated data in DynamoDB.
 */
public class InvoiceProcessingFunction implements RequestHandler<Map<String, Object>, Map<String, Object>> {
    private static final Logger logger = LoggerFactory.getLogger(InvoiceProcessingFunction.class);
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    // Default configuration values
    private static final int DEFAULT_BATCH_SIZE = 10;
    private static final int DEFAULT_WINDOW_SIZE_MINUTES = 5;
    private static final int DEFAULT_TIMEOUT_SECONDS = 60;
    
    // Environment variables
    private static final String INVOICE_QUEUE_URL = System.getenv("INVOICE_QUEUE_URL");
    
    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        logger.info("Processing invoice request: {}", gson.toJson(event));
        
        // Parse configuration from the event
        int batchSize = getIntParam(event, "batchSize", DEFAULT_BATCH_SIZE);
        int windowSizeMinutes = getIntParam(event, "windowSizeMinutes", DEFAULT_WINDOW_SIZE_MINUTES);
        int timeoutSeconds = getIntParam(event, "timeoutSeconds", DEFAULT_TIMEOUT_SECONDS);
        
        logger.info("Invoice processing configuration: batchSize={}, windowSizeMinutes={}, timeoutSeconds={}",
                batchSize, windowSizeMinutes, timeoutSeconds);
        
        // Validate the invoice queue URL
        if (INVOICE_QUEUE_URL == null || INVOICE_QUEUE_URL.isEmpty()) {
            String errorMessage = "INVOICE_QUEUE_URL environment variable is not set";
            logger.error(errorMessage);
            return createErrorResponse(errorMessage);
        }
        
        try {
            // Process invoices
            Map<String, Object> result = processInvoices(batchSize, windowSizeMinutes, timeoutSeconds);
            logger.info("Invoice processing completed successfully: {}", gson.toJson(result));
            return result;
        } catch (Exception e) {
            String errorMessage = "Error processing invoices: " + e.getMessage();
            logger.error(errorMessage, e);
            return createErrorResponse(errorMessage);
        }
    }
    
    /**
     * Process invoices from the SQS queue.
     * 
     * @param batchSize The maximum number of messages to process in a batch
     * @param windowSizeMinutes The size of the time window for grouping invoices
     * @param timeoutSeconds The timeout for processing
     * @return A map containing the results of the processing
     */
    private Map<String, Object> processInvoices(int batchSize, int windowSizeMinutes, int timeoutSeconds) {
        // Start time for timeout tracking
        long startTime = System.currentTimeMillis();
        long timeoutMillis = timeoutSeconds * 1000L;
        
        // Create a batch ID for this processing run
        String batchId = UUID.randomUUID().toString();
        
        // Track metrics
        int totalMessagesProcessed = 0;
        int totalInvoicesCreated = 0;
        
        // Process messages until timeout or no more messages
        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            // Receive messages from the queue
            List<Message> messages = SQSUtil.receiveMessages(INVOICE_QUEUE_URL, batchSize, 5);
            
            if (messages.isEmpty()) {
                logger.info("No more messages in the queue");
                break;
            }
            
            // Process the batch of messages
            Map<String, Object> batchResult = processBatch(messages, batchId, windowSizeMinutes);
            
            // Update metrics
            totalMessagesProcessed += messages.size();
            totalInvoicesCreated += (int) batchResult.get("invoicesCreated");
            
            // Delete the processed messages from the queue
            SQSUtil.deleteBatchMessages(INVOICE_QUEUE_URL, messages);
            
            // Check if we're approaching the timeout
            if (System.currentTimeMillis() - startTime > timeoutMillis * 0.8) {
                logger.info("Approaching timeout, stopping processing");
                break;
            }
        }
        
        // Create the response
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("batchId", batchId);
        response.put("messagesProcessed", totalMessagesProcessed);
        response.put("invoicesCreated", totalInvoicesCreated);
        response.put("processingTimeMs", System.currentTimeMillis() - startTime);
        
        return response;
    }
    
    /**
     * Process a batch of messages.
     * 
     * @param messages The messages to process
     * @param batchId The batch ID
     * @param windowSizeMinutes The size of the time window for grouping invoices
     * @return A map containing the results of the batch processing
     */
    private Map<String, Object> processBatch(List<Message> messages, String batchId, int windowSizeMinutes) {
        // Parse the messages into billing records
        List<BillingRecord> billingRecords = new ArrayList<>();
        
        for (Message message : messages) {
            try {
                // Parse the message body
                Map<String, Object> billingData = SQSUtil.parseMessageBody(message.body());
                
                // Get the billing record ID
                String billingId = (String) billingData.get("id");
                if (billingId == null) {
                    logger.error("Billing ID is missing in the message");
                    continue;
                }
                
                // Retrieve the billing record from DynamoDB
                BillingRecord billingRecord = DynamoDBUtil.getBillingRecord(billingId);
                if (billingRecord == null) {
                    logger.error("Billing record not found: {}", billingId);
                    continue;
                }
                
                // Only process completed billing records
                if (BillingRecord.STATUS_COMPLETED.equals(billingRecord.getStatus())) {
                    billingRecords.add(billingRecord);
                } else {
                    logger.warn("Skipping billing record with status {}: {}", 
                            billingRecord.getStatus(), billingId);
                }
            } catch (Exception e) {
                logger.error("Error processing message: {}", e.getMessage(), e);
            }
        }
        
        // Group billing records by customer and currency
        Map<String, List<BillingRecord>> groupedRecords = billingRecords.stream()
                .collect(Collectors.groupingBy(record -> 
                        record.getCustomerId() + ":" + record.getCurrency()));
        
        // Create invoices for each group
        int invoicesCreated = 0;
        
        for (Map.Entry<String, List<BillingRecord>> entry : groupedRecords.entrySet()) {
            try {
                // Get the customer ID and currency from the group key
                String[] parts = entry.getKey().split(":");
                String customerId = parts[0];
                String currency = parts[1];
                
                // Get the billing records for this group
                List<BillingRecord> records = entry.getValue();
                
                // Create time windows for the billing records
                Map<String, List<BillingRecord>> windowedRecords = createTimeWindows(records, windowSizeMinutes);
                
                // Create an invoice for each time window
                for (Map.Entry<String, List<BillingRecord>> windowEntry : windowedRecords.entrySet()) {
                    String windowId = windowEntry.getKey();
                    List<BillingRecord> windowRecords = windowEntry.getValue();
                    
                    // Create the invoice
                    Invoice invoice = new Invoice(customerId, currency, batchId, windowId);
                    
                    // Add the billing records to the invoice
                    for (BillingRecord record : windowRecords) {
                        invoice.addBillingRecord(record);
                    }
                    
                    // Set the invoice status to COMPLETED
                    invoice.setStatus(Invoice.STATUS_COMPLETED);
                    
                    // Save the invoice to DynamoDB
                    boolean saved = DynamoDBUtil.saveInvoice(invoice);
                    
                    if (saved) {
                        invoicesCreated++;
                        logger.info("Created invoice {} with {} billing records", 
                                invoice.getId(), windowRecords.size());
                    } else {
                        logger.error("Failed to save invoice for customer {} in window {}", 
                                customerId, windowId);
                    }
                }
            } catch (Exception e) {
                logger.error("Error creating invoice for group {}: {}", 
                        entry.getKey(), e.getMessage(), e);
            }
        }
        
        // Create the batch result
        Map<String, Object> batchResult = new HashMap<>();
        batchResult.put("messagesProcessed", messages.size());
        batchResult.put("billingRecordsProcessed", billingRecords.size());
        batchResult.put("invoicesCreated", invoicesCreated);
        
        return batchResult;
    }
    
    /**
     * Create time windows for billing records.
     * 
     * @param records The billing records
     * @param windowSizeMinutes The size of the time window in minutes
     * @return A map of window IDs to lists of billing records
     */
    private Map<String, List<BillingRecord>> createTimeWindows(List<BillingRecord> records, int windowSizeMinutes) {
        Map<String, List<BillingRecord>> windowedRecords = new HashMap<>();
        
        for (BillingRecord record : records) {
            // Get the created timestamp
            Instant createdAt = record.getCreatedAt();
            
            // Truncate to the nearest window
            Instant windowStart = createdAt.truncatedTo(ChronoUnit.HOURS);
            while (windowStart.isBefore(createdAt)) {
                windowStart = windowStart.plus(windowSizeMinutes, ChronoUnit.MINUTES);
            }
            windowStart = windowStart.minus(windowSizeMinutes, ChronoUnit.MINUTES);
            
            // Create a window ID
            String windowId = windowStart.toString();
            
            // Add the record to the window
            windowedRecords.computeIfAbsent(windowId, k -> new ArrayList<>()).add(record);
        }
        
        return windowedRecords;
    }
    
    /**
     * Get an integer parameter from the event, with a default value if not present.
     * 
     * @param event The event map
     * @param paramName The parameter name
     * @param defaultValue The default value
     * @return The parameter value
     */
    private int getIntParam(Map<String, Object> event, String paramName, int defaultValue) {
        if (event == null || !event.containsKey(paramName)) {
            return defaultValue;
        }
        
        Object value = event.get(paramName);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        
        return defaultValue;
    }
    
    /**
     * Create an error response.
     * 
     * @param errorMessage The error message
     * @return A map containing the error response
     */
    private Map<String, Object> createErrorResponse(String errorMessage) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("error", errorMessage);
        return response;
    }
}
