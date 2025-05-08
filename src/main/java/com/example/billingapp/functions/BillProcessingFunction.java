package com.example.billingapp.functions;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.example.billingapp.models.BillingRecord;
import com.example.billingapp.utils.DynamoDBUtil;
import com.example.billingapp.utils.SQSUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Lambda function that is triggered by messages from the SQS queue,
 * retrieves and updates billing records in DynamoDB, and implements retry logic with error handling.
 */
public class BillProcessingFunction implements RequestHandler<SQSEvent, Map<String, Object>> {
    private static final Logger logger = LoggerFactory.getLogger(BillProcessingFunction.class);
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    // Configuration for retry logic
    private static final int MAX_RETRY_COUNT = 3;
    private static final double PROCESSING_FAILURE_RATE = 0.1; // 10% chance of failure for simulation
    
    // Random number generator for simulating failures
    private static final Random random = new Random();
    
    @Override
    public Map<String, Object> handleRequest(SQSEvent event, Context context) {
        logger.info("Processing {} SQS messages", event.getRecords().size());
        
        int successCount = 0;
        int failureCount = 0;
        
        // Process each SQS message
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            try {
                // Process the message
                boolean processed = processBillingMessage(message);
                
                if (processed) {
                    successCount++;
                } else {
                    failureCount++;
                }
            } catch (Exception e) {
                logger.error("Error processing SQS message: {}", e.getMessage(), e);
                failureCount++;
            }
        }
        
        // Create the response
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("processedCount", event.getRecords().size());
        response.put("successCount", successCount);
        response.put("failureCount", failureCount);
        
        logger.info("Processed {} messages: {} succeeded, {} failed", 
                event.getRecords().size(), successCount, failureCount);
        
        return response;
    }
    
    /**
     * Process a billing message from SQS.
     * 
     * @param message The SQS message
     * @return true if the message was processed successfully, false otherwise
     */
    private boolean processBillingMessage(SQSEvent.SQSMessage message) {
        try {
            // Parse the message body
            String messageBody = message.getBody();
            Map<String, Object> billingData = SQSUtil.parseMessageBody(messageBody);
            
            // Get the billing record ID
            String billingId = (String) billingData.get("id");
            if (billingId == null) {
                logger.error("Billing ID is missing in the message");
                return false;
            }
            
            // Retrieve the billing record from DynamoDB
            BillingRecord billingRecord = DynamoDBUtil.getBillingRecord(billingId);
            if (billingRecord == null) {
                logger.error("Billing record not found: {}", billingId);
                return false;
            }
            
            // Check if the billing record has already been processed
            if (BillingRecord.STATUS_COMPLETED.equals(billingRecord.getStatus()) || 
                    BillingRecord.STATUS_FAILED.equals(billingRecord.getStatus())) {
                logger.info("Billing record already processed: {}", billingId);
                return true;
            }
            
            // Update the billing record status to PROCESSING
            billingRecord.setStatus(BillingRecord.STATUS_PROCESSING);
            DynamoDBUtil.updateBillingRecord(billingRecord);
            
            // Process the billing record (with simulated failures for testing)
            boolean processed = processBillingRecord(billingRecord);
            
            if (processed) {
                // Update the billing record status to COMPLETED
                billingRecord.setStatus(BillingRecord.STATUS_COMPLETED);
                DynamoDBUtil.updateBillingRecord(billingRecord);
                
                // Send the billing record to the invoice queue for further processing
                SQSUtil.sendInvoiceMessage(billingRecord.toDynamoDBItem());
                
                logger.info("Successfully processed billing record: {}", billingId);
                return true;
            } else {
                // Increment the retry count
                billingRecord.incrementRetryCount();
                
                // Check if we've reached the maximum retry count
                if (billingRecord.getRetryCount() >= MAX_RETRY_COUNT) {
                    // Update the billing record status to FAILED
                    billingRecord.setStatus(BillingRecord.STATUS_FAILED);
                    billingRecord.setErrorMessage("Failed after " + MAX_RETRY_COUNT + " retries");
                    DynamoDBUtil.updateBillingRecord(billingRecord);
                    
                    logger.warn("Billing record failed after {} retries: {}", MAX_RETRY_COUNT, billingId);
                    return false;
                } else {
                    // Update the billing record status back to PENDING for retry
                    billingRecord.setStatus(BillingRecord.STATUS_PENDING);
                    billingRecord.setErrorMessage("Processing failed, will retry");
                    DynamoDBUtil.updateBillingRecord(billingRecord);
                    
                    // Send the billing record back to the billing queue for retry
                    SQSUtil.sendBillingMessage(billingRecord.toDynamoDBItem());
                    
                    logger.info("Billing record processing failed, scheduled for retry: {}", billingId);
                    return false;
                }
            }
        } catch (Exception e) {
            logger.error("Error processing billing message: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Process a billing record (with simulated failures for testing).
     * 
     * @param billingRecord The billing record to process
     * @return true if the processing was successful, false otherwise
     */
    private boolean processBillingRecord(BillingRecord billingRecord) {
        try {
            // Simulate processing time (50-150ms)
            Thread.sleep(50 + random.nextInt(100));
            
            // Simulate processing failures for testing
            if (random.nextDouble() < PROCESSING_FAILURE_RATE) {
                logger.info("Simulating processing failure for billing record: {}", billingRecord.getId());
                return false;
            }
            
            // In a real application, this would perform actual billing processing logic
            // For example:
            // - Validate the billing data
            // - Calculate taxes and fees
            // - Process payment
            // - Generate receipt
            
            // For this example, we'll just simulate successful processing
            return true;
        } catch (Exception e) {
            logger.error("Error processing billing record: {}", e.getMessage(), e);
            return false;
        }
    }
}
