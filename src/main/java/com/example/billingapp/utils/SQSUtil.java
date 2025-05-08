package com.example.billingapp.utils;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for SQS operations.
 */
public class SQSUtil {
    private static final Logger logger = LoggerFactory.getLogger(SQSUtil.class);
    // Queue name prefix for feature flagging
    private static final String QUEUE_PREFIX = Boolean.parseBoolean(System.getenv("OTEL_ENABLED")) ? "Otel" : "";
    
    private static final String BILLING_QUEUE_URL = System.getenv("BILLING_QUEUE_URL");
    private static final String INVOICE_QUEUE_URL = System.getenv("INVOICE_QUEUE_URL");
    
    // Get the actual queue URLs with prefix
    private static final String ACTUAL_BILLING_QUEUE_URL = addPrefixToQueueUrl(BILLING_QUEUE_URL, QUEUE_PREFIX);
    private static final String ACTUAL_INVOICE_QUEUE_URL = addPrefixToQueueUrl(INVOICE_QUEUE_URL, QUEUE_PREFIX);
    
    private static final Gson gson = new Gson();
    
    private static SqsClient sqsClient;
    
    /**
     * Add a prefix to a queue URL.
     * 
     * @param queueUrl The queue URL
     * @param prefix The prefix to add
     * @return The queue URL with the prefix
     */
    private static String addPrefixToQueueUrl(String queueUrl, String prefix) {
        if (queueUrl == null || queueUrl.isEmpty() || prefix == null || prefix.isEmpty()) {
            return queueUrl;
        }
        
        // Extract the queue name from the URL
        int lastSlashIndex = queueUrl.lastIndexOf('/');
        if (lastSlashIndex == -1) {
            return queueUrl;
        }
        
        String queueName = queueUrl.substring(lastSlashIndex + 1);
        String baseUrl = queueUrl.substring(0, lastSlashIndex + 1);
        
        return baseUrl + prefix + queueName;
    }
    
    // Get the SQS client (singleton pattern)
    public static synchronized SqsClient getSqsClient() {
        if (sqsClient == null) {
            // Get the region from environment variable or use a default
            String regionName = Optional.ofNullable(System.getenv("AWS_REGION"))
                    .orElse("us-east-1");
            
            // Create the client builder
            SqsClientBuilder builder = SqsClient.builder()
                    .region(Region.of(regionName));
            
            // Instrument with OpenTelemetry if enabled
            builder = OpenTelemetryUtil.instrumentSqsClientBuilder(builder);
            
            // Build the client
            sqsClient = builder.build();
            
            logger.info("SQS client initialized with queue prefix: '{}', billing queue: {}, invoice queue: {}", 
                    QUEUE_PREFIX, ACTUAL_BILLING_QUEUE_URL, ACTUAL_INVOICE_QUEUE_URL);
        }
        return sqsClient;
    }
    
    /**
     * Send a message to the billing queue.
     * 
     * @param message The message to send
     * @return The message ID if successful, null otherwise
     */
    public static String sendBillingMessage(Object message) {
        return sendMessage(ACTUAL_BILLING_QUEUE_URL, message);
    }
    
    /**
     * Send a message to the invoice queue.
     * 
     * @param message The message to send
     * @return The message ID if successful, null otherwise
     */
    public static String sendInvoiceMessage(Object message) {
        return sendMessage(ACTUAL_INVOICE_QUEUE_URL, message);
    }
    
    /**
     * Send a message to an SQS queue.
     * 
     * @param queueUrl The URL of the queue
     * @param message The message to send
     * @return The message ID if successful, null otherwise
     */
    public static String sendMessage(String queueUrl, Object message) {
        return OpenTelemetryUtil.runWithSpan("SQS.SendMessage", () -> {
            try {
                String messageBody = gson.toJson(message);
                
                // Add OpenTelemetry attributes
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("messaging.system", "sqs");
                    OpenTelemetryUtil.addAttribute("messaging.destination", queueUrl);
                    OpenTelemetryUtil.addAttribute("messaging.operation", "send");
                }
                
                SendMessageRequest request = SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(messageBody)
                        .build();
                
                SendMessageResponse response = getSqsClient().sendMessage(request);
                logger.info("Successfully sent message to queue: {}", queueUrl);
                
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("messaging.message_id", response.messageId());
                }
                
                return response.messageId();
            } catch (SdkException e) {
                logger.error("Error sending message to queue {}: {}", queueUrl, e.getMessage(), e);
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.recordException(e);
                }
                return null;
            }
        });
    }
    
    /**
     * Send a batch of messages to an SQS queue.
     * 
     * @param queueUrl The URL of the queue
     * @param messages The list of messages to send
     * @return The number of successfully sent messages
     */
    public static int sendBatchMessages(String queueUrl, List<Object> messages) {
        return OpenTelemetryUtil.runWithSpan("SQS.SendMessageBatch", () -> {
            try {
                // Add OpenTelemetry attributes
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("messaging.system", "sqs");
                    OpenTelemetryUtil.addAttribute("messaging.destination", queueUrl);
                    OpenTelemetryUtil.addAttribute("messaging.operation", "send_batch");
                    OpenTelemetryUtil.addAttribute("messaging.batch.size", messages.size());
                }
                
                List<SendMessageBatchRequestEntry> entries = messages.stream()
                        .map(message -> {
                            String id = java.util.UUID.randomUUID().toString();
                            String messageBody = gson.toJson(message);
                            return SendMessageBatchRequestEntry.builder()
                                    .id(id)
                                    .messageBody(messageBody)
                                    .build();
                        })
                        .toList();
                
                // SQS allows a maximum of 10 messages per batch
                int successCount = 0;
                for (int i = 0; i < entries.size(); i += 10) {
                    int end = Math.min(i + 10, entries.size());
                    List<SendMessageBatchRequestEntry> batch = entries.subList(i, end);
                    
                    SendMessageBatchRequest request = SendMessageBatchRequest.builder()
                            .queueUrl(queueUrl)
                            .entries(batch)
                            .build();
                    
                    SendMessageBatchResponse response = getSqsClient().sendMessageBatch(request);
                    successCount += response.successful().size();
                    
                    if (!response.failed().isEmpty()) {
                        logger.warn("Failed to send {} messages in batch", response.failed().size());
                    }
                }
                
                logger.info("Successfully sent {} out of {} messages to queue: {}", 
                        successCount, messages.size(), queueUrl);
                
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("messaging.batch.success_count", successCount);
                }
                
                return successCount;
            } catch (SdkException e) {
                logger.error("Error sending batch messages to queue {}: {}", queueUrl, e.getMessage(), e);
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.recordException(e);
                }
                return 0;
            }
        });
    }
    
    /**
     * Receive messages from an SQS queue.
     * 
     * @param queueUrl The URL of the queue
     * @param maxMessages The maximum number of messages to receive (1-10)
     * @param waitTimeSeconds The wait time in seconds (0-20)
     * @return The list of received messages
     */
    public static List<Message> receiveMessages(String queueUrl, int maxMessages, int waitTimeSeconds) {
        return OpenTelemetryUtil.runWithSpan("SQS.ReceiveMessage", () -> {
            try {
                // Ensure maxMessages is within the valid range
                int validMaxMessages = Math.min(Math.max(1, maxMessages), 10);
                
                // Ensure waitTimeSeconds is within the valid range
                int validWaitTime = Math.min(Math.max(0, waitTimeSeconds), 20);
                
                // Add OpenTelemetry attributes
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("messaging.system", "sqs");
                    OpenTelemetryUtil.addAttribute("messaging.destination", queueUrl);
                    OpenTelemetryUtil.addAttribute("messaging.operation", "receive");
                    OpenTelemetryUtil.addAttribute("messaging.max_messages", validMaxMessages);
                    OpenTelemetryUtil.addAttribute("messaging.wait_time_seconds", validWaitTime);
                }
                
                ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(validMaxMessages)
                        .waitTimeSeconds(validWaitTime)
                        .build();
                
                ReceiveMessageResponse response = getSqsClient().receiveMessage(request);
                List<Message> messages = response.messages();
                
                logger.info("Received {} messages from queue: {}", messages.size(), queueUrl);
                
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("messaging.received_count", messages.size());
                }
                
                return messages;
            } catch (SdkException e) {
                logger.error("Error receiving messages from queue {}: {}", queueUrl, e.getMessage(), e);
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.recordException(e);
                }
                return List.of();
            }
        });
    }
    
    /**
     * Delete a message from an SQS queue.
     * 
     * @param queueUrl The URL of the queue
     * @param receiptHandle The receipt handle of the message
     * @return true if successful, false otherwise
     */
    public static boolean deleteMessage(String queueUrl, String receiptHandle) {
        return OpenTelemetryUtil.runWithSpan("SQS.DeleteMessage", () -> {
            try {
                // Add OpenTelemetry attributes
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("messaging.system", "sqs");
                    OpenTelemetryUtil.addAttribute("messaging.destination", queueUrl);
                    OpenTelemetryUtil.addAttribute("messaging.operation", "delete");
                }
                
                DeleteMessageRequest request = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(receiptHandle)
                        .build();
                
                getSqsClient().deleteMessage(request);
                logger.debug("Successfully deleted message from queue: {}", queueUrl);
                return true;
            } catch (SdkException e) {
                logger.error("Error deleting message from queue {}: {}", queueUrl, e.getMessage(), e);
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.recordException(e);
                }
                return false;
            }
        });
    }
    
    /**
     * Delete a batch of messages from an SQS queue.
     * 
     * @param queueUrl The URL of the queue
     * @param messages The list of messages to delete
     * @return The number of successfully deleted messages
     */
    public static int deleteBatchMessages(String queueUrl, List<Message> messages) {
        return OpenTelemetryUtil.runWithSpan("SQS.DeleteMessageBatch", () -> {
            try {
                // Add OpenTelemetry attributes
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("messaging.system", "sqs");
                    OpenTelemetryUtil.addAttribute("messaging.destination", queueUrl);
                    OpenTelemetryUtil.addAttribute("messaging.operation", "delete_batch");
                    OpenTelemetryUtil.addAttribute("messaging.batch.size", messages.size());
                }
                
                List<DeleteMessageBatchRequestEntry> entries = messages.stream()
                        .map(message -> DeleteMessageBatchRequestEntry.builder()
                                .id(message.messageId())
                                .receiptHandle(message.receiptHandle())
                                .build())
                        .toList();
                
                // SQS allows a maximum of 10 messages per batch
                int successCount = 0;
                for (int i = 0; i < entries.size(); i += 10) {
                    int end = Math.min(i + 10, entries.size());
                    List<DeleteMessageBatchRequestEntry> batch = entries.subList(i, end);
                    
                    DeleteMessageBatchRequest request = DeleteMessageBatchRequest.builder()
                            .queueUrl(queueUrl)
                            .entries(batch)
                            .build();
                    
                    DeleteMessageBatchResponse response = getSqsClient().deleteMessageBatch(request);
                    successCount += response.successful().size();
                    
                    if (!response.failed().isEmpty()) {
                        logger.warn("Failed to delete {} messages in batch", response.failed().size());
                    }
                }
                
                logger.info("Successfully deleted {} out of {} messages from queue: {}", 
                        successCount, messages.size(), queueUrl);
                
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.addAttribute("messaging.batch.success_count", successCount);
                }
                
                return successCount;
            } catch (SdkException e) {
                logger.error("Error deleting batch messages from queue {}: {}", queueUrl, e.getMessage(), e);
                if (OpenTelemetryUtil.isEnabled()) {
                    OpenTelemetryUtil.recordException(e);
                }
                return 0;
            }
        });
    }
    
    /**
     * Parse a message body from JSON to a specific class.
     * 
     * @param messageBody The message body as JSON
     * @param clazz The class to parse to
     * @param <T> The type of the class
     * @return The parsed object
     */
    public static <T> T parseMessageBody(String messageBody, Class<T> clazz) {
        return gson.fromJson(messageBody, clazz);
    }
    
    /**
     * Parse a message body from JSON to a Map.
     * 
     * @param messageBody The message body as JSON
     * @return The parsed map
     */
    public static Map<String, Object> parseMessageBody(String messageBody) {
        return gson.fromJson(messageBody, Map.class);
    }
}
