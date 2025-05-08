package com.example.billingapp.functions;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Lambda function that simulates client traffic by sending HTTP requests to the billing function.
 */
public class LoadGenerationFunction implements RequestHandler<Map<String, Object>, Map<String, Object>> {
    private static final Logger logger = LoggerFactory.getLogger(LoadGenerationFunction.class);
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    // Default configuration values
    private static final int DEFAULT_REQUEST_COUNT = 100;
    private static final int DEFAULT_CONCURRENCY = 10;
    private static final int DEFAULT_REQUEST_DELAY_MS = 100;
    private static final String DEFAULT_CURRENCY = "USD";
    
    // Environment variables
    private static final String BILLING_FUNCTION_URL = System.getenv("BILLING_FUNCTION_URL");
    
    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        logger.info("Processing load generation request: {}", gson.toJson(event));
        
        // Parse configuration from the event
        int requestCount = getIntParam(event, "requestCount", DEFAULT_REQUEST_COUNT);
        int concurrency = getIntParam(event, "concurrency", DEFAULT_CONCURRENCY);
        int requestDelayMs = getIntParam(event, "requestDelayMs", DEFAULT_REQUEST_DELAY_MS);
        String currency = getStringParam(event, "currency", DEFAULT_CURRENCY);
        
        logger.info("Load generation configuration: requestCount={}, concurrency={}, requestDelayMs={}, currency={}",
                requestCount, concurrency, requestDelayMs, currency);
        
        // Validate the billing function URL
        if (BILLING_FUNCTION_URL == null || BILLING_FUNCTION_URL.isEmpty()) {
            String errorMessage = "BILLING_FUNCTION_URL environment variable is not set";
            logger.error(errorMessage);
            return createErrorResponse(errorMessage);
        }
        
        try {
            // Generate and send the requests
            Map<String, Object> result = generateLoad(requestCount, concurrency, requestDelayMs, currency);
            logger.info("Load generation completed successfully: {}", gson.toJson(result));
            return result;
        } catch (Exception e) {
            String errorMessage = "Error generating load: " + e.getMessage();
            logger.error(errorMessage, e);
            return createErrorResponse(errorMessage);
        }
    }
    
    /**
     * Generate load by sending HTTP requests to the billing function.
     * 
     * @param requestCount The number of requests to send
     * @param concurrency The number of concurrent requests
     * @param requestDelayMs The delay between requests in milliseconds
     * @param currency The currency to use for the billing records
     * @return A map containing the results of the load generation
     */
    private Map<String, Object> generateLoad(int requestCount, int concurrency, int requestDelayMs, String currency) {
        // Create a thread pool for concurrent requests
        ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
        
        // Create an HTTP client
        SdkHttpClient httpClient = ApacheHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(5))
                .socketTimeout(Duration.ofSeconds(5))
                .build();
        
        // Start time for metrics
        long startTime = System.currentTimeMillis();
        
        // Generate the requests
        List<CompletableFuture<Map<String, Object>>> futures = IntStream.range(0, requestCount)
                .mapToObj(i -> CompletableFuture.supplyAsync(() -> {
                    try {
                        // Add delay between requests if specified
                        if (requestDelayMs > 0 && i > 0) {
                            Thread.sleep(requestDelayMs);
                        }
                        
                        // Generate a random billing record
                        Map<String, Object> billingRecord = generateRandomBillingRecord(currency);
                        
                        // Send the request to the billing function
                        return sendRequest(httpClient, billingRecord);
                    } catch (Exception e) {
                        logger.error("Error sending request: {}", e.getMessage(), e);
                        Map<String, Object> errorResult = new HashMap<>();
                        errorResult.put("success", false);
                        errorResult.put("error", e.getMessage());
                        return errorResult;
                    }
                }, executorService))
                .collect(Collectors.toList());
        
        // Wait for all requests to complete
        List<Map<String, Object>> results = futures.stream()
                .map(future -> {
                    try {
                        return future.get(30, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        logger.error("Error waiting for request completion: {}", e.getMessage(), e);
                        Map<String, Object> errorResult = new HashMap<>();
                        errorResult.put("success", false);
                        errorResult.put("error", e.getMessage());
                        return errorResult;
                    }
                })
                .collect(Collectors.toList());
        
        // End time for metrics
        long endTime = System.currentTimeMillis();
        long totalTimeMs = endTime - startTime;
        
        // Shutdown the executor service
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Close the HTTP client
        httpClient.close();
        
        // Calculate metrics
        long successCount = results.stream()
                .filter(result -> Boolean.TRUE.equals(result.get("success")))
                .count();
        
        // Create the response
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("requestCount", requestCount);
        response.put("successCount", successCount);
        response.put("failureCount", requestCount - successCount);
        response.put("totalTimeMs", totalTimeMs);
        response.put("requestsPerSecond", requestCount * 1000.0 / totalTimeMs);
        
        return response;
    }
    
    /**
     * Send an HTTP request to the billing function.
     * 
     * @param httpClient The HTTP client to use
     * @param billingRecord The billing record to send
     * @return A map containing the result of the request
     */
    private Map<String, Object> sendRequest(SdkHttpClient httpClient, Map<String, Object> billingRecord) {
        try {
            // Convert the billing record to JSON
            String requestBody = gson.toJson(billingRecord);
            
            // Create the HTTP request
            SdkHttpFullRequest request = SdkHttpFullRequest.builder()
                    .method(SdkHttpMethod.POST)
                    .uri(URI.create(BILLING_FUNCTION_URL))
                    .putHeader("Content-Type", "application/json")
                    .contentStreamProvider(() -> requestBody.getBytes())
                    .build();
            
            // Send the request
            software.amazon.awssdk.http.HttpExecuteResponse response = httpClient.prepareRequest(request).call();
            
            // Check the response status
            int statusCode = response.httpResponse().statusCode();
            boolean success = statusCode == HttpStatusCode.OK || statusCode == HttpStatusCode.CREATED;
            
            // Create the result
            Map<String, Object> result = new HashMap<>();
            result.put("success", success);
            result.put("statusCode", statusCode);
            
            if (success) {
                // Parse the response body if successful
                String responseBody = response.responseBody()
                        .map(body -> {
                            try {
                                return new String(body.asByteArray());
                            } catch (Exception e) {
                                return null;
                            }
                        })
                        .orElse(null);
                
                if (responseBody != null) {
                    try {
                        Map<String, Object> responseJson = gson.fromJson(responseBody, Map.class);
                        result.put("response", responseJson);
                    } catch (Exception e) {
                        result.put("response", responseBody);
                    }
                }
            } else {
                // Add error information if the request failed
                result.put("error", "Request failed with status code: " + statusCode);
            }
            
            return result;
        } catch (Exception e) {
            logger.error("Error sending request: {}", e.getMessage(), e);
            Map<String, Object> result = new HashMap<>();
            result.put("success", false);
            result.put("error", e.getMessage());
            return result;
        }
    }
    
    /**
     * Generate a random billing record.
     * 
     * @param currency The currency to use
     * @return A map representing a billing record
     */
    private Map<String, Object> generateRandomBillingRecord(String currency) {
        Random random = new Random();
        
        // Generate a random customer ID (between 1 and 100)
        String customerId = "customer-" + (random.nextInt(100) + 1);
        
        // Generate a random product ID (between 1 and 10)
        String productId = "product-" + (random.nextInt(10) + 1);
        
        // Generate a random amount (between 1.00 and 1000.00)
        double amount = 1.0 + (random.nextDouble() * 999.0);
        amount = Math.round(amount * 100.0) / 100.0; // Round to 2 decimal places
        
        // Create the billing record
        Map<String, Object> billingRecord = new HashMap<>();
        billingRecord.put("customerId", customerId);
        billingRecord.put("productId", productId);
        billingRecord.put("amount", amount);
        billingRecord.put("currency", currency);
        billingRecord.put("timestamp", System.currentTimeMillis());
        
        return billingRecord;
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
     * Get a string parameter from the event, with a default value if not present.
     * 
     * @param event The event map
     * @param paramName The parameter name
     * @param defaultValue The default value
     * @return The parameter value
     */
    private String getStringParam(Map<String, Object> event, String paramName, String defaultValue) {
        if (event == null || !event.containsKey(paramName)) {
            return defaultValue;
        }
        
        Object value = event.get(paramName);
        return value != null ? value.toString() : defaultValue;
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
