package com.example.billingapp.functions;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.example.billingapp.models.BillingRecord;
import com.example.billingapp.utils.DynamoDBUtil;
import com.example.billingapp.utils.SQSUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Lambda function that receives HTTP requests from the load generator,
 * stores initial billing data in DynamoDB, and publishes messages to an SQS queue for further processing.
 */
public class BillingFunction implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger logger = LoggerFactory.getLogger(BillingFunction.class);
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        logger.info("Processing billing request");
        
        try {
            // Parse the request body
            String requestBody = request.getBody();
            if (requestBody == null || requestBody.isEmpty()) {
                return createErrorResponse(400, "Request body is empty");
            }
            
            // Parse the request body as a map
            Map<String, Object> requestMap;
            try {
                requestMap = gson.fromJson(requestBody, Map.class);
            } catch (Exception e) {
                logger.error("Error parsing request body: {}", e.getMessage(), e);
                return createErrorResponse(400, "Invalid JSON in request body");
            }
            
            // Validate required fields
            if (!validateRequest(requestMap)) {
                return createErrorResponse(400, "Missing required fields in request");
            }
            
            // Create a billing record from the request
            BillingRecord billingRecord = createBillingRecord(requestMap);
            
            // Save the billing record to DynamoDB
            boolean savedToDynamoDB = DynamoDBUtil.saveBillingRecord(billingRecord);
            if (!savedToDynamoDB) {
                return createErrorResponse(500, "Failed to save billing record to DynamoDB");
            }
            
            // Send the billing record to SQS for further processing
            String messageId = SQSUtil.sendBillingMessage(billingRecord.toDynamoDBItem());
            if (messageId == null) {
                logger.warn("Failed to send billing record to SQS, but continuing with response");
            }
            
            // Create a successful response
            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("success", true);
            responseBody.put("message", "Billing record processed successfully");
            responseBody.put("billingId", billingRecord.getId());
            
            return createSuccessResponse(responseBody);
        } catch (Exception e) {
            logger.error("Error processing billing request: {}", e.getMessage(), e);
            return createErrorResponse(500, "Internal server error: " + e.getMessage());
        }
    }
    
    /**
     * Validate that the request contains all required fields.
     * 
     * @param requestMap The request map
     * @return true if the request is valid, false otherwise
     */
    private boolean validateRequest(Map<String, Object> requestMap) {
        if (requestMap == null) {
            return false;
        }
        
        // Check required fields
        return requestMap.containsKey("customerId") &&
                requestMap.containsKey("productId") &&
                requestMap.containsKey("amount") &&
                requestMap.containsKey("currency");
    }
    
    /**
     * Create a billing record from the request map.
     * 
     * @param requestMap The request map
     * @return The billing record
     */
    private BillingRecord createBillingRecord(Map<String, Object> requestMap) {
        String customerId = requestMap.get("customerId").toString();
        String productId = requestMap.get("productId").toString();
        
        // Parse the amount as a double
        double amount;
        Object amountObj = requestMap.get("amount");
        if (amountObj instanceof Number) {
            amount = ((Number) amountObj).doubleValue();
        } else {
            amount = Double.parseDouble(amountObj.toString());
        }
        
        String currency = requestMap.get("currency").toString();
        
        // Create the billing record
        BillingRecord billingRecord = new BillingRecord(customerId, productId, amount, currency);
        
        // Add any additional metadata from the request
        requestMap.forEach((key, value) -> {
            if (!key.equals("customerId") && !key.equals("productId") && 
                    !key.equals("amount") && !key.equals("currency")) {
                billingRecord.addMetadata(key, value.toString());
            }
        });
        
        return billingRecord;
    }
    
    /**
     * Create a successful API Gateway response.
     * 
     * @param responseBody The response body
     * @return The API Gateway response
     */
    private APIGatewayProxyResponseEvent createSuccessResponse(Map<String, Object> responseBody) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(200);
        response.setBody(gson.toJson(responseBody));
        
        // Add CORS headers
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Methods", "POST, OPTIONS");
        headers.put("Access-Control-Allow-Headers", "Content-Type");
        response.setHeaders(headers);
        
        return response;
    }
    
    /**
     * Create an error API Gateway response.
     * 
     * @param statusCode The HTTP status code
     * @param errorMessage The error message
     * @return The API Gateway response
     */
    private APIGatewayProxyResponseEvent createErrorResponse(int statusCode, String errorMessage) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(statusCode);
        
        Map<String, Object> errorBody = new HashMap<>();
        errorBody.put("success", false);
        errorBody.put("error", errorMessage);
        response.setBody(gson.toJson(errorBody));
        
        // Add CORS headers
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Methods", "POST, OPTIONS");
        headers.put("Access-Control-Allow-Headers", "Content-Type");
        response.setHeaders(headers);
        
        return response;
    }
}
