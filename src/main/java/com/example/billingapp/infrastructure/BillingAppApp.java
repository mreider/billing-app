package com.example.billingapp.infrastructure;

import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.lambda.Code;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

/**
 * Main CDK application class for the Billing Application.
 */
public class BillingAppApp {
    public static void main(final String[] args) {
        App app = new App();
        
        // Get the AWS account ID and region from environment variables
        String accountId = System.getenv("CDK_DEFAULT_ACCOUNT");
        String region = System.getenv("CDK_DEFAULT_REGION");
        
        // Create the environment
        Environment env = Environment.builder()
                .account(accountId)
                .region(region)
                .build();
        
        // Create the stack properties
        StackProps stackProps = StackProps.builder()
                .env(env)
                .build();
        
        // Get Dynatrace configuration from environment variables
        Map<String, String> dynatraceConfig = getDynatraceConfig();
        
        // Copy dtconfig.json to target directory if it exists
        copyDynatraceConfig();
        
        // Create the stack with Dynatrace configuration
        new BillingAppStack(app, "BillingAppStack", stackProps);
        
        app.synth();
    }
    
    /**
     * Get Dynatrace configuration from environment variables.
     * 
     * @return Map of Dynatrace configuration
     */
    private static Map<String, String> getDynatraceConfig() {
        Map<String, String> config = new HashMap<>();
        
        // Get Dynatrace configuration from environment variables
        String dtOtelEndpoint = System.getenv("DT_OTEL_ENDPOINT");
        String dtApiToken = System.getenv("DT_API_TOKEN");
        String dtLambdaLayerArn = System.getenv("DT_LAMBDA_LAYER_ARN");
        
        // Set default values if not provided
        if (dtOtelEndpoint == null || dtOtelEndpoint.isEmpty()) {
            System.out.println("Warning: DT_OTEL_ENDPOINT not set. Using default value.");
            dtOtelEndpoint = "https://example.live.dynatrace.com/api/v2/otlp";
        }
        
        if (dtApiToken == null || dtApiToken.isEmpty()) {
            System.out.println("Warning: DT_API_TOKEN not set. Using empty value.");
            dtApiToken = "";
        }
        
        if (dtLambdaLayerArn == null || dtLambdaLayerArn.isEmpty()) {
            String defaultRegion = System.getenv("CDK_DEFAULT_REGION");
            if (defaultRegion == null || defaultRegion.isEmpty()) {
                defaultRegion = "us-east-1";
            }
            System.out.println("Warning: DT_LAMBDA_LAYER_ARN not set. Using default value for region " + defaultRegion);
            dtLambdaLayerArn = "arn:aws:lambda:" + defaultRegion + ":725887861453:layer:Dynatrace:latest";
        }
        
        // Add configuration to map
        config.put("DT_OTEL_ENDPOINT", dtOtelEndpoint);
        config.put("DT_API_TOKEN", dtApiToken);
        config.put("DT_LAMBDA_LAYER_ARN", dtLambdaLayerArn);
        
        return config;
    }
    
    /**
     * Copy dtconfig.json to target directory if it exists.
     */
    private static void copyDynatraceConfig() {
        Path source = Paths.get("dtconfig.json");
        Path target = Paths.get("target/dtconfig.json");
        
        if (Files.exists(source)) {
            try {
                // Create target directory if it doesn't exist
                Files.createDirectories(target.getParent());
                
                // Copy the file
                Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                System.out.println("Copied dtconfig.json to target directory.");
            } catch (IOException e) {
                System.err.println("Error copying dtconfig.json: " + e.getMessage());
            }
        } else {
            System.out.println("Warning: dtconfig.json not found. Dynatrace Lambda Layer may not work correctly.");
        }
    }
}
