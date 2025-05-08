package com.example.billingapp.infrastructure;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.dynamodb.*;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.*;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * AWS CDK Stack for the Billing Application.
 */
public class BillingAppStack extends Stack {
    
    // Dynatrace configuration
    private final String dtOtelEndpoint;
    private final String dtApiToken;
    private final String dtLambdaLayerArn;
    
    public BillingAppStack(final Construct scope, final String id) {
        this(scope, id, null);
    }
    
    public BillingAppStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);
        
        // Get Dynatrace configuration from environment variables
        this.dtOtelEndpoint = Optional.ofNullable(System.getenv("DT_OTEL_ENDPOINT"))
                .orElse("https://abc12345.live.dynatrace.com/api/v2/otlp");
        this.dtApiToken = Optional.ofNullable(System.getenv("DT_API_TOKEN"))
                .orElse("");
        this.dtLambdaLayerArn = Optional.ofNullable(System.getenv("DT_LAMBDA_LAYER_ARN"))
                .orElse("arn:aws:lambda:" + props.getEnv().getRegion() + ":725887861453:layer:Dynatrace:latest");
        
        System.out.println("Dynatrace Configuration:");
        System.out.println("  Endpoint: " + this.dtOtelEndpoint);
        System.out.println("  Lambda Layer ARN: " + this.dtLambdaLayerArn);
        
        // Create DynamoDB tables
        Table billingTable = createBillingTable();
        Table invoiceTable = createInvoiceTable();
        
        // Create OpenTelemetry-enabled DynamoDB tables with prefix
        Table otelBillingTable = createBillingTable("Otel");
        Table otelInvoiceTable = createInvoiceTable("Otel");
        
        // Create SQS queues
        Queue billingDlq = createQueue("BillingDLQ");
        Queue billingQueue = createQueueWithDlq("BillingQueue", billingDlq);
        
        Queue invoiceDlq = createQueue("InvoiceDLQ");
        Queue invoiceQueue = createQueueWithDlq("InvoiceQueue", invoiceDlq);
        
        // Create OpenTelemetry-enabled SQS queues with prefix
        Queue otelBillingDlq = createQueue("OtelBillingDLQ");
        Queue otelBillingQueue = createQueueWithDlq("OtelBillingQueue", otelBillingDlq);
        
        Queue otelInvoiceDlq = createQueue("OtelInvoiceDLQ");
        Queue otelInvoiceQueue = createQueueWithDlq("OtelInvoiceQueue", otelInvoiceDlq);
        
        // Create standard Lambda functions (without OpenTelemetry)
        Function loadGenerationFunction = createLoadGenerationFunction(billingTable, billingQueue, false);
        Function billingFunction = createBillingFunction(billingTable, billingQueue, false);
        Function billProcessingFunction = createBillProcessingFunction(billingTable, billingQueue, invoiceQueue, false);
        Function invoiceProcessingFunction = createInvoiceProcessingFunction(billingTable, invoiceTable, invoiceQueue, false);
        
        // Create OpenTelemetry-enabled Lambda functions
        Function otelLoadGenerationFunction = createLoadGenerationFunction(otelBillingTable, otelBillingQueue, true);
        Function otelBillingFunction = createBillingFunction(otelBillingTable, otelBillingQueue, true);
        Function otelBillProcessingFunction = createBillProcessingFunction(otelBillingTable, otelBillingQueue, otelInvoiceQueue, true);
        Function otelInvoiceProcessingFunction = createInvoiceProcessingFunction(otelBillingTable, otelInvoiceTable, otelInvoiceQueue, true);
        
        // Set up event sources for standard functions
        billProcessingFunction.addEventSource(new SqsEventSource(billingQueue, SqsEventSourceProps.builder()
                .batchSize(10)
                .maxBatchingWindow(Duration.seconds(5))
                .build()));
        
        // Set up event sources for OpenTelemetry-enabled functions
        otelBillProcessingFunction.addEventSource(new SqsEventSource(otelBillingQueue, SqsEventSourceProps.builder()
                .batchSize(10)
                .maxBatchingWindow(Duration.seconds(5))
                .build()));
        
        // Grant permissions for standard functions
        billingTable.grantReadWriteData(billingFunction);
        billingTable.grantReadWriteData(billProcessingFunction);
        billingTable.grantReadData(invoiceProcessingFunction);
        
        invoiceTable.grantReadWriteData(invoiceProcessingFunction);
        
        billingQueue.grantSendMessages(billingFunction);
        billingQueue.grantSendMessages(billProcessingFunction);
        billingQueue.grantConsumeMessages(billProcessingFunction);
        
        invoiceQueue.grantSendMessages(billProcessingFunction);
        invoiceQueue.grantConsumeMessages(invoiceProcessingFunction);
        
        // Grant permissions for OpenTelemetry-enabled functions
        otelBillingTable.grantReadWriteData(otelBillingFunction);
        otelBillingTable.grantReadWriteData(otelBillProcessingFunction);
        otelBillingTable.grantReadData(otelInvoiceProcessingFunction);
        
        otelInvoiceTable.grantReadWriteData(otelInvoiceProcessingFunction);
        
        otelBillingQueue.grantSendMessages(otelBillingFunction);
        otelBillingQueue.grantSendMessages(otelBillProcessingFunction);
        otelBillingQueue.grantConsumeMessages(otelBillProcessingFunction);
        
        otelInvoiceQueue.grantSendMessages(otelBillProcessingFunction);
        otelInvoiceQueue.grantConsumeMessages(otelInvoiceProcessingFunction);
    }
    
    /**
     * Create the DynamoDB table for billing records.
     * 
     * @return The created table
     */
    private Table createBillingTable() {
        return createBillingTable("");
    }
    
    /**
     * Create the DynamoDB table for billing records with a prefix.
     * 
     * @param prefix The prefix to add to the table name
     * @return The created table
     */
    private Table createBillingTable(String prefix) {
        return Table.Builder.create(this, prefix + "BillingTable")
                .tableName(prefix + "BillingRecords")
                .partitionKey(Attribute.builder()
                        .name("id")
                        .type(AttributeType.STRING)
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .removalPolicy(RemovalPolicy.DESTROY) // For development only, use RETAIN for production
                .build();
    }
    
    /**
     * Create the DynamoDB table for invoices.
     * 
     * @return The created table
     */
    private Table createInvoiceTable() {
        return createInvoiceTable("");
    }
    
    /**
     * Create the DynamoDB table for invoices with a prefix.
     * 
     * @param prefix The prefix to add to the table name
     * @return The created table
     */
    private Table createInvoiceTable(String prefix) {
        return Table.Builder.create(this, prefix + "InvoiceTable")
                .tableName(prefix + "Invoices")
                .partitionKey(Attribute.builder()
                        .name("id")
                        .type(AttributeType.STRING)
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .removalPolicy(RemovalPolicy.DESTROY) // For development only, use RETAIN for production
                .build();
    }
    
    /**
     * Create an SQS queue.
     * 
     * @param id The ID of the queue
     * @return The created queue
     */
    private Queue createQueue(String id) {
        return Queue.Builder.create(this, id)
                .queueName(id)
                .visibilityTimeout(Duration.seconds(300))
                .retentionPeriod(Duration.days(14))
                .build();
    }
    
    /**
     * Create an SQS queue with a dead-letter queue.
     * 
     * @param id The ID of the queue
     * @param dlq The dead-letter queue
     * @return The created queue
     */
    private Queue createQueueWithDlq(String id, Queue dlq) {
        return Queue.Builder.create(this, id)
                .queueName(id)
                .visibilityTimeout(Duration.seconds(300))
                .retentionPeriod(Duration.days(14))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .queue(dlq)
                        .maxReceiveCount(3)
                        .build())
                .build();
    }
    
    /**
     * Create the load generation Lambda function.
     * 
     * @param billingTable The billing table
     * @param billingQueue The billing queue
     * @param enableOtel Whether to enable OpenTelemetry
     * @return The created function
     */
    private Function createLoadGenerationFunction(Table billingTable, Queue billingQueue, boolean enableOtel) {
        String prefix = enableOtel ? "Otel" : "";
        
        Map<String, String> environment = new HashMap<>();
        environment.put("BILLING_FUNCTION_URL", ""); // This will be set after deployment
        environment.put("BILLING_TABLE_NAME", billingTable.getTableName());
        environment.put("BILLING_QUEUE_URL", billingQueue.getQueueUrl());
        
        // Add OpenTelemetry environment variables if enabled
        if (enableOtel) {
            environment.put("OTEL_ENABLED", "true");
            environment.put("DT_OTEL_ENDPOINT", dtOtelEndpoint);
            environment.put("DT_API_TOKEN", dtApiToken);
            environment.put("OTEL_SERVICE_NAME", "billing-app-load-generator");
            environment.put("OTEL_RESOURCE_ATTRIBUTES", "service.name=billing-app-load-generator,service.version=1.0.0");
        }
        
        // Create the function builder
        FunctionProps.Builder functionBuilder = FunctionProps.builder()
                .functionName(prefix + "LoadGenerationFunction")
                .runtime(Runtime.JAVA_11)
                .code(Code.fromAsset("target/billing-app-1.0-SNAPSHOT.jar"))
                .handler("com.example.billingapp.functions.LoadGenerationFunction::handleRequest")
                .memorySize(512)
                .timeout(Duration.seconds(300))
                .environment(environment)
                .logRetention(RetentionDays.ONE_WEEK);
        
        // Add Dynatrace Lambda layer if enabled
        if (enableOtel) {
            functionBuilder.layers(List.of(LayerVersion.fromLayerVersionArn(this, 
                    prefix + "DynatraceLayer-LoadGen", dtLambdaLayerArn)));
        }
        
        // Create the function
        Function function = new Function(this, prefix + "LoadGenerationFunction", functionBuilder.build());
        
        // Create dtconfig.json file in the function's root directory if enabled
        if (enableOtel) {
            try {
                // Copy dtconfig.json to the function's code directory
                function.addEnvironment("DT_CONFIG_FILE_PATH", "/var/task/dtconfig.json");
            } catch (Exception e) {
                System.err.println("Error setting up Dynatrace configuration: " + e.getMessage());
            }
        }
        
        // Add permissions to invoke the billing function URL
        function.addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("lambda:InvokeFunctionUrl"))
                .resources(List.of("*"))
                .build());
        
        return function;
    }
    
    /**
     * Create the billing Lambda function.
     * 
     * @param billingTable The billing table
     * @param billingQueue The billing queue
     * @param enableOtel Whether to enable OpenTelemetry
     * @return The created function
     */
    private Function createBillingFunction(Table billingTable, Queue billingQueue, boolean enableOtel) {
        String prefix = enableOtel ? "Otel" : "";
        
        Map<String, String> environment = new HashMap<>();
        environment.put("BILLING_TABLE_NAME", billingTable.getTableName());
        environment.put("BILLING_QUEUE_URL", billingQueue.getQueueUrl());
        
        // Add OpenTelemetry environment variables if enabled
        if (enableOtel) {
            environment.put("OTEL_ENABLED", "true");
            environment.put("DT_OTEL_ENDPOINT", dtOtelEndpoint);
            environment.put("DT_API_TOKEN", dtApiToken);
            environment.put("OTEL_SERVICE_NAME", "billing-app-billing-function");
            environment.put("OTEL_RESOURCE_ATTRIBUTES", "service.name=billing-app-billing-function,service.version=1.0.0");
        }
        
        // Create the function builder
        FunctionProps.Builder functionBuilder = FunctionProps.builder()
                .functionName(prefix + "BillingFunction")
                .runtime(Runtime.JAVA_11)
                .code(Code.fromAsset("target/billing-app-1.0-SNAPSHOT.jar"))
                .handler("com.example.billingapp.functions.BillingFunction::handleRequest")
                .memorySize(512)
                .timeout(Duration.seconds(30))
                .environment(environment)
                .logRetention(RetentionDays.ONE_WEEK);
        
        // Add Dynatrace Lambda layer if enabled
        if (enableOtel) {
            functionBuilder.layers(List.of(LayerVersion.fromLayerVersionArn(this, 
                    prefix + "DynatraceLayer-Billing", dtLambdaLayerArn)));
        }
        
        // Create the function
        Function function = new Function(this, prefix + "BillingFunction", functionBuilder.build());
        
        // Create dtconfig.json file in the function's root directory if enabled
        if (enableOtel) {
            try {
                // Copy dtconfig.json to the function's code directory
                function.addEnvironment("DT_CONFIG_FILE_PATH", "/var/task/dtconfig.json");
            } catch (Exception e) {
                System.err.println("Error setting up Dynatrace configuration: " + e.getMessage());
            }
        }
        
        // Create a function URL for the billing function
        FunctionUrl functionUrl = FunctionUrl.Builder.create(this, prefix + "BillingFunctionUrl")
                .function(function)
                .authType(FunctionUrlAuthType.NONE) // For development only, use AWS_IAM for production
                .build();
        
        return function;
    }
    
    /**
     * Create the bill processing Lambda function.
     * 
     * @param billingTable The billing table
     * @param billingQueue The billing queue
     * @param invoiceQueue The invoice queue
     * @param enableOtel Whether to enable OpenTelemetry
     * @return The created function
     */
    private Function createBillProcessingFunction(Table billingTable, Queue billingQueue, Queue invoiceQueue, boolean enableOtel) {
        String prefix = enableOtel ? "Otel" : "";
        
        Map<String, String> environment = new HashMap<>();
        environment.put("BILLING_TABLE_NAME", billingTable.getTableName());
        environment.put("BILLING_QUEUE_URL", billingQueue.getQueueUrl());
        environment.put("INVOICE_QUEUE_URL", invoiceQueue.getQueueUrl());
        
        // Add OpenTelemetry environment variables if enabled
        if (enableOtel) {
            environment.put("OTEL_ENABLED", "true");
            environment.put("DT_OTEL_ENDPOINT", dtOtelEndpoint);
            environment.put("DT_API_TOKEN", dtApiToken);
            environment.put("OTEL_SERVICE_NAME", "billing-app-bill-processor");
            environment.put("OTEL_RESOURCE_ATTRIBUTES", "service.name=billing-app-bill-processor,service.version=1.0.0");
        }
        
        // Create the function builder
        FunctionProps.Builder functionBuilder = FunctionProps.builder()
                .functionName(prefix + "BillProcessingFunction")
                .runtime(Runtime.JAVA_11)
                .code(Code.fromAsset("target/billing-app-1.0-SNAPSHOT.jar"))
                .handler("com.example.billingapp.functions.BillProcessingFunction::handleRequest")
                .memorySize(512)
                .timeout(Duration.seconds(60))
                .environment(environment)
                .logRetention(RetentionDays.ONE_WEEK);
        
        // Add Dynatrace Lambda layer if enabled
        if (enableOtel) {
            functionBuilder.layers(List.of(LayerVersion.fromLayerVersionArn(this, 
                    prefix + "DynatraceLayer-BillProc", dtLambdaLayerArn)));
        }
        
        // Create the function
        Function function = new Function(this, prefix + "BillProcessingFunction", functionBuilder.build());
        
        // Create dtconfig.json file in the function's root directory if enabled
        if (enableOtel) {
            try {
                // Copy dtconfig.json to the function's code directory
                function.addEnvironment("DT_CONFIG_FILE_PATH", "/var/task/dtconfig.json");
            } catch (Exception e) {
                System.err.println("Error setting up Dynatrace configuration: " + e.getMessage());
            }
        }
        
        return function;
    }
    
    /**
     * Create the invoice processing Lambda function.
     * 
     * @param billingTable The billing table
     * @param invoiceTable The invoice table
     * @param invoiceQueue The invoice queue
     * @param enableOtel Whether to enable OpenTelemetry
     * @return The created function
     */
    private Function createInvoiceProcessingFunction(Table billingTable, Table invoiceTable, Queue invoiceQueue, boolean enableOtel) {
        String prefix = enableOtel ? "Otel" : "";
        
        Map<String, String> environment = new HashMap<>();
        environment.put("BILLING_TABLE_NAME", billingTable.getTableName());
        environment.put("INVOICE_TABLE_NAME", invoiceTable.getTableName());
        environment.put("INVOICE_QUEUE_URL", invoiceQueue.getQueueUrl());
        
        // Add OpenTelemetry environment variables if enabled
        if (enableOtel) {
            environment.put("OTEL_ENABLED", "true");
            environment.put("DT_OTEL_ENDPOINT", dtOtelEndpoint);
            environment.put("DT_API_TOKEN", dtApiToken);
            environment.put("OTEL_SERVICE_NAME", "billing-app-invoice-processor");
            environment.put("OTEL_RESOURCE_ATTRIBUTES", "service.name=billing-app-invoice-processor,service.version=1.0.0");
        }
        
        // Create the function builder
        FunctionProps.Builder functionBuilder = FunctionProps.builder()
                .functionName(prefix + "InvoiceProcessingFunction")
                .runtime(Runtime.JAVA_11)
                .code(Code.fromAsset("target/billing-app-1.0-SNAPSHOT.jar"))
                .handler("com.example.billingapp.functions.InvoiceProcessingFunction::handleRequest")
                .memorySize(512)
                .timeout(Duration.seconds(300))
                .environment(environment)
                .logRetention(RetentionDays.ONE_WEEK);
        
        // Add Dynatrace Lambda layer if enabled
        if (enableOtel) {
            functionBuilder.layers(List.of(LayerVersion.fromLayerVersionArn(this, 
                    prefix + "DynatraceLayer-InvoiceProc", dtLambdaLayerArn)));
        }
        
        // Create the function
        Function function = new Function(this, prefix + "InvoiceProcessingFunction", functionBuilder.build());
        
        // Create dtconfig.json file in the function's root directory if enabled
        if (enableOtel) {
            try {
                // Copy dtconfig.json to the function's code directory
                function.addEnvironment("DT_CONFIG_FILE_PATH", "/var/task/dtconfig.json");
            } catch (Exception e) {
                System.err.println("Error setting up Dynatrace configuration: " + e.getMessage());
            }
        }
        
        return function;
    }
}
