# Serverless Billing System with Dynatrace Monitoring

A complete serverless billing system built on AWS Lambda, SQS, and DynamoDB with Dynatrace monitoring integration. This application demonstrates the fan-out/fan-in pattern, batch processing, and event-driven architecture using AWS serverless services.

## Architecture

The system consists of the following components, with both standard and OpenTelemetry-instrumented versions:

1. **Load Generation Function**: Simulates client traffic by sending HTTP requests to the billing function with configurable parameters for request volume and frequency.

2. **Billing Function**: Receives HTTP requests, stores initial billing data in DynamoDB, and publishes messages to an SQS queue for further processing.

3. **Bill Processing Function**: Triggered by messages from the SQS queue, retrieves and updates billing records in DynamoDB, and implements retry logic with error handling.

4. **Invoice Processing Function**: Processes invoices in configurable batch sizes, implements a windowing mechanism to group related invoices, uses SQS batch processing capabilities, and stores final aggregated data in DynamoDB.

5. **Infrastructure as Code**: AWS CDK deployment script that creates all required resources and configures appropriate permissions.

## Architecture Diagram

```
┌─────────────────┐     HTTP     ┌─────────────────┐    SQS     ┌─────────────────┐
│ Load Generation │───────────────│ Billing Function│─────────────│Bill Processing  │
│    Function     │               │                 │             │    Function     │
└─────────────────┘               └─────────────────┘             └─────────────────┘
                                         │                               │
                                         │                               │
                                         ▼                               ▼
                                  ┌─────────────────┐           ┌─────────────────┐
                                  │   DynamoDB      │           │      SQS        │
                                  │ Billing Table   │           │  Invoice Queue  │
                                  └─────────────────┘           └─────────────────┘
                                                                        │
                                                                        │
                                                                        ▼
                                                               ┌─────────────────┐
                                                               │Invoice Processing│
                                                               │    Function     │
                                                               └─────────────────┘
                                                                        │
                                                                        │
                                                                        ▼
                                                               ┌─────────────────┐
                                                               │    DynamoDB     │
                                                               │  Invoice Table  │
                                                               └─────────────────┘
```

## Prerequisites

- Java 11 or later
- Maven
- AWS CLI configured with appropriate credentials
- AWS CDK installed
- Dynatrace account with access to API tokens

## Building the Application

To build the application, run the following command:

```bash
mvn clean package
```

This will compile the Java code and create a JAR file in the `target` directory.

## Deploying the Application

To deploy the application to AWS, run the following commands:

```bash
# Bootstrap the CDK (only needed once per AWS account/region)
cdk bootstrap

# Deploy the application
cdk deploy
```

The CDK deployment will create all the necessary AWS resources, including Lambda functions, DynamoDB tables, SQS queues, and IAM roles. The deployment includes both standard and OpenTelemetry-instrumented versions of all components.

### Dynatrace Configuration

The application is deployed with two sets of resources:

1. **Standard Resources**: Lambda functions, DynamoDB tables, and SQS queues without Dynatrace instrumentation.
2. **Dynatrace-Instrumented Resources**: Lambda functions, DynamoDB tables, and SQS queues with Dynatrace instrumentation enabled.

The Dynatrace-instrumented resources have the prefix "Otel" in their names to distinguish them from the standard resources.

To use the Dynatrace-instrumented version, you need to:

1. Set up a Dynatrace account and obtain the necessary API tokens.
2. Create a `dt-config.env` file based on the provided template.
3. Create a `dtconfig.json` file based on the provided template.

#### Setting up Dynatrace Configuration

1. Copy the template files:
   ```bash
   cp dt-config-template.env dt-config.env
   cp dtconfig-template.json dtconfig.json
   ```

2. Edit the `dt-config.env` file with your Dynatrace tenant information and API token:
   ```
   DT_OTEL_ENDPOINT=https://your-tenant-id.live.dynatrace.com/api/v2/otlp
   DT_API_TOKEN=your-api-token
   DT_TENANT_ID=your-tenant-id
   DT_TENANT_URL=https://your-tenant-id.live.dynatrace.com
   DT_CONNECTION_AUTH=Api-Token your-api-token
   DT_CONNECTION_BASE_URL=https://your-tenant-id.live.dynatrace.com
   ```

3. Edit the `dtconfig.json` file with your Dynatrace tenant information:
   ```json
   {
     "tenant": {
       "id": "your-tenant-id",
       "url": "https://your-tenant-id.live.dynatrace.com"
     },
     "connection": {
       "auth": "Api-Token your-api-token",
       "baseUrl": "https://your-tenant-id.live.dynatrace.com"
     },
     "options": {
       "debug": false,
       "agentPath": "/opt/dynatrace",
       "logPath": "/tmp/dynatrace_log",
       "tracingEnabled": true,
       "metricsEnabled": true,
       "logAnalyticsEnabled": true
     }
   }
   ```

4. Set the environment variables before deploying:
   ```bash
   source dt-config.env
   export DT_LAMBDA_LAYER_ARN="arn:aws:lambda:your-region:725887861453:layer:Dynatrace:latest"
   ```

## Testing the Application

After deployment, you can test the application by invoking either the standard or OpenTelemetry-instrumented Load Generation Function:

### Standard Version
```bash
aws lambda invoke --function-name LoadGenerationFunction --payload '{"requestCount": 100, "concurrency": 10, "requestDelayMs": 100, "currency": "USD"}' response.json
```

### OpenTelemetry-Instrumented Version
```bash
aws lambda invoke --function-name OtelLoadGenerationFunction --payload '{"requestCount": 100, "concurrency": 10, "requestDelayMs": 100, "currency": "USD"}' response.json
```

This will simulate 100 billing requests with a concurrency of 10 and a delay of 100ms between requests.

You can then check the DynamoDB tables to see the billing records and invoices:

#### Standard Tables
```bash
# List billing records
aws dynamodb scan --table-name BillingRecords --select COUNT

# List invoices
aws dynamodb scan --table-name Invoices --select COUNT
```

#### OpenTelemetry-Instrumented Tables
```bash
# List billing records
aws dynamodb scan --table-name OtelBillingRecords --select COUNT

# List invoices
aws dynamodb scan --table-name OtelInvoices --select COUNT
```

## Configuration Parameters

### Load Generation Function

- `requestCount`: The number of requests to generate (default: 100)
- `concurrency`: The number of concurrent requests (default: 10)
- `requestDelayMs`: The delay between requests in milliseconds (default: 100)
- `currency`: The currency to use for the billing records (default: "USD")

### Invoice Processing Function

- `batchSize`: The maximum number of messages to process in a batch (default: 10)
- `windowSizeMinutes`: The size of the time window for grouping invoices in minutes (default: 5)
- `timeoutSeconds`: The timeout for processing in seconds (default: 60)

## Error Handling and Retry Logic

The system implements comprehensive error handling and retry logic:

1. **SQS Dead-Letter Queues**: Messages that fail processing after multiple attempts are sent to dead-letter queues for further analysis.

2. **Retry Logic in Bill Processing**: The Bill Processing Function implements retry logic for failed billing records, with a configurable maximum retry count.

3. **Error Logging**: All components include detailed error logging to help diagnose issues.

## Monitoring and Logging

All Lambda functions are configured with CloudWatch Logs for monitoring and troubleshooting. You can view the logs in the AWS Console or using the AWS CLI:

```bash
aws logs get-log-events --log-group-name /aws/lambda/BillingFunction --log-stream-name <log-stream-name>
```

## Cleaning Up

To avoid incurring charges, you can delete all the resources created by the CDK:

```bash
cdk destroy
```

## Dynatrace Instrumentation

The application includes comprehensive Dynatrace instrumentation for:

1. **Lambda Functions**: All Lambda functions are instrumented using the Dynatrace Lambda Layer.
2. **DynamoDB Operations**: All DynamoDB operations are traced with detailed attributes.
3. **SQS Operations**: All SQS operations are traced with detailed attributes.

The instrumentation is controlled by the `OTEL_ENABLED` environment variable, which is set to `true` for the Dynatrace-instrumented resources and `false` for the standard resources.

### Viewing Telemetry Data

To view the telemetry data, log in to your Dynatrace dashboard. You'll be able to see:

1. **Distributed Traces**: End-to-end traces of requests flowing through your Lambda functions, DynamoDB, and SQS.
2. **Service Flow**: Visual representation of the interactions between services.
3. **Database Monitoring**: Performance metrics for DynamoDB operations.
4. **Queue Monitoring**: Performance metrics for SQS operations.
5. **Lambda Function Monitoring**: Performance metrics for Lambda functions.

### Dynatrace Lambda Layer

The application uses the Dynatrace Lambda Layer for instrumenting AWS Lambda functions. The layer is automatically added to the Lambda functions during deployment. The layer ARN is specified in the `DT_LAMBDA_LAYER_ARN` environment variable.

## Development

### Project Structure

- `src/main/java/com/example/billingapp/models/`: Data models
- `src/main/java/com/example/billingapp/functions/`: Lambda functions
- `src/main/java/com/example/billingapp/utils/`: Utility classes
  - `DynamoDBUtil.java`: DynamoDB operations with OpenTelemetry instrumentation
  - `SQSUtil.java`: SQS operations with OpenTelemetry instrumentation
  - `OpenTelemetryUtil.java`: OpenTelemetry configuration and utilities
- `src/main/java/com/example/billingapp/infrastructure/`: CDK infrastructure code

### Adding New Features

To add new features to the application:

1. Implement the necessary Java code in the appropriate package
2. Update the CDK infrastructure code if needed
3. Build and deploy the application

## License

This project is licensed under the MIT License - see the LICENSE file for details.
