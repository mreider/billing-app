#!/bin/bash

# Serverless Billing System with Dynatrace Monitoring - Deployment Script

# Check if configuration files exist and create them if they don't
if [ ! -f dt-config.env ]; then
    echo "dt-config.env file not found. Creating from template..."
    cp dt-config-template.env dt-config.env
    echo "Please edit dt-config.env with your Dynatrace configuration before deploying."
    exit 1
fi

if [ ! -f dtconfig.json ]; then
    echo "dtconfig.json file not found. Creating from template..."
    cp dtconfig-template.json dtconfig.json
    echo "Please edit dtconfig.json with your Dynatrace configuration before deploying."
    exit 1
fi

# Source the Dynatrace configuration
echo "Loading Dynatrace configuration..."
source dt-config.env

# Build the application
echo "Building the application..."
mvn clean package

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Error: Maven build failed."
    exit 1
fi

# Deploy the application using CDK
echo "Deploying the application using CDK..."
cdk deploy

echo "Deployment complete!"
echo ""
echo "You can now test the application by invoking the Lambda functions:"
echo ""
echo "Standard version:"
echo "aws lambda invoke --function-name LoadGenerationFunction --payload '{\"requestCount\": 100, \"concurrency\": 10, \"requestDelayMs\": 100, \"currency\": \"USD\"}' response.json"
echo ""
echo "Dynatrace-instrumented version:"
echo "aws lambda invoke --function-name OtelLoadGenerationFunction --payload '{\"requestCount\": 100, \"concurrency\": 10, \"requestDelayMs\": 100, \"currency\": \"USD\"}' response.json"
echo ""
echo "To view the Dynatrace monitoring data, log in to your Dynatrace dashboard."
