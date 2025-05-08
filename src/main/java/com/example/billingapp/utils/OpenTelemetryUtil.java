package com.example.billingapp.utils;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.contrib.awsxray.AwsXrayIdGenerator;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.instrumentation.awslambda.v1_0.AwsLambdaTracing;
import io.opentelemetry.instrumentation.awssdk.v2_2.AwsSdkTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Utility class for OpenTelemetry configuration and instrumentation.
 */
public class OpenTelemetryUtil {
    private static final Logger logger = LoggerFactory.getLogger(OpenTelemetryUtil.class);
    
    // Feature flag to enable/disable OpenTelemetry
    private static final boolean OTEL_ENABLED = Boolean.parseBoolean(
            Optional.ofNullable(System.getenv("OTEL_ENABLED")).orElse("false"));
    
    // Dynatrace OpenTelemetry endpoint
    private static final String OTEL_EXPORTER_OTLP_ENDPOINT = 
            Optional.ofNullable(System.getenv("DT_OTEL_ENDPOINT"))
                    .orElse("http://localhost:4317");
    
    // Dynatrace API token
    private static final String DT_API_TOKEN = 
            Optional.ofNullable(System.getenv("DT_API_TOKEN"))
                    .orElse("");
    
    // Service name
    private static final String SERVICE_NAME = 
            Optional.ofNullable(System.getenv("OTEL_SERVICE_NAME"))
                    .orElse("billing-app");
    
    // Singleton instance of OpenTelemetry
    private static OpenTelemetry openTelemetry;
    
    // Singleton instance of Tracer
    private static Tracer tracer;
    
    // Cache for active spans
    private static final ConcurrentHashMap<String, Span> activeSpans = new ConcurrentHashMap<>();
    
    /**
     * Initialize OpenTelemetry.
     * 
     * @return The OpenTelemetry instance
     */
    public static synchronized OpenTelemetry initOpenTelemetry() {
        if (openTelemetry != null) {
            return openTelemetry;
        }
        
        if (!OTEL_ENABLED) {
            logger.info("OpenTelemetry is disabled");
            openTelemetry = OpenTelemetry.noop();
            return openTelemetry;
        }
        
        try {
            logger.info("Initializing OpenTelemetry with endpoint: {}", OTEL_EXPORTER_OTLP_ENDPOINT);
            
            // Create a resource with service information
            Resource resource = Resource.getDefault()
                    .merge(Resource.create(Attributes.of(
                            ResourceAttributes.SERVICE_NAME, SERVICE_NAME,
                            ResourceAttributes.SERVICE_VERSION, "1.0.0"
                    )));
            
            // Create the OTLP exporter with Dynatrace-specific configuration
            OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                    .setEndpoint(OTEL_EXPORTER_OTLP_ENDPOINT)
                    .addHeader("Authorization", "Api-Token " + DT_API_TOKEN)
                    .build();
            
            // Create the tracer provider
            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .setResource(resource)
                    .setSampler(Sampler.alwaysOn())
                    .setIdGenerator(AwsXrayIdGenerator.getInstance())
                    .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                    .build();
            
            // Create the OpenTelemetry SDK
            openTelemetry = OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                    .build();
            
            // Register a shutdown hook to close the tracer provider
            Runtime.getRuntime().addShutdownHook(new Thread(tracerProvider::close));
            
            logger.info("OpenTelemetry initialized successfully");
            return openTelemetry;
        } catch (Exception e) {
            logger.error("Error initializing OpenTelemetry: {}", e.getMessage(), e);
            openTelemetry = OpenTelemetry.noop();
            return openTelemetry;
        }
    }
    
    /**
     * Get the OpenTelemetry instance.
     * 
     * @return The OpenTelemetry instance
     */
    public static OpenTelemetry getOpenTelemetry() {
        if (openTelemetry == null) {
            return initOpenTelemetry();
        }
        return openTelemetry;
    }
    
    /**
     * Get the Tracer instance.
     * 
     * @return The Tracer instance
     */
    public static Tracer getTracer() {
        if (tracer == null) {
            tracer = getOpenTelemetry().getTracer(SERVICE_NAME);
        }
        return tracer;
    }
    
    /**
     * Check if OpenTelemetry is enabled.
     * 
     * @return true if OpenTelemetry is enabled, false otherwise
     */
    public static boolean isEnabled() {
        return OTEL_ENABLED;
    }
    
    /**
     * Create a new span.
     * 
     * @param spanName The name of the span
     * @return The span
     */
    public static Span createSpan(String spanName) {
        return getTracer().spanBuilder(spanName)
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
    }
    
    /**
     * Create a new span and make it the current span.
     * 
     * @param spanName The name of the span
     * @return The scope, which should be closed when the span is complete
     */
    public static Scope withSpan(String spanName) {
        Span span = createSpan(spanName);
        activeSpans.put(spanName, span);
        return span.makeCurrent();
    }
    
    /**
     * End a span by name.
     * 
     * @param spanName The name of the span
     */
    public static void endSpan(String spanName) {
        Span span = activeSpans.remove(spanName);
        if (span != null) {
            span.end();
        }
    }
    
    /**
     * Run a function within a span.
     * 
     * @param spanName The name of the span
     * @param function The function to run
     */
    public static void runWithSpan(String spanName, Runnable function) {
        if (!OTEL_ENABLED) {
            function.run();
            return;
        }
        
        Span span = createSpan(spanName);
        try (Scope scope = span.makeCurrent()) {
            function.run();
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Run a function within a span and return the result.
     * 
     * @param spanName The name of the span
     * @param function The function to run
     * @param <T> The return type of the function
     * @return The result of the function
     */
    public static <T> T runWithSpan(String spanName, java.util.function.Supplier<T> function) {
        if (!OTEL_ENABLED) {
            return function.get();
        }
        
        Span span = createSpan(spanName);
        try (Scope scope = span.makeCurrent()) {
            return function.get();
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Add an attribute to the current span.
     * 
     * @param key The attribute key
     * @param value The attribute value
     */
    public static void addAttribute(String key, String value) {
        if (!OTEL_ENABLED) {
            return;
        }
        
        Span span = Span.current();
        if (span.isRecording()) {
            span.setAttribute(key, value);
        }
    }
    
    /**
     * Add an attribute to the current span.
     * 
     * @param key The attribute key
     * @param value The attribute value
     */
    public static void addAttribute(String key, long value) {
        if (!OTEL_ENABLED) {
            return;
        }
        
        Span span = Span.current();
        if (span.isRecording()) {
            span.setAttribute(key, value);
        }
    }
    
    /**
     * Add an attribute to the current span.
     * 
     * @param key The attribute key
     * @param value The attribute value
     */
    public static void addAttribute(String key, double value) {
        if (!OTEL_ENABLED) {
            return;
        }
        
        Span span = Span.current();
        if (span.isRecording()) {
            span.setAttribute(key, value);
        }
    }
    
    /**
     * Add an attribute to the current span.
     * 
     * @param key The attribute key
     * @param value The attribute value
     */
    public static void addAttribute(String key, boolean value) {
        if (!OTEL_ENABLED) {
            return;
        }
        
        Span span = Span.current();
        if (span.isRecording()) {
            span.setAttribute(key, value);
        }
    }
    
    /**
     * Add an event to the current span.
     * 
     * @param name The event name
     */
    public static void addEvent(String name) {
        if (!OTEL_ENABLED) {
            return;
        }
        
        Span span = Span.current();
        if (span.isRecording()) {
            span.addEvent(name);
        }
    }
    
    /**
     * Record an exception in the current span.
     * 
     * @param exception The exception to record
     */
    public static void recordException(Throwable exception) {
        if (!OTEL_ENABLED) {
            return;
        }
        
        Span span = Span.current();
        if (span.isRecording()) {
            span.recordException(exception);
        }
    }
    
    /**
     * Instrument a DynamoDB client builder with OpenTelemetry.
     * 
     * @param builder The DynamoDB client builder
     * @return The instrumented DynamoDB client builder
     */
    public static DynamoDbClientBuilder instrumentDynamoDbClientBuilder(DynamoDbClientBuilder builder) {
        if (!OTEL_ENABLED) {
            return builder;
        }
        
        return builder.overrideConfiguration(
                ClientOverrideConfiguration.builder()
                        .addExecutionInterceptor(
                                AwsSdkTelemetry.create(getOpenTelemetry()).newExecutionInterceptor())
                        .build());
    }
    
    /**
     * Instrument an SQS client builder with OpenTelemetry.
     * 
     * @param builder The SQS client builder
     * @return The instrumented SQS client builder
     */
    public static SqsClientBuilder instrumentSqsClientBuilder(SqsClientBuilder builder) {
        if (!OTEL_ENABLED) {
            return builder;
        }
        
        return builder.overrideConfiguration(
                ClientOverrideConfiguration.builder()
                        .addExecutionInterceptor(
                                AwsSdkTelemetry.create(getOpenTelemetry()).newExecutionInterceptor())
                        .build());
    }
    
    /**
     * Get the AWS Lambda tracing wrapper for OpenTelemetry.
     * 
     * @return The AWS Lambda tracing wrapper
     */
    public static AwsLambdaTracing getLambdaTracing() {
        if (!OTEL_ENABLED) {
            return AwsLambdaTracing.create(OpenTelemetry.noop());
        }
        
        return AwsLambdaTracing.create(getOpenTelemetry());
    }
    
    /**
     * Extract the current context from the Lambda context.
     * 
     * @param context The Lambda context
     * @return The OpenTelemetry context
     */
    public static Context extractContext(com.amazonaws.services.lambda.runtime.Context context) {
        if (!OTEL_ENABLED) {
            return Context.current();
        }
        
        return getLambdaTracing().extractContext(context);
    }
}
