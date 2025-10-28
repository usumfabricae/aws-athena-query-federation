/*-
 * #%L
 * athena-databricks
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.databricks;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Handles error mapping and retry logic for Databricks connector operations.
 * Maps Databricks-specific errors to appropriate Athena error codes and provides
 * retry mechanisms for transient failures.
 */
public class DatabricksErrorHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksErrorHandler.class);
    
    // Retry configuration constants
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 1000; // 1 second
    private static final double RETRY_BACKOFF_MULTIPLIER = 2.0;
    private static final long MAX_RETRY_DELAY_MS = 30000; // 30 seconds
    
    // Databricks error patterns for classification
    private static final String[] CONNECTION_ERROR_PATTERNS = {
        "Connection refused",
        "Connection timed out",
        "Network is unreachable",
        "No route to host",
        "Connection reset",
        "Unable to connect"
    };
    
    private static final String[] AUTHENTICATION_ERROR_PATTERNS = {
        "Authentication failed",
        "Invalid token",
        "Access denied",
        "Unauthorized",
        "Invalid credentials",
        "Token expired"
    };
    
    private static final String[] TRANSIENT_ERROR_PATTERNS = {
        "Temporary failure",
        "Service unavailable",
        "Too many requests",
        "Rate limit exceeded",
        "Cluster is starting",
        "Cluster is terminating",
        "Resource temporarily unavailable"
    };
    
    private static final String[] RESOURCE_ERROR_PATTERNS = {
        "Table not found",
        "Schema not found",
        "Database not found",
        "Column not found",
        "Catalog not found"
    };

    /**
     * Maps Databricks SQLException to appropriate AthenaConnectorException with proper error codes.
     * 
     * @param operation The operation that failed (for logging context)
     * @param sqlException The original SQLException from Databricks
     * @return AthenaConnectorException with appropriate error code
     */
    public static AthenaConnectorException mapSqlException(String operation, SQLException sqlException)
    {
        String errorMessage = sqlException.getMessage();
        String sqlState = sqlException.getSQLState();
        int errorCode = sqlException.getErrorCode();
        
        LOGGER.error("Databricks SQL error during {}: SQLState={}, ErrorCode={}, Message={}", 
                    operation, sqlState, errorCode, errorMessage, sqlException);
        
        // Map based on error message patterns
        FederationSourceErrorCode federationErrorCode = classifyError(errorMessage, sqlState, errorCode);
        
        String enhancedMessage = String.format("Databricks %s failed: %s (SQLState: %s, ErrorCode: %d)", 
                                             operation, errorMessage, sqlState, errorCode);
        
        return new AthenaConnectorException(enhancedMessage, 
                ErrorDetails.builder()
                    .errorCode(federationErrorCode.toString())
                    .errorMessage(enhancedMessage)
                    .build());
    }
    
    /**
     * Maps general Exception to appropriate AthenaConnectorException.
     * 
     * @param operation The operation that failed
     * @param exception The original exception
     * @return AthenaConnectorException with appropriate error code
     */
    public static AthenaConnectorException mapException(String operation, Exception exception)
    {
        String errorMessage = exception.getMessage();
        
        LOGGER.error("Databricks error during {}: {}", operation, errorMessage, exception);
        
        FederationSourceErrorCode federationErrorCode;
        
        if (exception instanceof SQLException) {
            return mapSqlException(operation, (SQLException) exception);
        }
        else if (exception instanceof ClassNotFoundException) {
            federationErrorCode = FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION;
        }
        else if (exception instanceof IllegalArgumentException) {
            federationErrorCode = FederationSourceErrorCode.INVALID_INPUT_EXCEPTION;
        }
        else if (exception instanceof SecurityException) {
            federationErrorCode = FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION;
        }
        else {
            federationErrorCode = FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION;
        }
        
        String enhancedMessage = String.format("Databricks %s failed: %s", operation, errorMessage);
        
        return new AthenaConnectorException(enhancedMessage,
                ErrorDetails.builder()
                    .errorCode(federationErrorCode.toString())
                    .errorMessage(enhancedMessage)
                    .build());
    }
    
    /**
     * Classifies error based on message patterns, SQL state, and error codes.
     * 
     * @param errorMessage The error message
     * @param sqlState The SQL state
     * @param errorCode The error code
     * @return Appropriate FederationSourceErrorCode
     */
    private static FederationSourceErrorCode classifyError(String errorMessage, String sqlState, int errorCode)
    {
        if (errorMessage == null) {
            return FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION;
        }
        
        String lowerMessage = errorMessage.toLowerCase();
        
        // Check for authentication errors
        for (String pattern : AUTHENTICATION_ERROR_PATTERNS) {
            if (lowerMessage.contains(pattern.toLowerCase())) {
                return FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION;
            }
        }
        
        // Check for connection errors
        for (String pattern : CONNECTION_ERROR_PATTERNS) {
            if (lowerMessage.contains(pattern.toLowerCase())) {
                return FederationSourceErrorCode.INVALID_INPUT_EXCEPTION;
            }
        }
        
        // Check for resource not found errors
        for (String pattern : RESOURCE_ERROR_PATTERNS) {
            if (lowerMessage.contains(pattern.toLowerCase())) {
                return FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION;
            }
        }
        
        // Check for transient errors
        for (String pattern : TRANSIENT_ERROR_PATTERNS) {
            if (lowerMessage.contains(pattern.toLowerCase())) {
                return FederationSourceErrorCode.THROTTLING_EXCEPTION;
            }
        }
        
        // Check SQL state patterns
        if (sqlState != null) {
            if (sqlState.startsWith("08")) { // Connection exception
                return FederationSourceErrorCode.INVALID_INPUT_EXCEPTION;
            }
            else if (sqlState.startsWith("28")) { // Invalid authorization specification
                return FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION;
            }
            else if (sqlState.startsWith("42")) { // Syntax error or access rule violation
                return FederationSourceErrorCode.INVALID_INPUT_EXCEPTION;
            }
        }
        
        // Default to internal service exception
        return FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION;
    }
    
    /**
     * Determines if an error is transient and should be retried.
     * 
     * @param exception The exception to check
     * @return true if the error is transient and should be retried
     */
    public static boolean isTransientError(Exception exception)
    {
        if (exception == null) {
            return false;
        }
        
        String errorMessage = exception.getMessage();
        if (errorMessage == null) {
            return false;
        }
        
        String lowerMessage = errorMessage.toLowerCase();
        
        // Check for transient error patterns
        for (String pattern : TRANSIENT_ERROR_PATTERNS) {
            if (lowerMessage.contains(pattern.toLowerCase())) {
                return true;
            }
        }
        
        // Check for specific SQL states that indicate transient errors
        if (exception instanceof SQLException) {
            SQLException sqlException = (SQLException) exception;
            String sqlState = sqlException.getSQLState();
            
            if (sqlState != null) {
                // Connection exceptions that might be transient
                if (sqlState.startsWith("08")) {
                    return lowerMessage.contains("timeout") || 
                           lowerMessage.contains("connection reset") ||
                           lowerMessage.contains("connection refused");
                }
            }
        }
        
        return false;
    }
    
    /**
     * Executes an operation with retry logic for transient failures.
     * 
     * @param operation The operation to execute
     * @param operationName Name of the operation for logging
     * @param <T> Return type of the operation
     * @return Result of the operation
     * @throws Exception if all retry attempts fail
     */
    public static <T> T executeWithRetry(RetryableOperation<T> operation, String operationName) throws Exception
    {
        Exception lastException = null;
        long delay = INITIAL_RETRY_DELAY_MS;
        
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                LOGGER.debug("Executing {} (attempt {} of {})", operationName, attempt, MAX_RETRY_ATTEMPTS);
                return operation.execute();
            }
            catch (Exception e) {
                lastException = e;
                
                if (attempt == MAX_RETRY_ATTEMPTS || !isTransientError(e)) {
                    LOGGER.error("Operation {} failed after {} attempts", operationName, attempt, e);
                    throw e;
                }
                
                LOGGER.warn("Transient error in {} (attempt {} of {}), retrying in {}ms: {}", 
                           operationName, attempt, MAX_RETRY_ATTEMPTS, delay, e.getMessage());
                
                try {
                    TimeUnit.MILLISECONDS.sleep(delay);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry interrupted", ie);
                }
                
                // Exponential backoff with maximum delay
                delay = Math.min((long) (delay * RETRY_BACKOFF_MULTIPLIER), MAX_RETRY_DELAY_MS);
            }
        }
        
        // This should never be reached, but just in case
        throw lastException != null ? lastException : new RuntimeException("Unknown error in retry logic");
    }
    
    /**
     * Functional interface for operations that can be retried.
     * 
     * @param <T> Return type of the operation
     */
    @FunctionalInterface
    public interface RetryableOperation<T>
    {
        T execute() throws Exception;
    }
}