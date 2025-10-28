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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides structured logging and performance metrics for Databricks connector operations.
 * Includes connection tracking, query performance metrics, and operation timing.
 */
public class DatabricksMetrics
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksMetrics.class);
    
    // MDC keys for structured logging
    public static final String MDC_OPERATION = "databricks.operation";
    public static final String MDC_CATALOG = "databricks.catalog";
    public static final String MDC_SCHEMA = "databricks.schema";
    public static final String MDC_TABLE = "databricks.table";
    public static final String MDC_QUERY_ID = "databricks.queryId";
    public static final String MDC_DURATION_MS = "databricks.durationMs";
    public static final String MDC_ROW_COUNT = "databricks.rowCount";
    public static final String MDC_PARTITION_COUNT = "databricks.partitionCount";
    public static final String MDC_CONNECTION_ID = "databricks.connectionId";
    
    // Performance counters
    private static final AtomicLong connectionCount = new AtomicLong(0);
    private static final AtomicLong queryCount = new AtomicLong(0);
    private static final AtomicLong errorCount = new AtomicLong(0);
    private static final AtomicLong totalQueryTime = new AtomicLong(0);
    private static final AtomicLong totalRowsProcessed = new AtomicLong(0);
    
    // Operation timing tracking
    private static final Map<String, Long> operationStartTimes = new ConcurrentHashMap<>();
    
    /**
     * Logs connection attempt with structured data.
     * 
     * @param catalog The catalog being connected to
     * @param connectionId Unique connection identifier
     */
    public static void logConnectionAttempt(String catalog, String connectionId)
    {
        try {
            MDC.put(MDC_OPERATION, "connection");
            MDC.put(MDC_CATALOG, catalog);
            MDC.put(MDC_CONNECTION_ID, connectionId);
            
            long count = connectionCount.incrementAndGet();
            LOGGER.info("Attempting Databricks connection #{} to catalog: {}", count, catalog);
        }
        finally {
            clearMDC();
        }
    }
    
    /**
     * Logs successful connection with timing information.
     * 
     * @param catalog The catalog connected to
     * @param connectionId Unique connection identifier
     * @param durationMs Connection establishment time in milliseconds
     */
    public static void logConnectionSuccess(String catalog, String connectionId, long durationMs)
    {
        try {
            MDC.put(MDC_OPERATION, "connection");
            MDC.put(MDC_CATALOG, catalog);
            MDC.put(MDC_CONNECTION_ID, connectionId);
            MDC.put(MDC_DURATION_MS, String.valueOf(durationMs));
            
            LOGGER.info("Successfully established Databricks connection to catalog: {} in {}ms", catalog, durationMs);
        }
        finally {
            clearMDC();
        }
    }
    
    /**
     * Logs connection failure with error details.
     * 
     * @param catalog The catalog that failed to connect
     * @param connectionId Unique connection identifier
     * @param durationMs Time spent attempting connection
     * @param error The error that occurred
     */
    public static void logConnectionFailure(String catalog, String connectionId, long durationMs, String error)
    {
        try {
            MDC.put(MDC_OPERATION, "connection");
            MDC.put(MDC_CATALOG, catalog);
            MDC.put(MDC_CONNECTION_ID, connectionId);
            MDC.put(MDC_DURATION_MS, String.valueOf(durationMs));
            
            errorCount.incrementAndGet();
            LOGGER.error("Failed to establish Databricks connection to catalog: {} after {}ms - {}", catalog, durationMs, error);
        }
        finally {
            clearMDC();
        }
    }
    
    /**
     * Logs query execution start.
     * 
     * @param queryId Unique query identifier
     * @param catalog The catalog being queried
     * @param schema The schema being queried
     * @param table The table being queried
     * @param operation The type of operation (metadata, record, etc.)
     */
    public static void logQueryStart(String queryId, String catalog, String schema, String table, String operation)
    {
        try {
            MDC.put(MDC_OPERATION, operation);
            MDC.put(MDC_CATALOG, catalog);
            MDC.put(MDC_SCHEMA, schema);
            MDC.put(MDC_TABLE, table);
            MDC.put(MDC_QUERY_ID, queryId);
            
            long count = queryCount.incrementAndGet();
            String operationKey = queryId + ":" + operation;
            operationStartTimes.put(operationKey, System.currentTimeMillis());
            
            LOGGER.info("Starting Databricks {} operation #{} for {}.{}.{}", operation, count, catalog, schema, table);
        }
        finally {
            clearMDC();
        }
    }
    
    /**
     * Logs successful query completion with performance metrics.
     * 
     * @param queryId Unique query identifier
     * @param catalog The catalog queried
     * @param schema The schema queried
     * @param table The table queried
     * @param operation The type of operation
     * @param rowCount Number of rows processed
     * @param partitionCount Number of partitions processed (if applicable)
     */
    public static void logQuerySuccess(String queryId, String catalog, String schema, String table, 
                                     String operation, long rowCount, int partitionCount)
    {
        try {
            String operationKey = queryId + ":" + operation;
            Long startTime = operationStartTimes.remove(operationKey);
            long durationMs = startTime != null ? System.currentTimeMillis() - startTime : 0;
            
            MDC.put(MDC_OPERATION, operation);
            MDC.put(MDC_CATALOG, catalog);
            MDC.put(MDC_SCHEMA, schema);
            MDC.put(MDC_TABLE, table);
            MDC.put(MDC_QUERY_ID, queryId);
            MDC.put(MDC_DURATION_MS, String.valueOf(durationMs));
            MDC.put(MDC_ROW_COUNT, String.valueOf(rowCount));
            if (partitionCount > 0) {
                MDC.put(MDC_PARTITION_COUNT, String.valueOf(partitionCount));
            }
            
            totalQueryTime.addAndGet(durationMs);
            totalRowsProcessed.addAndGet(rowCount);
            
            LOGGER.info("Completed Databricks {} operation for {}.{}.{} - Duration: {}ms, Rows: {}, Partitions: {}", 
                       operation, catalog, schema, table, durationMs, rowCount, partitionCount);
        }
        finally {
            clearMDC();
        }
    }
    
    /**
     * Logs query failure with error details.
     * 
     * @param queryId Unique query identifier
     * @param catalog The catalog being queried
     * @param schema The schema being queried
     * @param table The table being queried
     * @param operation The type of operation
     * @param error The error that occurred
     */
    public static void logQueryFailure(String queryId, String catalog, String schema, String table, 
                                     String operation, String error)
    {
        try {
            String operationKey = queryId + ":" + operation;
            Long startTime = operationStartTimes.remove(operationKey);
            long durationMs = startTime != null ? System.currentTimeMillis() - startTime : 0;
            
            MDC.put(MDC_OPERATION, operation);
            MDC.put(MDC_CATALOG, catalog);
            MDC.put(MDC_SCHEMA, schema);
            MDC.put(MDC_TABLE, table);
            MDC.put(MDC_QUERY_ID, queryId);
            MDC.put(MDC_DURATION_MS, String.valueOf(durationMs));
            
            errorCount.incrementAndGet();
            
            LOGGER.error("Failed Databricks {} operation for {}.{}.{} after {}ms - {}", 
                        operation, catalog, schema, table, durationMs, error);
        }
        finally {
            clearMDC();
        }
    }
    
    /**
     * Logs partition discovery metrics.
     * 
     * @param queryId Unique query identifier
     * @param schema The schema being queried
     * @param table The table being queried
     * @param partitionCount Number of partitions discovered
     * @param durationMs Time taken to discover partitions
     */
    public static void logPartitionDiscovery(String queryId, String schema, String table, 
                                           int partitionCount, long durationMs)
    {
        try {
            MDC.put(MDC_OPERATION, "partition_discovery");
            MDC.put(MDC_SCHEMA, schema);
            MDC.put(MDC_TABLE, table);
            MDC.put(MDC_QUERY_ID, queryId);
            MDC.put(MDC_PARTITION_COUNT, String.valueOf(partitionCount));
            MDC.put(MDC_DURATION_MS, String.valueOf(durationMs));
            
            LOGGER.info("Discovered {} partitions for {}.{} in {}ms", partitionCount, schema, table, durationMs);
        }
        finally {
            clearMDC();
        }
    }
    
    /**
     * Logs data type conversion metrics.
     * 
     * @param fieldName The field being converted
     * @param sourceType The source data type
     * @param targetType The target Arrow type
     * @param conversionTime Time taken for conversion in nanoseconds
     */
    public static void logDataTypeConversion(String fieldName, String sourceType, String targetType, long conversionTime)
    {
        if (LOGGER.isTraceEnabled()) {
            try {
                MDC.put(MDC_OPERATION, "data_conversion");
                
                LOGGER.trace("Converted field {} from {} to {} in {}ns", fieldName, sourceType, targetType, conversionTime);
            }
            finally {
                clearMDC();
            }
        }
    }
    
    /**
     * Logs performance summary statistics.
     */
    public static void logPerformanceSummary()
    {
        try {
            MDC.put(MDC_OPERATION, "performance_summary");
            
            long connections = connectionCount.get();
            long queries = queryCount.get();
            long errors = errorCount.get();
            long totalTime = totalQueryTime.get();
            long totalRows = totalRowsProcessed.get();
            
            double avgQueryTime = queries > 0 ? (double) totalTime / queries : 0;
            double errorRate = queries > 0 ? (double) errors / queries * 100 : 0;
            double throughput = totalTime > 0 ? (double) totalRows / (totalTime / 1000.0) : 0;
            
            LOGGER.info("Databricks Connector Performance Summary - Connections: {}, Queries: {}, Errors: {} ({:.2f}%), " +
                       "Avg Query Time: {:.2f}ms, Total Rows: {}, Throughput: {:.2f} rows/sec", 
                       connections, queries, errors, errorRate, avgQueryTime, totalRows, throughput);
        }
        finally {
            clearMDC();
        }
    }
    
    /**
     * Gets current performance metrics as a map.
     * 
     * @return Map containing current performance metrics
     */
    public static Map<String, Object> getMetrics()
    {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("connectionCount", connectionCount.get());
        metrics.put("queryCount", queryCount.get());
        metrics.put("errorCount", errorCount.get());
        metrics.put("totalQueryTimeMs", totalQueryTime.get());
        metrics.put("totalRowsProcessed", totalRowsProcessed.get());
        
        long queries = queryCount.get();
        if (queries > 0) {
            metrics.put("avgQueryTimeMs", (double) totalQueryTime.get() / queries);
            metrics.put("errorRatePercent", (double) errorCount.get() / queries * 100);
        }
        
        return metrics;
    }
    
    /**
     * Resets all performance counters (useful for testing).
     */
    public static void resetMetrics()
    {
        connectionCount.set(0);
        queryCount.set(0);
        errorCount.set(0);
        totalQueryTime.set(0);
        totalRowsProcessed.set(0);
        operationStartTimes.clear();
    }
    
    /**
     * Clears all MDC keys to prevent leakage between operations.
     */
    private static void clearMDC()
    {
        MDC.remove(MDC_OPERATION);
        MDC.remove(MDC_CATALOG);
        MDC.remove(MDC_SCHEMA);
        MDC.remove(MDC_TABLE);
        MDC.remove(MDC_QUERY_ID);
        MDC.remove(MDC_DURATION_MS);
        MDC.remove(MDC_ROW_COUNT);
        MDC.remove(MDC_PARTITION_COUNT);
        MDC.remove(MDC_CONNECTION_ID);
    }
}