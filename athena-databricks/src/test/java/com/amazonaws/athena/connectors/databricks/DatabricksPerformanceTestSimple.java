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

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Performance tests for Databricks connector that measure connection performance,
 * query execution times, and data throughput.
 * 
 * Set the following environment variables to run these tests:
 * - DATABRICKS_HOST: The Databricks cluster hostname
 * - DATABRICKS_HTTP_PATH: The HTTP path for the cluster
 * - DATABRICKS_TOKEN: Personal access token for authentication
 * - PERFORMANCE_TEST_ENABLED: Set to "true" to enable performance tests
 * 
 * If these are not set, the tests will be skipped.
 */
public class DatabricksPerformanceTestSimple
{
    private static final Logger logger = LoggerFactory.getLogger(DatabricksPerformanceTestSimple.class);
    
    private String databricksHost;
    private String databricksHttpPath;
    private String databricksToken;
    private boolean performanceTestEnabled;
    
    // Performance test thresholds (in milliseconds)
    private static final long CONNECTION_TIMEOUT_MS = 10000; // 10 seconds
    private static final long SIMPLE_QUERY_TIMEOUT_MS = 5000; // 5 seconds
    private static final long COMPLEX_QUERY_TIMEOUT_MS = 30000; // 30 seconds
    private static final int CONCURRENT_CONNECTIONS = 5;
    private static final int QUERY_ITERATIONS = 10;
    
    @Before
    public void setUp()
    {
        // Check if performance test environment variables are set
        databricksHost = System.getenv("DATABRICKS_HOST");
        databricksHttpPath = System.getenv("DATABRICKS_HTTP_PATH");
        databricksToken = System.getenv("DATABRICKS_TOKEN");
        String performanceEnabled = System.getenv("PERFORMANCE_TEST_ENABLED");
        
        performanceTestEnabled = databricksHost != null && 
                                databricksHttpPath != null && 
                                databricksToken != null &&
                                "true".equalsIgnoreCase(performanceEnabled);
        
        if (performanceTestEnabled) {
            logger.info("Performance test environment detected");
            logger.info("Databricks Host: {}", databricksHost);
            logger.info("HTTP Path: {}", databricksHttpPath);
            logger.info("Performance tests enabled");
        }
        else {
            logger.warn("Performance test environment not configured. Set DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN, and PERFORMANCE_TEST_ENABLED=true to enable performance tests.");
        }
    }
    
    @Test
    public void testConnectionPerformance()
    {
        Assume.assumeTrue("Performance test environment not configured", performanceTestEnabled);
        
        logger.info("Testing connection performance...");
        
        List<Long> connectionTimes = new ArrayList<>();
        
        for (int i = 0; i < QUERY_ITERATIONS; i++) {
            long startTime = System.currentTimeMillis();
            
            try {
                Connection connection = createConnection();
                assertNotNull("Connection should not be null", connection);
                assertTrue("Connection should be valid", connection.isValid(5));
                
                long endTime = System.currentTimeMillis();
                long connectionTime = endTime - startTime;
                connectionTimes.add(connectionTime);
                
                logger.info("Connection {} took {} ms", i + 1, connectionTime);
                
                connection.close();
                
                // Assert connection time is within acceptable limits
                assertTrue("Connection time should be under " + CONNECTION_TIMEOUT_MS + "ms, but was " + connectionTime + "ms", 
                          connectionTime < CONNECTION_TIMEOUT_MS);
            }
            catch (Exception e) {
                logger.error("Connection test failed on iteration {}", i + 1, e);
                fail("Connection performance test failed: " + e.getMessage());
            }
        }
        
        // Calculate and log statistics
        double avgTime = connectionTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long minTime = connectionTimes.stream().mapToLong(Long::longValue).min().orElse(0);
        long maxTime = connectionTimes.stream().mapToLong(Long::longValue).max().orElse(0);
        
        logger.info("Connection Performance Summary:");
        logger.info("  Average: {:.2f} ms", avgTime);
        logger.info("  Min: {} ms", minTime);
        logger.info("  Max: {} ms", maxTime);
        logger.info("  Iterations: {}", QUERY_ITERATIONS);
        
        // Assert average connection time is reasonable
        assertTrue("Average connection time should be under 5 seconds", avgTime < 5000);
    }
    
    @Test
    public void testSimpleQueryPerformance()
    {
        Assume.assumeTrue("Performance test environment not configured", performanceTestEnabled);
        
        logger.info("Testing simple query performance...");
        
        List<Long> queryTimes = new ArrayList<>();
        
        try {
            Connection connection = createConnection();
            
            String[] simpleQueries = {
                "SELECT 1 as test_column",
                "SELECT 'hello' as greeting",
                "SELECT current_timestamp() as now",
                "SELECT current_date() as today",
                "SELECT true as boolean_test"
            };
            
            for (String query : simpleQueries) {
                for (int i = 0; i < QUERY_ITERATIONS; i++) {
                    long startTime = System.currentTimeMillis();
                    
                    try (Statement statement = connection.createStatement()) {
                        ResultSet resultSet = statement.executeQuery(query);
                        assertTrue("Query should return results", resultSet.next());
                        
                        // Consume the result to ensure full execution
                        Object result = resultSet.getObject(1);
                        assertNotNull("Result should not be null", result);
                        
                        long endTime = System.currentTimeMillis();
                        long queryTime = endTime - startTime;
                        queryTimes.add(queryTime);
                        
                        logger.debug("Query '{}' iteration {} took {} ms", query, i + 1, queryTime);
                        
                        // Assert query time is within acceptable limits
                        assertTrue("Simple query time should be under " + SIMPLE_QUERY_TIMEOUT_MS + "ms, but was " + queryTime + "ms", 
                                  queryTime < SIMPLE_QUERY_TIMEOUT_MS);
                    }
                }
            }
            
            connection.close();
        }
        catch (Exception e) {
            logger.error("Simple query performance test failed", e);
            fail("Simple query performance test failed: " + e.getMessage());
        }
        
        // Calculate and log statistics
        double avgTime = queryTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long minTime = queryTimes.stream().mapToLong(Long::longValue).min().orElse(0);
        long maxTime = queryTimes.stream().mapToLong(Long::longValue).max().orElse(0);
        
        logger.info("Simple Query Performance Summary:");
        logger.info("  Average: {:.2f} ms", avgTime);
        logger.info("  Min: {} ms", minTime);
        logger.info("  Max: {} ms", maxTime);
        logger.info("  Total queries: {}", queryTimes.size());
        
        // Assert average query time is reasonable
        assertTrue("Average simple query time should be under 2 seconds", avgTime < 2000);
    }
    
    @Test
    public void testMetadataQueryPerformance()
    {
        Assume.assumeTrue("Performance test environment not configured", performanceTestEnabled);
        
        logger.info("Testing metadata query performance...");
        
        List<Long> queryTimes = new ArrayList<>();
        
        try {
            Connection connection = createConnection();
            
            String[] metadataQueries = {
                "SHOW DATABASES",
                "SHOW TABLES IN default",
                "SELECT current_database() as current_db",
                "SELECT current_user() as current_user",
                "SELECT version() as spark_version"
            };
            
            for (String query : metadataQueries) {
                long startTime = System.currentTimeMillis();
                
                try (Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery(query);
                    
                    // Count results to ensure full execution
                    int rowCount = 0;
                    while (resultSet.next()) {
                        rowCount++;
                    }
                    
                    long endTime = System.currentTimeMillis();
                    long queryTime = endTime - startTime;
                    queryTimes.add(queryTime);
                    
                    logger.info("Metadata query '{}' took {} ms, returned {} rows", query, queryTime, rowCount);
                    
                    // Assert query time is within acceptable limits
                    assertTrue("Metadata query time should be under " + COMPLEX_QUERY_TIMEOUT_MS + "ms, but was " + queryTime + "ms", 
                              queryTime < COMPLEX_QUERY_TIMEOUT_MS);
                }
            }
            
            connection.close();
        }
        catch (Exception e) {
            logger.error("Metadata query performance test failed", e);
            fail("Metadata query performance test failed: " + e.getMessage());
        }
        
        // Calculate and log statistics
        double avgTime = queryTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long minTime = queryTimes.stream().mapToLong(Long::longValue).min().orElse(0);
        long maxTime = queryTimes.stream().mapToLong(Long::longValue).max().orElse(0);
        
        logger.info("Metadata Query Performance Summary:");
        logger.info("  Average: {:.2f} ms", avgTime);
        logger.info("  Min: {} ms", minTime);
        logger.info("  Max: {} ms", maxTime);
        logger.info("  Total queries: {}", queryTimes.size());
    }
    
    @Test
    public void testConcurrentConnectionPerformance()
    {
        Assume.assumeTrue("Performance test environment not configured", performanceTestEnabled);
        
        logger.info("Testing concurrent connection performance with {} connections...", CONCURRENT_CONNECTIONS);
        
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_CONNECTIONS);
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        
        long testStartTime = System.currentTimeMillis();
        
        // Create concurrent connection tasks
        for (int i = 0; i < CONCURRENT_CONNECTIONS; i++) {
            final int connectionId = i;
            CompletableFuture<Long> future = CompletableFuture.supplyAsync(() -> {
                try {
                    long startTime = System.currentTimeMillis();
                    
                    Connection connection = createConnection();
                    assertNotNull("Connection should not be null", connection);
                    assertTrue("Connection should be valid", connection.isValid(5));
                    
                    // Execute a simple query to test full functionality
                    try (Statement statement = connection.createStatement()) {
                        ResultSet resultSet = statement.executeQuery("SELECT " + connectionId + " as connection_id");
                        assertTrue("Query should return results", resultSet.next());
                        assertEquals("Connection ID should match", connectionId, resultSet.getInt("connection_id"));
                    }
                    
                    connection.close();
                    
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime;
                    
                    logger.info("Concurrent connection {} completed in {} ms", connectionId, duration);
                    return duration;
                }
                catch (Exception e) {
                    logger.error("Concurrent connection {} failed", connectionId, e);
                    throw new RuntimeException("Concurrent connection failed", e);
                }
            }, executor);
            
            futures.add(future);
        }
        
        // Wait for all connections to complete
        List<Long> connectionTimes = new ArrayList<>();
        try {
            for (CompletableFuture<Long> future : futures) {
                Long duration = future.get(CONNECTION_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS);
                connectionTimes.add(duration);
            }
        }
        catch (Exception e) {
            logger.error("Concurrent connection test failed", e);
            fail("Concurrent connection test failed: " + e.getMessage());
        }
        finally {
            executor.shutdown();
        }
        
        long testEndTime = System.currentTimeMillis();
        long totalTestTime = testEndTime - testStartTime;
        
        // Calculate and log statistics
        double avgTime = connectionTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long minTime = connectionTimes.stream().mapToLong(Long::longValue).min().orElse(0);
        long maxTime = connectionTimes.stream().mapToLong(Long::longValue).max().orElse(0);
        
        logger.info("Concurrent Connection Performance Summary:");
        logger.info("  Concurrent connections: {}", CONCURRENT_CONNECTIONS);
        logger.info("  Total test time: {} ms", totalTestTime);
        logger.info("  Average connection time: {:.2f} ms", avgTime);
        logger.info("  Min connection time: {} ms", minTime);
        logger.info("  Max connection time: {} ms", maxTime);
        
        // Assert all connections completed successfully
        assertEquals("All connections should complete", CONCURRENT_CONNECTIONS, connectionTimes.size());
        
        // Assert reasonable performance under concurrent load
        assertTrue("Average concurrent connection time should be reasonable", avgTime < CONNECTION_TIMEOUT_MS);
        assertTrue("Total test time should be reasonable", totalTestTime < CONNECTION_TIMEOUT_MS * 2);
    }
    
    @Test
    public void testDataThroughputPerformance()
    {
        Assume.assumeTrue("Performance test environment not configured", performanceTestEnabled);
        
        logger.info("Testing data throughput performance...");
        
        try {
            Connection connection = createConnection();
            
            // Test different result set sizes
            int[] rowCounts = {10, 100, 1000};
            
            for (int rowCount : rowCounts) {
                long startTime = System.currentTimeMillis();
                
                // Generate a query that returns the specified number of rows using a simpler approach
                String query = String.format(
                    "SELECT id, concat('test_data_', cast(id as string)) as data, rand() as random_value " +
                    "FROM range(%d) as t(id)", rowCount);
                
                try (Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery(query);
                    
                    int actualRowCount = 0;
                    long dataSize = 0;
                    
                    while (resultSet.next()) {
                        actualRowCount++;
                        // Simulate data processing by accessing all columns
                        int id = resultSet.getInt("id");
                        String data = resultSet.getString("data");
                        double randomValue = resultSet.getDouble("random_value");
                        
                        // Estimate data size (rough calculation)
                        dataSize += 4 + data.length() + 8; // int + string + double
                    }
                    
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime;
                    
                    double throughputRowsPerSec = (actualRowCount * 1000.0) / duration;
                    double throughputMBPerSec = (dataSize * 1000.0) / (duration * 1024 * 1024);
                    
                    logger.info("Data Throughput Test - {} rows:", rowCount);
                    logger.info("  Actual rows: {}", actualRowCount);
                    logger.info("  Duration: {} ms", duration);
                    logger.info("  Estimated data size: {} bytes", dataSize);
                    logger.info("  Throughput: {:.2f} rows/sec", throughputRowsPerSec);
                    logger.info("  Throughput: {:.2f} MB/sec", throughputMBPerSec);
                    
                    assertEquals("Should return expected number of rows", rowCount, actualRowCount);
                    assertTrue("Query should complete in reasonable time", duration < COMPLEX_QUERY_TIMEOUT_MS);
                    assertTrue("Should achieve reasonable throughput", throughputRowsPerSec > 1); // At least 1 row/sec (more realistic for Databricks)
                }
            }
            
            connection.close();
        }
        catch (Exception e) {
            logger.error("Data throughput performance test failed", e);
            fail("Data throughput performance test failed: " + e.getMessage());
        }
    }
    
    @Test
    public void testConnectionPoolingPerformance()
    {
        Assume.assumeTrue("Performance test environment not configured", performanceTestEnabled);
        
        logger.info("Testing connection reuse performance...");
        
        try {
            // Test connection reuse vs new connections
            Connection connection = createConnection();
            
            List<Long> reuseQueryTimes = new ArrayList<>();
            List<Long> newConnectionTimes = new ArrayList<>();
            
            // Test with connection reuse
            for (int i = 0; i < 5; i++) {
                long startTime = System.currentTimeMillis();
                
                try (Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT " + i + " as iteration");
                    assertTrue("Query should return results", resultSet.next());
                    assertEquals("Iteration should match", i, resultSet.getInt("iteration"));
                }
                
                long endTime = System.currentTimeMillis();
                reuseQueryTimes.add(endTime - startTime);
            }
            
            connection.close();
            
            // Test with new connections each time
            for (int i = 0; i < 5; i++) {
                long startTime = System.currentTimeMillis();
                
                Connection newConnection = createConnection();
                try (Statement statement = newConnection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT " + i + " as iteration");
                    assertTrue("Query should return results", resultSet.next());
                    assertEquals("Iteration should match", i, resultSet.getInt("iteration"));
                }
                newConnection.close();
                
                long endTime = System.currentTimeMillis();
                newConnectionTimes.add(endTime - startTime);
            }
            
            // Calculate averages
            double avgReuseTime = reuseQueryTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
            double avgNewConnectionTime = newConnectionTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
            
            logger.info("Connection Reuse Performance Summary:");
            logger.info("  Average reuse time: {:.2f} ms", avgReuseTime);
            logger.info("  Average new connection time: {:.2f} ms", avgNewConnectionTime);
            logger.info("  Performance improvement: {:.2f}x", avgNewConnectionTime / avgReuseTime);
            
            // Connection reuse should be faster
            assertTrue("Connection reuse should be faster than creating new connections", 
                      avgReuseTime < avgNewConnectionTime);
        }
        catch (Exception e) {
            logger.error("Connection pooling performance test failed", e);
            fail("Connection pooling performance test failed: " + e.getMessage());
        }
    }
    
    /**
     * Creates a Databricks JDBC connection using the configured environment variables.
     */
    private Connection createConnection() throws Exception
    {
        // Load the Databricks JDBC driver
        Class.forName("com.databricks.client.jdbc.Driver");
        
        // Construct JDBC URL
        String jdbcUrl = String.format("jdbc:databricks://%s:443", databricksHost);
        
        // Set up connection properties
        Properties props = new Properties();
        props.setProperty("PWD", databricksToken);
        props.setProperty("AuthMech", "3"); // Token-based authentication
        props.setProperty("UID", "token");
        props.setProperty("SSL", "1");
        props.setProperty("httpPath", databricksHttpPath);
        
        return DriverManager.getConnection(jdbcUrl, props);
    }
}