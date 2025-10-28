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
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Simple connectivity test for Databricks that tests direct JDBC connectivity.
 * 
 * This test validates basic connectivity to a Databricks cluster using environment variables.
 * Set the following environment variables to run this test:
 * - DATABRICKS_HOST: The Databricks cluster hostname
 * - DATABRICKS_HTTP_PATH: The HTTP path for the cluster
 * - DATABRICKS_TOKEN: Personal access token for authentication
 * 
 * If these are not set, the tests will be skipped.
 */
public class DatabricksConnectivityTestSimple
{
    private static final Logger logger = LoggerFactory.getLogger(DatabricksConnectivityTestSimple.class);
    
    private String databricksHost;
    private String databricksHttpPath;
    private String databricksToken;
    private boolean integrationTestEnabled;
    
    @Before
    public void setUp()
    {
        // Check if integration test environment variables are set
        databricksHost = System.getenv("DATABRICKS_HOST");
        databricksHttpPath = System.getenv("DATABRICKS_HTTP_PATH");
        databricksToken = System.getenv("DATABRICKS_TOKEN");
        
        integrationTestEnabled = databricksHost != null && 
                                databricksHttpPath != null && 
                                databricksToken != null;
        
        if (integrationTestEnabled) {
            logger.info("Integration test environment detected");
            logger.info("Databricks Host: {}", databricksHost);
            logger.info("HTTP Path: {}", databricksHttpPath);
        }
        else {
            logger.warn("Integration test environment not configured. Set DATABRICKS_HOST, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN to enable integration tests.");
        }
    }
    
    @Test
    public void testDatabricksJdbcConnection()
    {
        Assume.assumeTrue("Integration test environment not configured", integrationTestEnabled);
        
        logger.info("Testing direct Databricks JDBC connection...");
        
        try {
            Connection connection = createConnection();
            assertNotNull("Connection should not be null", connection);
            assertTrue("Connection should be valid", connection.isValid(5));
            
            logger.info("✅ Successfully connected to Databricks!");
            
            // Test basic query
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT 1 as test_column");
                assertTrue("Query should return results", resultSet.next());
                assertEquals("Query result should be 1", 1, resultSet.getInt("test_column"));
                
                logger.info("Successfully executed test query");
            }
            
            connection.close();
            logger.info("Connection test completed successfully");
        }
        catch (Exception e) {
            logger.error("Failed to establish Databricks connection", e);
            fail("Connection test failed: " + e.getMessage());
        }
    }
    
    @Test
    public void testListDatabases()
    {
        Assume.assumeTrue("Integration test environment not configured", integrationTestEnabled);
        
        logger.info("Testing database listing...");
        
        try {
            Connection connection = createConnection();
            assertNotNull("Connection should not be null", connection);
            
            // Test listing databases/schemas
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SHOW DATABASES");
                
                boolean foundDefault = false;
                int databaseCount = 0;
                while (resultSet.next()) {
                    String databaseName = resultSet.getString(1);
                    logger.info("Found database: {}", databaseName);
                    databaseCount++;
                    
                    if ("default".equals(databaseName)) {
                        foundDefault = true;
                    }
                }
                
                assertTrue("Should find at least one database", databaseCount > 0);
                logger.info("Successfully listed {} databases", databaseCount);
            }
            
            connection.close();
        }
        catch (Exception e) {
            logger.error("Failed to list databases", e);
            fail("Database listing test failed: " + e.getMessage());
        }
    }
    
    @Test
    public void testListTables()
    {
        Assume.assumeTrue("Integration test environment not configured", integrationTestEnabled);
        
        logger.info("Testing table listing...");
        
        try {
            Connection connection = createConnection();
            assertNotNull("Connection should not be null", connection);
            
            // Test listing tables in default schema
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SHOW TABLES IN default");
                
                int tableCount = 0;
                while (resultSet.next()) {
                    String tableName = resultSet.getString("tableName");
                    logger.info("Found table: {}", tableName);
                    tableCount++;
                }
                
                logger.info("Found {} tables in default schema", tableCount);
            }
            
            connection.close();
        }
        catch (Exception e) {
            logger.error("Failed to list tables", e);
            fail("Table listing test failed: " + e.getMessage());
        }
    }
    
    @Test
    public void testQueryExecution()
    {
        Assume.assumeTrue("Integration test environment not configured", integrationTestEnabled);
        
        logger.info("Testing query execution...");
        
        try {
            Connection connection = createConnection();
            assertNotNull("Connection should not be null", connection);
            
            // Test various query types
            String[] testQueries = {
                "SELECT 1 as test_int",
                "SELECT 'test' as test_string",
                "SELECT true as test_boolean",
                "SELECT current_timestamp() as test_timestamp",
                "SELECT current_date() as test_date"
            };
            
            for (String query : testQueries) {
                logger.info("Executing query: {}", query);
                
                try (Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery(query);
                    assertTrue("Query should return results: " + query, resultSet.next());
                    
                    // Log the result
                    Object result = resultSet.getObject(1);
                    logger.info("Query result: {}", result);
                }
            }
            
            logger.info("Successfully executed all test queries");
            connection.close();
        }
        catch (Exception e) {
            logger.error("Failed to execute queries", e);
            fail("Query execution test failed: " + e.getMessage());
        }
    }
    
    /**
     * Creates a Databricks JDBC connection using the configured environment variables.
     */
    private Connection createConnection() throws Exception
    {
        // Load the Databricks JDBC driver
        Class.forName("com.databricks.client.jdbc.Driver");
        
        // Construct JDBC URL - use the host only, httpPath goes in properties
        String jdbcUrl = String.format("jdbc:databricks://%s:443", databricksHost);
        
        // Set up connection properties with minimal configuration
        Properties props = new Properties();
        props.setProperty("PWD", databricksToken);
        props.setProperty("AuthMech", "3"); // Token-based authentication
        props.setProperty("UID", "token");
        props.setProperty("SSL", "1");
        props.setProperty("httpPath", databricksHttpPath); // Add httpPath as property
        
        return DriverManager.getConnection(jdbcUrl, props);
    }
    
    /**
     * Masks sensitive information in JDBC URL for logging.
     */
    private String maskSensitiveUrl(String jdbcUrl)
    {
        if (jdbcUrl == null) {
            return null;
        }
        
        // Replace any token or password parameters with masked values
        return jdbcUrl.replaceAll("(?i)(token|pwd|password)=([^;&]+)", "$1=***");
    }
}