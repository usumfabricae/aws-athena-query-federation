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

import com.amazonaws.athena.connectors.jdbc.JdbcEnvironmentProperties;

import java.util.Map;

/**
 * Handles environment variable parsing and configuration for Databricks connections.
 */
public class DatabricksEnvironmentProperties extends JdbcEnvironmentProperties
{
    private static final String DATABRICKS_DRIVER_CLASS = "com.databricks.client.jdbc.Driver";
    private static final int DEFAULT_CONNECTION_TIMEOUT = 30000; // 30 seconds
    private static final int DEFAULT_SOCKET_TIMEOUT = 60000; // 60 seconds
    
    @Override
    protected String getConnectionStringPrefix(Map<String, String> connectionProperties)
    {
        return "jdbc:databricks://";
    }
    
    /**
     * Gets the JDBC driver class name for Databricks.
     * 
     * @return The Databricks JDBC driver class name
     */
    public String getJdbcDriverClass()
    {
        return DATABRICKS_DRIVER_CLASS;
    }
    
    /**
     * Constructs the JDBC connection string for Databricks.
     * 
     * @param catalogName The catalog name to connect to
     * @return The complete JDBC connection string
     */
    public String getJdbcConnectionString(String catalogName)
    {
        // First try the individual environment variables
        String host = System.getenv("DATABRICKS_HOST");
        String httpPath = System.getenv("DATABRICKS_HTTP_PATH");
        
        if (host != null && !host.isEmpty() && httpPath != null && !httpPath.isEmpty()) {
            return String.format("jdbc:databricks://%s:443%s", host, httpPath);
        }
        
        // Fall back to the default connection string from Lambda environment
        String defaultConnectionString = System.getenv("default");
        if (defaultConnectionString != null && !defaultConnectionString.isEmpty()) {
            // Handle both formats: "databricks://host:port/path" and "host:port/path"
            if (defaultConnectionString.startsWith("databricks://")) {
                return "jdbc:" + defaultConnectionString;
            } else if (defaultConnectionString.contains(".databricks.com")) {
                return "jdbc:databricks://" + defaultConnectionString;
            } else {
                throw new IllegalArgumentException("Invalid connection string format: " + defaultConnectionString);
            }
        }
        
        throw new IllegalArgumentException("Either DATABRICKS_HOST/DATABRICKS_HTTP_PATH or 'default' environment variable is required");
    }
    
    /**
     * Gets the access token for Databricks authentication.
     * 
     * @return The Databricks access token
     */
    public String getAccessToken()
    {
        // First try the dedicated token environment variable
        String token = System.getenv("DATABRICKS_TOKEN");
        if (token != null && !token.isEmpty()) {
            return token;
        }
        
        // Fall back to checking if it's in the database connection config
        // This will be handled by the connection factory through the databaseConnectionConfig map
        // For now, return a placeholder that will be overridden by the PWD property
        return "token_from_config";
    }
    
    /**
     * Determines if SSL should be used for the connection.
     * 
     * @return true if SSL should be used, false otherwise
     */
    public boolean useSSL()
    {
        return true; // Always use SSL for Databricks
    }
    
    /**
     * Gets the connection timeout in milliseconds.
     * 
     * @return The connection timeout
     */
    public int getConnectionTimeout()
    {
        String timeout = System.getenv("DATABRICKS_CONNECTION_TIMEOUT");
        if (timeout != null && !timeout.isEmpty()) {
            try {
                return Integer.parseInt(timeout);
            } catch (NumberFormatException e) {
                // Fall back to default
            }
        }
        return DEFAULT_CONNECTION_TIMEOUT;
    }
    
    /**
     * Gets the socket timeout in milliseconds.
     * 
     * @return The socket timeout
     */
    public int getSocketTimeout()
    {
        String timeout = System.getenv("DATABRICKS_SOCKET_TIMEOUT");
        if (timeout != null && !timeout.isEmpty()) {
            try {
                return Integer.parseInt(timeout);
            } catch (NumberFormatException e) {
                // Fall back to default
            }
        }
        return DEFAULT_SOCKET_TIMEOUT;
    }
    
    /**
     * Gets the default catalog to use.
     * 
     * @return The default catalog name
     */
    public String getDefaultCatalog()
    {
        return System.getenv("TEST_CATALOG");
    }
    
    /**
     * Gets the default schema to use.
     * 
     * @return The default schema name
     */
    public String getDefaultSchema()
    {
        return System.getenv("TEST_SCHEMA");
    }
}