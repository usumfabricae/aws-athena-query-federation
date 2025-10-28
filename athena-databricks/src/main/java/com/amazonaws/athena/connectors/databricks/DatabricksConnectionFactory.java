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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Creates connections to Databricks clusters using the Databricks JDBC driver.
 * Includes comprehensive error handling and retry logic for connection failures.
 */
public class DatabricksConnectionFactory extends GenericJdbcConnectionFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksConnectionFactory.class);
    
    private final DatabricksEnvironmentProperties environmentProperties;

    public DatabricksConnectionFactory(final DatabaseConnectionConfig databaseConnectionConfig, 
                                     final DatabricksEnvironmentProperties environmentProperties)
    {
        super(databaseConnectionConfig, java.util.Collections.emptyMap(), CONNECTION_INFO);
        this.environmentProperties = Validate.notNull(environmentProperties, "environmentProperties must not be null");
    }
    
    private static final com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo CONNECTION_INFO = 
        new com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo("com.databricks.client.jdbc.Driver", 443);

    @Override
    public Connection getConnection(final CredentialsProvider credentialsProvider) throws Exception
    {
        String connectionId = "conn-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
        String catalogName = environmentProperties.getDefaultCatalog();
        DatabricksMetrics.logConnectionAttempt(catalogName, connectionId);
        
        long startTime = System.currentTimeMillis();
        
        try {
            Connection connection = DatabricksErrorHandler.executeWithRetry(() -> {
                return createConnectionInternal(catalogName);
            }, "Databricks connection creation");
            
            long duration = System.currentTimeMillis() - startTime;
            DatabricksMetrics.logConnectionSuccess(catalogName, connectionId, duration);
            
            return connection;
        }
        catch (SQLException e) {
            long duration = System.currentTimeMillis() - startTime;
            DatabricksMetrics.logConnectionFailure(catalogName, connectionId, duration, e.getMessage());
            throw e;
        }
        catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            DatabricksMetrics.logConnectionFailure(catalogName, connectionId, duration, e.getMessage());
            throw DatabricksErrorHandler.mapException("connection creation", e);
        }
    }
    
    /**
     * Internal method to create a Databricks connection.
     * 
     * @param catalogName The catalog name to connect to
     * @return JDBC connection to Databricks
     * @throws SQLException if connection fails
     */
    private Connection createConnectionInternal(final String catalogName) throws SQLException
    {
        long startTime = System.currentTimeMillis();
        
        try {
            // Load the Databricks JDBC driver
            LOGGER.debug("Loading Databricks JDBC driver: {}", environmentProperties.getJdbcDriverClass());
            Class.forName(environmentProperties.getJdbcDriverClass());
        }
        catch (ClassNotFoundException e) {
            LOGGER.error("Failed to load Databricks JDBC driver: {}", environmentProperties.getJdbcDriverClass(), e);
            throw DatabricksErrorHandler.mapException("JDBC driver loading", e);
        }

        String jdbcUrl;
        try {
            jdbcUrl = environmentProperties.getJdbcConnectionString(catalogName);
            LOGGER.debug("Databricks JDBC URL constructed: {}", maskSensitiveUrl(jdbcUrl));
        }
        catch (Exception e) {
            LOGGER.error("Failed to construct Databricks JDBC URL for catalog: {}", catalogName, e);
            throw DatabricksErrorHandler.mapException("JDBC URL construction", e);
        }

        Properties connectionProperties = new Properties();

        try {
            // Get credentials from Secrets Manager using AWS SDK
            String secretName = "AthenaDatabricksFederation/default";
            LOGGER.debug("Retrieving credentials from Secrets Manager: {}", secretName);
            
            software.amazon.awssdk.services.secretsmanager.SecretsManagerClient secretsClient = 
                software.amazon.awssdk.services.secretsmanager.SecretsManagerClient.create();
            
            software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest getSecretValueRequest = 
                software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest.builder()
                    .secretId(secretName)
                    .build();
            
            software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse getSecretValueResponse = 
                secretsClient.getSecretValue(getSecretValueRequest);
            
            String credentials = getSecretValueResponse.secretString();
            LOGGER.debug("Successfully retrieved credentials from Secrets Manager");
            
            // Parse the JSON credentials
            com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode credentialsJson = objectMapper.readTree(credentials);
            
            String accessToken = credentialsJson.get("password").asText();
            String serverHostname = credentialsJson.get("server_hostname").asText();
            String httpPath = credentialsJson.get("http_path").asText();
            
            LOGGER.debug("Parsed credentials - Server: {}, HTTP Path: {}", serverHostname, httpPath);
            
            // Override the JDBC URL with credentials from Secrets Manager
            // For Databricks, the HTTP path should be a connection parameter, not part of the URL path
            // Don't specify a default database, let Databricks handle the default catalog
            jdbcUrl = String.format("jdbc:databricks://%s:443;httpPath=%s", serverHostname, httpPath);
            LOGGER.debug("Updated JDBC URL from Secrets Manager: {}", maskSensitiveUrl(jdbcUrl));
            
            // Set authentication properties
            connectionProperties.setProperty("PWD", accessToken);
            connectionProperties.setProperty("AuthMech", "3"); // Token-based authentication
            connectionProperties.setProperty("UID", "token");
            LOGGER.debug("Authentication properties configured for token-based auth");

            // Set SSL properties
            if (environmentProperties.useSSL()) {
                connectionProperties.setProperty("SSL", "1");
                LOGGER.debug("SSL enabled for Databricks connection");
            }

            // Note: Databricks JDBC driver doesn't support ConnTimeout and SocketTimeout properties
            // These would cause "Configuration ConnTimeout is not available" errors
            LOGGER.debug("Skipping timeout properties as they are not supported by Databricks JDBC driver");

            // Set catalog and schema if specified
            String defaultCatalog = environmentProperties.getDefaultCatalog();
            if (defaultCatalog != null && !defaultCatalog.isEmpty()) {
                connectionProperties.setProperty("ConnCatalog", defaultCatalog);
                LOGGER.debug("Default catalog set: {}", defaultCatalog);
            }

            String defaultSchema = environmentProperties.getDefaultSchema();
            if (defaultSchema != null && !defaultSchema.isEmpty()) {
                connectionProperties.setProperty("ConnSchema", defaultSchema);
                LOGGER.debug("Default schema set: {}", defaultSchema);
            }
            
            // Close the secrets client
            secretsClient.close();
        }
        catch (Exception e) {
            LOGGER.error("Failed to configure connection properties", e);
            throw DatabricksErrorHandler.mapException("connection properties configuration", e);
        }

        try {
            LOGGER.debug("Establishing connection to Databricks...");
            Connection connection = DriverManager.getConnection(jdbcUrl, connectionProperties);
            
            long connectionTime = System.currentTimeMillis() - startTime;
            LOGGER.info("Successfully established Databricks connection for catalog {} in {}ms", 
                       catalogName, connectionTime);
            
            // Validate the connection
            if (!connection.isValid(5)) {
                LOGGER.error("Connection validation failed for catalog: {}", catalogName);
                throw new SQLException("Connection validation failed");
            }
            
            LOGGER.debug("Connection validation successful");
            return connection;
        }
        catch (SQLException e) {
            long failureTime = System.currentTimeMillis() - startTime;
            LOGGER.error("Failed to establish Databricks connection for catalog {} after {}ms: {}", 
                        catalogName, failureTime, e.getMessage());
            throw DatabricksErrorHandler.mapSqlException("connection establishment", e);
        }
    }
    
    /**
     * Masks sensitive information in JDBC URL for logging.
     * 
     * @param jdbcUrl The original JDBC URL
     * @return Masked URL safe for logging
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