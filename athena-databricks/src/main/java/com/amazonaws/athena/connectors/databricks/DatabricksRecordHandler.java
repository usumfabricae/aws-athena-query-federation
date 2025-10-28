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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Handles record operations for Databricks connector.
 * Extends JdbcRecordHandler to provide Databricks-specific data retrieval functionality.
 */
public class DatabricksRecordHandler extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksRecordHandler.class);

    @VisibleForTesting
    protected static final String DATABRICKS_QUOTE_CHARACTER = "`";

    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link DatabricksCompositeHandler} instead.
     */
    public DatabricksRecordHandler(Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(DatabricksConstants.DATABRICKS_NAME, configOptions), configOptions);
    }

    public DatabricksRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, 
             new DatabricksConnectionFactoryWrapper(databaseConnectionConfig, 
                                                  new DatabricksEnvironmentProperties()), 
             configOptions);
    }

    public DatabricksRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, 
                                 JdbcConnectionFactory jdbcConnectionFactory, 
                                 Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, 
             S3Client.create(), 
             SecretsManagerClient.create(), 
             AthenaClient.create(),
             jdbcConnectionFactory, 
             new DatabricksQueryStringBuilder(DATABRICKS_QUOTE_CHARACTER, 
                                            new DatabricksFederationExpressionParser()), 
             configOptions);
    }

    @VisibleForTesting
    DatabricksRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, 
                          S3Client amazonS3, 
                          SecretsManagerClient secretsManager,
                          AthenaClient athena, 
                          JdbcConnectionFactory jdbcConnectionFactory, 
                          JdbcSplitQueryBuilder jdbcSplitQueryBuilder, 
                          Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, 
                                         String catalogName, 
                                         TableName tableName, 
                                         Schema schema, 
                                         Constraints constraints, 
                                         Split split)
            throws SQLException
    {
        String queryId = "sql-build-" + System.currentTimeMillis();
        DatabricksMetrics.logQueryStart(queryId, catalogName, tableName.getSchemaName(), 
                                      tableName.getTableName(), "buildSplitSql");
        
        // LOG ALL INPUT PARAMETERS FOR DEBUGGING
        LOGGER.info("=== DATABRICKS QUERY BUILDING DEBUG ===");
        LOGGER.info("Query ID: {}", queryId);
        LOGGER.info("Original catalogName from Athena: '{}'", catalogName);
        LOGGER.info("TableName.schemaName: '{}'", tableName.getSchemaName());
        LOGGER.info("TableName.tableName: '{}'", tableName.getTableName());
        LOGGER.info("Split properties: {}", split.getProperties());
        LOGGER.info("Constraints summary: {}", constraints.getSummary());
        LOGGER.info("Connection URL: {}", jdbcConnection.getMetaData().getURL());
        
        // CHECK FOR CATALOG NAME MAPPING IN ENVIRONMENT VARIABLES
        String mappedCatalogName = catalogName;
        String envVarName = catalogName.replace("-", "_");
        String envCatalogMapping = System.getenv(envVarName);
        if (envCatalogMapping != null && !envCatalogMapping.isEmpty()) {
            mappedCatalogName = envCatalogMapping;
            LOGGER.info("Found catalog mapping: '{}' -> '{}' via env var '{}'", 
                       catalogName, mappedCatalogName, envVarName);
        } else {
            LOGGER.info("No catalog mapping found for '{}' (checked env var '{}')", catalogName, envVarName);
        }
        
        long startTime = System.currentTimeMillis();
        
        try {
            PreparedStatement preparedStatement;

            if (constraints.isQueryPassThrough()) {
                LOGGER.info("Using query passthrough for table {}.{}", 
                           tableName.getSchemaName(), tableName.getTableName());
                preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
            }
            else {
                LOGGER.info("Building split SQL for table {}.{} with {} constraints using mapped catalog '{}'", 
                           tableName.getSchemaName(), tableName.getTableName(), 
                           constraints.getSummary().size(), mappedCatalogName);
                preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, 
                                                                 mappedCatalogName, 
                                                                 tableName.getSchemaName(), 
                                                                 tableName.getTableName(), 
                                                                 schema, 
                                                                 constraints, 
                                                                 split);
            }
            
            // LOG THE ACTUAL SQL QUERY BEING EXECUTED
            try {
                String actualSql = preparedStatement.toString();
                LOGGER.info("=== FINAL SQL QUERY TO DATABRICKS ===");
                LOGGER.info("PreparedStatement toString(): {}", actualSql);
                
                // Try to extract just the SQL part if possible
                if (actualSql.contains(":")) {
                    String sqlPart = actualSql.substring(actualSql.indexOf(":") + 1).trim();
                    LOGGER.info("Extracted SQL: {}", sqlPart);
                }
            } catch (Exception e) {
                LOGGER.warn("Could not extract SQL from PreparedStatement: {}", e.getMessage());
            }

            // Configure fetch size for optimal performance with Databricks
            // Use streaming mode to handle large result sets efficiently
            try {
                preparedStatement.setFetchSize(1000);
                LOGGER.debug("Set fetch size to 1000 for optimal Databricks performance");
            }
            catch (SQLException e) {
                LOGGER.warn("Failed to set fetch size, continuing with default: {}", e.getMessage());
            }
            
            long duration = System.currentTimeMillis() - startTime;
            DatabricksMetrics.logQuerySuccess(queryId, catalogName, tableName.getSchemaName(), 
                                            tableName.getTableName(), "buildSplitSql", 0, 0);

            return preparedStatement;
        }
        catch (SQLException e) {
            long duration = System.currentTimeMillis() - startTime;
            DatabricksMetrics.logQueryFailure(queryId, catalogName, tableName.getSchemaName(), 
                                            tableName.getTableName(), "buildSplitSql", e.getMessage());
            throw DatabricksErrorHandler.mapSqlException("SQL building", e);
        }
        catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            DatabricksMetrics.logQueryFailure(queryId, catalogName, tableName.getSchemaName(), 
                                            tableName.getTableName(), "buildSplitSql", e.getMessage());
            throw DatabricksErrorHandler.mapException("SQL building", e);
        }
    }

    /**
     * Handles Databricks-specific data type conversions and optimizations.
     * Overrides the base extractor to provide better handling of Databricks data types.
     */
    @Override
    protected Extractor makeExtractor(Field field, ResultSet resultSet, Map<String, String> partitionValues)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        final String fieldName = field.getName();

        // Handle partition values first
        if (partitionValues.containsKey(fieldName)) {
            return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
            {
                dst.isSet = 1;
                dst.value = partitionValues.get(fieldName);
            };
        }

        // Handle Databricks-specific data type optimizations
        switch (fieldType) {
            case DECIMAL:
                // Databricks DECIMAL handling with proper precision and scale
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) ->
                {
                    long conversionStart = System.nanoTime();
                    try {
                        java.math.BigDecimal value = resultSet.getBigDecimal(fieldName);
                        if (resultSet.wasNull() || value == null) {
                            dst.isSet = 0;
                            LOGGER.debug("DECIMAL field {} is null", fieldName);
                        } else {
                            dst.isSet = 1;
                            dst.value = value;
                            LOGGER.trace("Extracted DECIMAL value for field {}: {}", fieldName, value);
                        }
                        
                        long conversionTime = System.nanoTime() - conversionStart;
                        DatabricksMetrics.logDataTypeConversion(fieldName, "DECIMAL", "DECIMAL", conversionTime);
                    } catch (SQLException ex) {
                        LOGGER.error("Error extracting DECIMAL value for field {}: SQLState={}, ErrorCode={}, Message={}", 
                                   fieldName, ex.getSQLState(), ex.getErrorCode(), ex.getMessage());
                        dst.isSet = 0;
                    } catch (Exception ex) {
                        LOGGER.error("Unexpected error extracting DECIMAL value for field {}: {}", fieldName, ex.getMessage(), ex);
                        dst.isSet = 0;
                    }
                };
            case DATEMILLI:
                // Databricks DATE handling
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
                {
                    long conversionStart = System.nanoTime();
                    try {
                        java.sql.Date value = resultSet.getDate(fieldName);
                        if (resultSet.wasNull() || value == null) {
                            dst.isSet = 0;
                            LOGGER.debug("DATE field {} is null", fieldName);
                        } else {
                            dst.isSet = 1;
                            dst.value = value.getTime();
                            LOGGER.trace("Extracted DATE value for field {}: {}", fieldName, value);
                        }
                        
                        long conversionTime = System.nanoTime() - conversionStart;
                        DatabricksMetrics.logDataTypeConversion(fieldName, "DATE", "DATEMILLI", conversionTime);
                    } catch (SQLException ex) {
                        LOGGER.error("Error extracting DATE value for field {}: SQLState={}, ErrorCode={}, Message={}", 
                                   fieldName, ex.getSQLState(), ex.getErrorCode(), ex.getMessage());
                        dst.isSet = 0;
                    } catch (Exception ex) {
                        LOGGER.error("Unexpected error extracting DATE value for field {}: {}", fieldName, ex.getMessage(), ex);
                        dst.isSet = 0;
                    }
                };
            case TIMESTAMPMILLITZ:
                // Databricks TIMESTAMP handling - using DateMilliExtractor for timestamps
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
                {
                    long conversionStart = System.nanoTime();
                    try {
                        java.sql.Timestamp value = resultSet.getTimestamp(fieldName);
                        if (resultSet.wasNull() || value == null) {
                            dst.isSet = 0;
                            LOGGER.debug("TIMESTAMP field {} is null", fieldName);
                        } else {
                            dst.isSet = 1;
                            dst.value = value.getTime();
                            LOGGER.trace("Extracted TIMESTAMP value for field {}: {}", fieldName, value);
                        }
                        
                        long conversionTime = System.nanoTime() - conversionStart;
                        DatabricksMetrics.logDataTypeConversion(fieldName, "TIMESTAMP", "TIMESTAMPMILLITZ", conversionTime);
                    } catch (SQLException ex) {
                        LOGGER.error("Error extracting TIMESTAMP value for field {}: SQLState={}, ErrorCode={}, Message={}", 
                                   fieldName, ex.getSQLState(), ex.getErrorCode(), ex.getMessage());
                        dst.isSet = 0;
                    } catch (Exception ex) {
                        LOGGER.error("Unexpected error extracting TIMESTAMP value for field {}: {}", fieldName, ex.getMessage(), ex);
                        dst.isSet = 0;
                    }
                };
            default:
                // Use the base implementation for other types
                LOGGER.trace("Using base extractor for field {} with type {}", fieldName, fieldType);
                return super.makeExtractor(field, resultSet, partitionValues);
        }
    }



    /**
     * Enable case-sensitive lookup for Databricks connections if needed.
     * Databricks is generally case-insensitive but this can be configured.
     */
    @Override
    protected boolean enableCaseSensitivelyLookUpSession(Connection connection)
    {
        try {
            // Databricks doesn't require special case sensitivity configuration
            // but we can add session parameters if needed
            LOGGER.debug("Databricks connection established, case sensitivity handled by default");
            return true;
        } catch (Exception ex) {
            LOGGER.warn("Failed to configure case sensitivity for Databricks connection: {}", ex.getMessage());
            return false;
        }
    }
}