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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.DATABRICKS_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.DATABRICKS_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.DATABRICKS_NAME;

/**
 * Handles metadata operations for Databricks connector.
 * Provides schema discovery, table listing, partition handling, and split generation
 * for Databricks data sources through the Athena Query Federation framework.
 */
public class DatabricksMetadataHandler extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksMetadataHandler.class);
    
    // JDBC properties for Databricks connections
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    
    // Partition-related constants
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition_name";
    static final String ALL_PARTITIONS = "*";
    static final String PARTITION_COLUMN_NAME = "partition_name";
    
    // Maximum number of splits to return in a single request
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;
    
    // Query to get partition information from Databricks
    static final String GET_PARTITIONS_QUERY = 
        "SELECT DISTINCT partition_id as partition_name " +
        "FROM system.information_schema.table_partitions " +
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?";
    
    // Query for paginated table listing
    static final String LIST_PAGINATED_TABLES_QUERY = 
        "SELECT table_name as TABLE_NAME, table_schema as TABLE_SCHEM " +
        "FROM system.information_schema.tables " +
        "WHERE table_schema = ? " +
        "ORDER BY table_name " +
        "LIMIT ? OFFSET ?";

    /**
     * Instantiates handler to be used by Lambda function directly.
     * 
     * @param configOptions Configuration options for the connector
     */
    public DatabricksMetadataHandler(Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(DATABRICKS_NAME, configOptions), configOptions);
    }

    /**
     * Used by Mux for multi-database support.
     * 
     * @param databaseConnectionConfig Database connection configuration
     * @param configOptions Configuration options for the connector
     */
    public DatabricksMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, 
             new DatabricksConnectionFactory(databaseConnectionConfig, new DatabricksEnvironmentProperties()), 
             configOptions);
    }

    /**
     * Constructor with custom JDBC connection factory.
     * 
     * @param databaseConnectionConfig Database connection configuration
     * @param jdbcConnectionFactory JDBC connection factory
     * @param configOptions Configuration options for the connector
     */
    public DatabricksMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, 
                                   JdbcConnectionFactory jdbcConnectionFactory, 
                                   Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, jdbcConnectionFactory, configOptions, 
             new DatabricksCaseResolver());
    }

    /**
     * Constructor with custom case resolver.
     * 
     * @param databaseConnectionConfig Database connection configuration
     * @param jdbcConnectionFactory JDBC connection factory
     * @param configOptions Configuration options for the connector
     * @param caseResolver Case resolver for identifier handling
     */
    public DatabricksMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, 
                                   JdbcConnectionFactory jdbcConnectionFactory, 
                                   Map<String, String> configOptions,
                                   DatabricksCaseResolver caseResolver)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions, caseResolver);
    }

    /**
     * Constructor for testing with custom clients.
     * 
     * @param databaseConnectionConfig Database connection configuration
     * @param secretsManager Secrets Manager client
     * @param athena Athena client
     * @param jdbcConnectionFactory JDBC connection factory
     * @param configOptions Configuration options for the connector
     */
    @VisibleForTesting
    protected DatabricksMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig,
                                      SecretsManagerClient secretsManager,
                                      AthenaClient athena,
                                      JdbcConnectionFactory jdbcConnectionFactory,
                                      Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions);
    }

    /**
     * Advertises the data source capabilities including predicate pushdown support.
     * 
     * @param allocator Block allocator for memory management
     * @param request Request containing catalog information
     * @return Response with supported capabilities
     */
    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, 
                                                                        GetDataSourceCapabilitiesRequest request)
    {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        
        // Advertise filter pushdown capabilities
        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
            FilterPushdownSubType.SORTED_RANGE_SET, 
            FilterPushdownSubType.NULLABLE_COMPARISON
        ));
        
        // Advertise complex expression pushdown capabilities
        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
            ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
                .withSubTypeProperties(Arrays.stream(StandardFunctions.values())
                    .map(standardFunctions -> standardFunctions.getFunctionName().getFunctionName())
                    .toArray(String[]::new))
        ));

        // Add query passthrough capability if enabled
        jdbcQueryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    /**
     * Defines the partition schema for Databricks tables.
     * Returns minimal schema with partition_name field to support split generation.
     * 
     * @param catalogName The catalog name
     * @return Minimal schema with partition_name field
     */
    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        // Return minimal schema with partition_name field to support split generation
        // This prevents partition-based WHERE clauses while maintaining compatibility
        return SchemaBuilder.newBuilder()
                .addStringField(BLOCK_PARTITION_COLUMN_NAME)
                .build();
    } 
   /**
     * Discovers and returns partition information for Databricks tables.
     * Returns a single default partition to ensure proper split generation.
     * 
     * @param blockWriter Writer for partition data
     * @param getTableLayoutRequest Request containing table information
     * @param queryStatusChecker Checker for query status
     * @throws Exception if partition discovery fails
     */
    @Override
    public void getPartitions(final BlockWriter blockWriter, 
                            final GetTableLayoutRequest getTableLayoutRequest, 
                            QueryStatusChecker queryStatusChecker) throws Exception
    {
        // Write a single default partition to ensure proper split generation
        // This prevents partition-based WHERE clauses while maintaining compatibility
        blockWriter.writeRows((Block block, int rowNum) -> {
            block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
            LOGGER.info("Added default partition for table {}.{}", 
                       getTableLayoutRequest.getTableName().getSchemaName(),
                       getTableLayoutRequest.getTableName().getTableName());
            return 1; // wrote 1 row
        });
    }
    
    /**
     * Internal method to get partitions with proper error handling.
     * 
     * @return Number of partitions discovered
     */
    private int getPartitionsInternal(final BlockWriter blockWriter, 
                                    final GetTableLayoutRequest getTableLayoutRequest, 
                                    QueryStatusChecker queryStatusChecker) throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            TableName tableName = getTableLayoutRequest.getTableName();
            
            // Validate connection
            if (!connection.isValid(5)) {
                throw new SQLException("Connection validation failed");
            }
            
            // Try to get partitions from Databricks system tables
            List<String> parameters = Arrays.asList(
                connection.getCatalog(), // catalog
                tableName.getSchemaName(), // schema
                tableName.getTableName() // table
            );
            
            try (PreparedStatement preparedStatement = new PreparedStatementBuilder()
                    .withConnection(connection)
                    .withQuery(GET_PARTITIONS_QUERY)
                    .withParameters(parameters)
                    .build();
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                
                boolean hasPartitions = false;
                int partitionCount = 0;
                
                // Process partition results
                while (resultSet.next() && queryStatusChecker.isQueryRunning()) {
                    hasPartitions = true;
                    final String partitionName = resultSet.getString(PARTITION_COLUMN_NAME);
                    
                    if (partitionName != null && !partitionName.trim().isEmpty()) {
                        blockWriter.writeRows((Block block, int rowNum) -> {
                            block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionName);
                            LOGGER.debug("Adding partition {}", partitionName);
                            return 1; // wrote 1 row
                        });
                        partitionCount++;
                    }
                }
                
                // If no partitions found, create a single partition for the entire table
                if (!hasPartitions) {
                    blockWriter.writeRows((Block block, int rowNum) -> {
                        block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                        LOGGER.info("No partitions found, adding single partition {}", ALL_PARTITIONS);
                        return 1; // wrote 1 row
                    });
                    partitionCount = 1;
                }
                
                LOGGER.debug("Retrieved {} partitions for table {}.{}", 
                           partitionCount, tableName.getSchemaName(), tableName.getTableName());
                
                return partitionCount;
            }
            catch (SQLException e) {
                // If partition query fails, fall back to single partition
                LOGGER.warn("Failed to query partitions for table {}.{}, using single partition: {}", 
                           tableName.getSchemaName(), tableName.getTableName(), e.getMessage());
                
                blockWriter.writeRows((Block block, int rowNum) -> {
                    block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                    LOGGER.info("Using fallback single partition {}", ALL_PARTITIONS);
                    return 1; // wrote 1 row
                });
                
                return 1; // Single partition fallback
            }
        }
        catch (Exception e) {
            LOGGER.error("Error in partition discovery for {}.{}: {}", 
                        getTableLayoutRequest.getTableName().getSchemaName(),
                        getTableLayoutRequest.getTableName().getTableName(), 
                        e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Lists tables in the specified Databricks schema with pagination support.
     * 
     * @param connection JDBC connection to Databricks
     * @param listTablesRequest Request containing schema and pagination information
     * @return Response with paginated table list
     * @throws SQLException if table listing fails
     */
    @Override
    protected ListTablesResponse listPaginatedTables(final Connection connection, 
                                                   final ListTablesRequest listTablesRequest) throws SQLException
    {
        String token = listTablesRequest.getNextToken();
        int pageSize = listTablesRequest.getPageSize();
        
        int offset = token != null ? Integer.parseInt(token) : 0;
        
        LOGGER.info("Starting pagination at offset {} with page size {}", offset, pageSize);
        
        List<TableName> paginatedTables = getPaginatedTables(connection, 
                                                            listTablesRequest.getSchemaName(), 
                                                            pageSize, 
                                                            offset);
        
        LOGGER.info("{} tables returned. Next offset would be {}", paginatedTables.size(), offset + pageSize);
        
        // Calculate next token - null if we've reached the end
        String nextToken = paginatedTables.isEmpty() || paginatedTables.size() < pageSize ? 
                          null : Integer.toString(offset + pageSize);
        
        return new ListTablesResponse(listTablesRequest.getCatalogName(), paginatedTables, nextToken);
    }

    /**
     * Gets a paginated list of tables from Databricks using LIMIT and OFFSET.
     * 
     * @param connection JDBC connection to Databricks
     * @param databaseName Schema name to list tables from
     * @param limit Maximum number of tables to return
     * @param offset Number of tables to skip
     * @return List of table names
     * @throws SQLException if query execution fails
     */
    @VisibleForTesting
    protected List<TableName> getPaginatedTables(Connection connection, 
                                               String databaseName, 
                                               int limit, 
                                               int offset) throws SQLException
    {
        PreparedStatement preparedStatement = connection.prepareStatement(LIST_PAGINATED_TABLES_QUERY);
        preparedStatement.setString(1, databaseName);
        preparedStatement.setInt(2, limit);
        preparedStatement.setInt(3, offset);
        
        LOGGER.debug("Prepared Statement for getting tables in schema {} : {}", databaseName, preparedStatement);
        
        return JDBCUtil.getTableMetadata(preparedStatement, TABLES_AND_VIEWS);
    }

    /**
     * Lists all tables in the specified Databricks schema.
     * Uses Databricks system tables for comprehensive table discovery.
     * 
     * @param jdbcConnection JDBC connection to Databricks
     * @param databaseName Schema name to list tables from
     * @return List of all tables in the schema
     * @throws SQLException if table listing fails
     */
    @Override
    protected List<TableName> listTables(final Connection jdbcConnection, final String databaseName) throws SQLException
    {
        LOGGER.debug("Listing tables for schema: {}", databaseName);
        
        try {
            // Try using Databricks system tables first
            String query = "SELECT table_name as \"TABLE_NAME\", table_schema as \"TABLE_SCHEM\" " +
                          "FROM system.information_schema.tables " +
                          "WHERE table_schema = ? " +
                          "ORDER BY table_name";
            
            try (PreparedStatement preparedStatement = jdbcConnection.prepareStatement(query)) {
                preparedStatement.setString(1, databaseName);
                
                return JDBCUtil.getTableMetadata(preparedStatement, TABLES_AND_VIEWS);
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to query system.information_schema.tables, falling back to standard JDBC metadata: {}", 
                       e.getMessage());
            
            // Fall back to standard JDBC metadata approach
            return JDBCUtil.getTables(jdbcConnection, databaseName);
        }
    }

    /**
     * Generates splits for parallel query execution.
     * Creates optimal work units based on partition information and supports query passthrough.
     * 
     * @param blockAllocator Block allocator for memory management
     * @param getSplitsRequest Request containing table and constraint information
     * @return Response with generated splits for parallel processing
     */
    @Override
    public GetSplitsResponse doGetSplits(final BlockAllocator blockAllocator, 
                                       final GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("{}: Generating splits for catalog {}, table {}", 
                   getSplitsRequest.getQueryId(), 
                   getSplitsRequest.getTableName().getSchemaName(), 
                   getSplitsRequest.getTableName().getTableName());
        
        // Handle query passthrough requests
        if (getSplitsRequest.getConstraints().isQueryPassThrough()) {
            LOGGER.info("Query passthrough split requested");
            return setupQueryPassthroughSplit(getSplitsRequest);
        }

        // Handle continuation token for pagination
        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();

        // If no partitions (empty schema), create a single split for the entire table
        if (partitions.getRowCount() == 0) {
            LOGGER.info("No partitions found, creating single split for entire table");
            SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);
            Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey());
            splits.add(splitBuilder.build());
        }
        else {
            // Generate splits based on partitions
            for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
                FieldReader locationReader = partitions.getFieldReader(BLOCK_PARTITION_COLUMN_NAME);
                locationReader.setPosition(curPartition);

                SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);
                String partitionName = String.valueOf(locationReader.readText());

                LOGGER.debug("{}: Creating split for partition {}", getSplitsRequest.getQueryId(), partitionName);

                // Build split with partition information
                Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(BLOCK_PARTITION_COLUMN_NAME, partitionName);

                splits.add(splitBuilder.build());

                // Check if we've reached the maximum splits per request
                if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                    LOGGER.info("Reached maximum splits per request ({}), returning with continuation token", 
                               MAX_SPLITS_PER_REQUEST);
                    return new GetSplitsResponse(getSplitsRequest.getCatalogName(), 
                                               splits, 
                                               encodeContinuationToken(curPartition + 1));
                }
            }
        }

        LOGGER.info("Generated {} splits for table {}.{}", 
                   splits.size(),
                   getSplitsRequest.getTableName().getSchemaName(),
                   getSplitsRequest.getTableName().getTableName());

        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
    }

    /**
     * Decodes continuation token for split pagination.
     * 
     * @param request The get splits request
     * @return The partition index to continue from
     */
    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.valueOf(request.getContinuationToken());
        }
        return 0; // No continuation token present
    }

    /**
     * Encodes continuation token for split pagination.
     * 
     * @param partition The partition index to encode
     * @return The encoded continuation token
     */
    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }
}