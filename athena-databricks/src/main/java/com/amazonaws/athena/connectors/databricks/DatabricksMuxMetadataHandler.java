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

import com.amazonaws.athena.connectors.jdbc.MultiplexingJdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandlerFactory;
import org.apache.arrow.util.VisibleForTesting;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Map;

import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.DATABRICKS_NAME;

/**
 * Factory class for creating DatabricksMetadataHandler instances in multiplexing scenarios.
 */
class DatabricksMetadataHandlerFactory implements JdbcMetadataHandlerFactory
{
    @Override
    public String getEngine()
    {
        return DATABRICKS_NAME;
    }

    @Override
    public JdbcMetadataHandler createJdbcMetadataHandler(DatabaseConnectionConfig config, Map<String, String> configOptions)
    {
        return new DatabricksMetadataHandler(config, configOptions);
    }
}

/**
 * Multiplexing metadata handler that supports multiple Databricks cluster configurations.
 * This handler routes metadata requests to appropriate Databricks clusters based on catalog configuration.
 */
public class DatabricksMuxMetadataHandler extends MultiplexingJdbcMetadataHandler
{
    /**
     * Creates a new DatabricksMuxMetadataHandler with the provided configuration options.
     * 
     * @param configOptions Configuration options for the handler
     */
    public DatabricksMuxMetadataHandler(Map<String, String> configOptions)
    {
        super(new DatabricksMetadataHandlerFactory(), configOptions);
    }

    /**
     * Creates a new DatabricksMuxMetadataHandler for testing purposes.
     * 
     * @param secretsManager The SecretsManager client
     * @param athena The Athena client
     * @param jdbcConnectionFactory The JDBC connection factory
     * @param metadataHandlerMap Map of metadata handlers by catalog
     * @param databaseConnectionConfig The database connection configuration
     * @param configOptions Configuration options for the handler
     */
    @VisibleForTesting
    protected DatabricksMuxMetadataHandler(SecretsManagerClient secretsManager, AthenaClient athena, 
            JdbcConnectionFactory jdbcConnectionFactory, Map<String, JdbcMetadataHandler> metadataHandlerMap, 
            DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> configOptions)
    {
        super(secretsManager, athena, jdbcConnectionFactory, metadataHandlerMap, databaseConnectionConfig, 
              configOptions, new DatabricksCaseResolver());
    }
}