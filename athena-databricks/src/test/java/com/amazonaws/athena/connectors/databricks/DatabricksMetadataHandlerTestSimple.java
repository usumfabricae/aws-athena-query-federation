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

import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.DATABRICKS_NAME;

public class DatabricksMetadataHandlerTestSimple
{
    private DatabricksMetadataHandler metadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    @Before
    public void setup()
    {
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        
        DatabaseConnectionConfig config = new DatabaseConnectionConfig("testCatalog", DATABRICKS_NAME,
                "databricks://jdbc:databricks://hostname/user=A&password=B");
        
        this.metadataHandler = new DatabricksMetadataHandler(config, secretsManager, athena, jdbcConnectionFactory, ImmutableMap.of());
    }

    @Test
    public void testGetDataSourceCapabilities()
    {
        FederatedIdentity identity = Mockito.mock(FederatedIdentity.class);
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(identity, "testCatalog", "testQueryId");
        
        GetDataSourceCapabilitiesResponse response = metadataHandler.doGetDataSourceCapabilities(null, request);
        
        Assert.assertNotNull("Response should not be null", response);
        Assert.assertNotNull("Capabilities should not be null", response.getCapabilities());
    }

    @Test
    public void testMetadataHandlerCreation()
    {
        Assert.assertNotNull("Metadata handler should not be null", metadataHandler);
    }
}