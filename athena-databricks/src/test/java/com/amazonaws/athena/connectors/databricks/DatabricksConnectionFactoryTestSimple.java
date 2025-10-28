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

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import org.junit.Test;
import org.junit.Assert;

import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.DATABRICKS_NAME;

public class DatabricksConnectionFactoryTestSimple
{
    @Test
    public void testConnectionFactoryCreation()
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig("testCatalog", DATABRICKS_NAME,
                "databricks://jdbc:databricks://hostname/user=A&password=B");
        
        DatabricksEnvironmentProperties envProperties = new DatabricksEnvironmentProperties();
        DatabricksConnectionFactory factory = new DatabricksConnectionFactory(config, envProperties);
        
        Assert.assertNotNull("Connection factory should not be null", factory);
    }

    @Test
    public void testDatabaseConnectionConfig()
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig("testCatalog", DATABRICKS_NAME,
                "databricks://jdbc:databricks://hostname/user=A&password=B");
        
        Assert.assertEquals("testCatalog", config.getCatalog());
        Assert.assertEquals(DATABRICKS_NAME, config.getEngine());
        Assert.assertTrue(config.getJdbcConnectionString().contains("databricks"));
    }
}