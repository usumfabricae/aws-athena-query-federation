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

import com.amazonaws.athena.connectors.jdbc.MultiplexingJdbcCompositeHandler;

/**
 * Multiplexing composite handler that allows us to use a single Lambda function for both
 * Metadata and Data operations across multiple Databricks clusters. This handler supports
 * configuration-based cluster routing for multi-cluster deployments.
 * 
 * In this case we compose {@link DatabricksMuxMetadataHandler} and {@link DatabricksMuxRecordHandler}.
 */
public class DatabricksMuxCompositeHandler extends MultiplexingJdbcCompositeHandler
{
    /**
     * Creates a new DatabricksMuxCompositeHandler that supports multiple Databricks cluster configurations.
     * The handler will route requests to appropriate clusters based on catalog configuration.
     * 
     * @throws java.lang.ReflectiveOperationException if handler classes cannot be instantiated
     */
    public DatabricksMuxCompositeHandler() throws java.lang.ReflectiveOperationException
    {
        super(DatabricksMuxMetadataHandler.class, DatabricksMuxRecordHandler.class, 
              DatabricksMetadataHandler.class, DatabricksRecordHandler.class);
    }
}