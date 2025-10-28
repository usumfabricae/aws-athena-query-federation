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

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * Composite handler that combines metadata and record handlers for Databricks.
 * This is the main entry point for the Lambda function and handles Lambda function lifecycle management.
 * Includes performance monitoring and structured logging capabilities.
 * 
 * Recommend using {@link DatabricksMuxCompositeHandler} for multi-cluster support instead.
 */
public class DatabricksCompositeHandler extends CompositeHandler
{
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(DatabricksCompositeHandler.class);
    
    /**
     * Creates a new DatabricksCompositeHandler with proper environment configuration.
     * Initializes both metadata and record handlers with Databricks-specific configuration.
     */
    public DatabricksCompositeHandler()
    {
        super(new DatabricksMetadataHandler(System.getenv()), 
              new DatabricksRecordHandler(System.getenv()));
        
        LOGGER.info("Initialized DatabricksCompositeHandler with performance monitoring");
        
        // Log performance summary periodically (every 100 operations)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Lambda function shutting down, logging final performance metrics");
            DatabricksMetrics.logPerformanceSummary();
        }));
    }
}