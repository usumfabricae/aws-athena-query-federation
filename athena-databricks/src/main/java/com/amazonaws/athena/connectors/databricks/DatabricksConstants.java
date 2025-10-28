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

/**
 * Constants for the Databricks connector.
 */
public final class DatabricksConstants
{
    private DatabricksConstants() {}

    /**
     * The name of the Databricks connector.
     */
    public static final String DATABRICKS_NAME = "databricks";

    /**
     * The Databricks JDBC driver class name.
     */
    public static final String DATABRICKS_DRIVER_CLASS = "com.databricks.client.jdbc.Driver";

    /**
     * The default port for Databricks connections (HTTPS).
     */
    public static final int DATABRICKS_DEFAULT_PORT = 443;

    /**
     * The connection URL pattern for Databricks JDBC connections.
     */
    public static final String DATABRICKS_URL_PATTERN = "jdbc:databricks://%s:%d%s";

    /**
     * Environment variable for Databricks server hostname.
     */
    public static final String DATABRICKS_SERVER_HOSTNAME = "databricks_server_hostname";

    /**
     * Environment variable for Databricks HTTP path.
     */
    public static final String DATABRICKS_HTTP_PATH = "databricks_http_path";

    /**
     * Environment variable for Databricks access token.
     */
    public static final String DATABRICKS_TOKEN = "databricks_token";

    /**
     * Environment variable for Databricks catalog.
     */
    public static final String DATABRICKS_CATALOG = "databricks_catalog";

    /**
     * Environment variable for Databricks schema.
     */
    public static final String DATABRICKS_SCHEMA = "databricks_schema";

    /**
     * Environment variable for enabling SSL.
     */
    public static final String DATABRICKS_USE_SSL = "databricks_use_ssl";

    /**
     * Environment variable for connection timeout.
     */
    public static final String DATABRICKS_CONNECTION_TIMEOUT = "databricks_connection_timeout";

    /**
     * Environment variable for socket timeout.
     */
    public static final String DATABRICKS_SOCKET_TIMEOUT = "databricks_socket_timeout";
}