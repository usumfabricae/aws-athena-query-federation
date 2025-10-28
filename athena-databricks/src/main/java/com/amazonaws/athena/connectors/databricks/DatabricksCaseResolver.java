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

import com.amazonaws.athena.connectors.jdbc.resolver.JDBCCaseResolver;

/**
 * Handles case sensitivity and identifier quoting for Databricks.
 */
public class DatabricksCaseResolver extends JDBCCaseResolver
{
    /**
     * Creates a new DatabricksCaseResolver.
     */
    public DatabricksCaseResolver()
    {
        // Databricks source type
        super("databricks");
    }

    public String quoteIdentifier(String identifier)
    {
        // Databricks uses backticks for quoting identifiers
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        
        // Check if identifier needs quoting (contains spaces, special characters, or is a reserved word)
        if (needsQuoting(identifier)) {
            return "`" + identifier.replace("`", "``") + "`";
        }
        
        return identifier;
    }

    /**
     * Determines if an identifier needs to be quoted.
     *
     * @param identifier The identifier to check
     * @return true if the identifier needs quoting, false otherwise
     */
    private boolean needsQuoting(String identifier)
    {
        if (identifier == null || identifier.isEmpty()) {
            return false;
        }

        // Check if identifier contains spaces or special characters
        if (identifier.contains(" ") || identifier.contains("-") || identifier.contains(".")) {
            return true;
        }

        // Check if identifier starts with a number
        if (Character.isDigit(identifier.charAt(0))) {
            return true;
        }

        // Check for common SQL reserved words that might need quoting in Databricks
        String upperIdentifier = identifier.toUpperCase();
        return isReservedWord(upperIdentifier);
    }

    /**
     * Checks if the identifier is a reserved word in Databricks.
     *
     * @param upperIdentifier The identifier in uppercase
     * @return true if it's a reserved word, false otherwise
     */
    private boolean isReservedWord(String upperIdentifier)
    {
        // Common SQL reserved words that might conflict in Databricks
        switch (upperIdentifier) {
            case "SELECT":
            case "FROM":
            case "WHERE":
            case "GROUP":
            case "ORDER":
            case "BY":
            case "HAVING":
            case "INSERT":
            case "UPDATE":
            case "DELETE":
            case "CREATE":
            case "DROP":
            case "ALTER":
            case "TABLE":
            case "VIEW":
            case "INDEX":
            case "DATABASE":
            case "SCHEMA":
            case "CATALOG":
            case "PARTITION":
            case "LOCATION":
            case "USING":
            case "OPTIONS":
            case "TBLPROPERTIES":
                return true;
            default:
                return false;
        }
    }
}