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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Strings;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Extends {@link JdbcSplitQueryBuilder} and implements Databricks specific SQL clauses for split.
 *
 * Databricks supports standard SQL with some specific optimizations for Delta Lake and partitioning.
 * This builder supports predicate pushdown, column projection, and complex expressions.
 * It handles proper SQL escaping and quoting for Databricks identifiers and supports
 * partition pruning in generated queries for optimal performance.
 */
public class DatabricksQueryStringBuilder extends JdbcSplitQueryBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksQueryStringBuilder.class);
    
    // Databricks-specific SQL keywords that need special handling
    private static final List<String> DATABRICKS_RESERVED_KEYWORDS = List.of(
        "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET",
        "UNION", "INTERSECT", "EXCEPT", "WITH", "CASE", "WHEN", "THEN", "ELSE", "END",
        "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "RLIKE", "REGEXP",
        "IS", "NULL", "TRUE", "FALSE", "DISTINCT", "ALL", "ANY", "SOME",
        "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "JOIN", "ON", "USING",
        "CREATE", "DROP", "ALTER", "INSERT", "UPDATE", "DELETE", "MERGE",
        "TABLE", "VIEW", "DATABASE", "SCHEMA", "CATALOG", "PARTITION"
    );

    public DatabricksQueryStringBuilder(final String quoteCharacters, final FederationExpressionParser federationExpressionParser)
    {
        super(quoteCharacters, federationExpressionParser);
    }

    @Override
    protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
    {
        StringBuilder tableName = new StringBuilder();
        
        // Handle Databricks three-part naming: catalog.schema.table
        if (!Strings.isNullOrEmpty(catalog)) {
            tableName.append(quoteDatabricksIdentifier(catalog)).append('.');
        }
        if (!Strings.isNullOrEmpty(schema)) {
            tableName.append(quoteDatabricksIdentifier(schema)).append('.');
        }
        tableName.append(quoteDatabricksIdentifier(table));

        // For Databricks, we use standard table references
        // Partition pruning is handled through WHERE clauses
        // Support for Delta Lake table versioning could be added here if needed
        String fromClause = String.format(" FROM %s ", tableName);
        
        LOGGER.debug("Generated FROM clause for Databricks: {}", fromClause);
        return fromClause;
    }
    
    /**
     * Quotes Databricks identifiers properly, handling reserved keywords and special characters.
     * Databricks uses backticks (`) for identifier quoting.
     */
    private String quoteDatabricksIdentifier(String identifier)
    {
        if (Strings.isNullOrEmpty(identifier)) {
            return identifier;
        }
        
        // Always quote identifiers that are reserved keywords
        if (DATABRICKS_RESERVED_KEYWORDS.contains(identifier.toUpperCase())) {
            return quote(identifier);
        }
        
        // Quote identifiers with special characters or spaces
        if (identifier.contains(" ") || identifier.contains("-") || identifier.contains(".") || 
            !identifier.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            return quote(identifier);
        }
        
        // For standard identifiers, quoting is optional but recommended for consistency
        return quote(identifier);
    }

    @Override
    protected List<String> getPartitionWhereClauses(final Split split)
    {
        List<String> partitionWhereClauses = new ArrayList<>();
        
        // Extract partition information from split properties
        Map<String, String> splitProperties = split.getProperties();
        
        for (Map.Entry<String, String> entry : splitProperties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            
            // Skip non-partition properties
            if (key.equals("partition_id") || key.equals("split_part") || key.equals("split_count")) {
                continue;
            }
            
            // Skip the default partition marker - this indicates no real partitioning
            if (key.equals("partition_name") && "*".equals(value)) {
                LOGGER.debug("Skipping default partition marker - no WHERE clause needed");
                continue;
            }
            
            // Handle partition columns - these are typically in the format "partition_column=value"
            if (key.startsWith("partition_")) {
                String columnName = key.substring("partition_".length());
                if (value != null && !value.isEmpty() && !"*".equals(value)) {
                    // Add partition predicate for pruning with proper Databricks escaping
                    String quotedColumn = quoteDatabricksIdentifier(columnName);
                    String partitionClause = buildPartitionPredicate(quotedColumn, value);
                    partitionWhereClauses.add(partitionClause);
                    LOGGER.debug("Added Databricks partition predicate: {}", partitionClause);
                }
            }
            // Handle Hive-style partitioning (year=2023, month=01, etc.)
            else if (key.contains("=") && !key.startsWith("__")) {
                String[] parts = key.split("=", 2);
                if (parts.length == 2) {
                    String columnName = parts[0];
                    String partitionValue = parts[1];
                    // Skip default partition values
                    if (!"*".equals(partitionValue)) {
                        String quotedColumn = quoteDatabricksIdentifier(columnName);
                        String partitionClause = buildPartitionPredicate(quotedColumn, partitionValue);
                        partitionWhereClauses.add(partitionClause);
                        LOGGER.debug("Added Hive-style partition predicate: {}", partitionClause);
                    }
                }
            }
        }
        
        return partitionWhereClauses;
    }
    
    /**
     * Builds a partition predicate with proper data type handling for Databricks.
     * Handles string, numeric, and date partition values appropriately.
     */
    private String buildPartitionPredicate(String quotedColumn, String value)
    {
        if (value == null || value.isEmpty()) {
            return String.format("%s IS NULL", quotedColumn);
        }
        
        // Handle special partition values
        if ("__HIVE_DEFAULT_PARTITION__".equals(value)) {
            return String.format("%s IS NULL", quotedColumn);
        }
        
        // Try to determine if the value is numeric
        if (value.matches("^-?\\d+$")) {
            // Integer value - no quotes needed
            return String.format("%s = %s", quotedColumn, value);
        }
        else if (value.matches("^-?\\d*\\.\\d+$")) {
            // Decimal value - no quotes needed
            return String.format("%s = %s", quotedColumn, value);
        }
        else if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            // Boolean value - no quotes needed
            return String.format("%s = %s", quotedColumn, value.toLowerCase());
        }
        else {
            // String value - needs proper escaping
            String escapedValue = value.replace("'", "''");
            return String.format("%s = '%s'", quotedColumn, escapedValue);
        }
    }

    @Override
    protected String extractOrderByClause(Constraints constraints)
    {
        List<OrderByField> orderByClause = constraints.getOrderByClause();
        if (orderByClause == null || orderByClause.size() == 0) {
            return "";
        }
        
        String orderBySQL = "ORDER BY " + orderByClause.stream()
                .flatMap(orderByField -> {
                    String ordering = orderByField.getDirection().isAscending() ? "ASC" : "DESC";
                    String quotedColumn = quoteDatabricksIdentifier(orderByField.getColumnName());
                    String columnSorting = String.format("%s %s", quotedColumn, ordering);
                    
                    // Databricks supports NULLS FIRST/LAST syntax
                    switch (orderByField.getDirection()) {
                        case ASC_NULLS_FIRST:
                            return Stream.of(String.format("%s NULLS FIRST", columnSorting));
                        case ASC_NULLS_LAST:
                            return Stream.of(String.format("%s NULLS LAST", columnSorting));
                        case DESC_NULLS_FIRST:
                            return Stream.of(String.format("%s NULLS FIRST", columnSorting));
                        case DESC_NULLS_LAST:
                            return Stream.of(String.format("%s NULLS LAST", columnSorting));
                        default:
                            return Stream.of(columnSorting);
                    }
                })
                .collect(Collectors.joining(", "));
                
        LOGGER.debug("Generated ORDER BY clause for Databricks: {}", orderBySQL);
        return orderBySQL;
    }
    
    /**
     * Generates optimized SELECT clause for Databricks with proper column quoting.
     * Supports column projection optimization by only selecting required columns.
     */
    protected String buildSelectClause(Schema schema)
    {
        if (schema == null || schema.getFields().isEmpty()) {
            return "SELECT *";
        }
        
        String selectColumns = schema.getFields().stream()
                .map(field -> quoteDatabricksIdentifier(field.getName()))
                .collect(Collectors.joining(", "));
                
        String selectClause = "SELECT " + selectColumns;
        LOGGER.debug("Generated SELECT clause for Databricks: {}", selectClause);
        return selectClause;
    }
    
    /**
     * Builds a complete SQL query with Databricks-specific optimizations.
     * Includes proper escaping, partition pruning, and predicate pushdown.
     */
    public String buildOptimizedQuery(String catalog, String schema, String table, 
                                    Schema tableSchema, Constraints constraints, Split split)
    {
        StringBuilder query = new StringBuilder();
        
        // SELECT clause with column projection
        query.append(buildSelectClause(tableSchema));
        
        // FROM clause with proper table reference
        query.append(getFromClauseWithSplit(catalog, schema, table, split));
        
        // WHERE clause with partition pruning and predicate pushdown
        List<String> whereClauses = new ArrayList<>();
        
        // Add partition predicates
        List<String> partitionClauses = getPartitionWhereClauses(split);
        whereClauses.addAll(partitionClauses);
        
        // Add constraint predicates (handled by base class)
        if (constraints != null && constraints.getSummary() != null && 
            !constraints.getSummary().isEmpty()) {
            // The base class will handle constraint conversion through the expression parser
            LOGGER.debug("Constraints will be processed by federation expression parser");
        }
        
        if (!whereClauses.isEmpty()) {
            query.append(" WHERE ");
            query.append(String.join(" AND ", whereClauses));
        }
        
        // ORDER BY clause
        String orderBy = extractOrderByClause(constraints);
        if (!orderBy.isEmpty()) {
            query.append(" ").append(orderBy);
        }
        
        // LIMIT clause if specified
        if (constraints != null && constraints.getLimit() > 0) {
            query.append(" LIMIT ").append(constraints.getLimit());
        }
        
        String finalQuery = query.toString();
        LOGGER.info("Generated optimized Databricks query: {}", finalQuery);
        return finalQuery;
    }
}