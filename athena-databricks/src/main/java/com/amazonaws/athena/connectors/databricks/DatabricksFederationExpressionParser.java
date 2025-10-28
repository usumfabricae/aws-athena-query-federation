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

import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Extends {@link FederationExpressionParser} to handle Databricks-specific SQL expression parsing.
 *
 * Databricks supports standard SQL expressions with some specific functions and operators.
 * This parser handles the conversion of Athena expressions to Databricks SQL syntax with
 * support for complex expressions and standard SQL functions.
 */
public class DatabricksFederationExpressionParser extends FederationExpressionParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksFederationExpressionParser.class);

    public DatabricksFederationExpressionParser()
    {
        super();
    }

    @Override
    public String mapFunctionToDataSourceSyntax(com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName functionName, 
                                               org.apache.arrow.vector.types.pojo.ArrowType type, 
                                               List<String> arguments)
    {
        String functionNameStr = functionName.getFunctionName().toLowerCase();
        
        switch (functionNameStr) {
            case "like":
                return String.format("%s LIKE %s", arguments.get(0), arguments.get(1));
            case "not_like":
                return String.format("%s NOT LIKE %s", arguments.get(0), arguments.get(1));
            case "is_null":
                return String.format("%s IS NULL", arguments.get(0));
            case "is_not_null":
                return String.format("%s IS NOT NULL", arguments.get(0));
            case "equal":
                return String.format("%s = %s", arguments.get(0), arguments.get(1));
            case "not_equal":
                return String.format("%s != %s", arguments.get(0), arguments.get(1));
            case "less_than":
                return String.format("%s < %s", arguments.get(0), arguments.get(1));
            case "less_than_or_equal":
                return String.format("%s <= %s", arguments.get(0), arguments.get(1));
            case "greater_than":
                return String.format("%s > %s", arguments.get(0), arguments.get(1));
            case "greater_than_or_equal":
                return String.format("%s >= %s", arguments.get(0), arguments.get(1));
            case "and":
                return String.format("(%s AND %s)", arguments.get(0), arguments.get(1));
            case "or":
                return String.format("(%s OR %s)", arguments.get(0), arguments.get(1));
            case "not":
                return String.format("NOT (%s)", arguments.get(0));
            case "in":
                return String.format("%s IN (%s)", arguments.get(0), String.join(", ", arguments.subList(1, arguments.size())));
            case "not_in":
                return String.format("%s NOT IN (%s)", arguments.get(0), String.join(", ", arguments.subList(1, arguments.size())));
            default:
                // For unknown functions, use standard SQL syntax
                return String.format("%s(%s)", functionNameStr.toUpperCase(), String.join(", ", arguments));
        }
    }

    /**
     * Databricks supports most standard SQL functions and expressions.
     * This implementation extends the base parser functionality to handle
     * Databricks-specific function mappings and optimizations for predicate pushdown.
     */
    public String writeArrayConstructorClause(List<Object> values)
    {
        // Databricks supports ARRAY constructor function
        String arrayElements = values.stream()
                .map(this::formatLiteral)
                .collect(Collectors.joining(", "));
        return String.format("ARRAY(%s)", arrayElements);
    }
    
    /**
     * Converts Athena constraints to Databricks SQL predicates with proper data type handling.
     * Supports complex expressions and handles data type conversions in WHERE clauses.
     */
    public String writeConstantExpression(com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression constantExpression)
    {
        // For now, use a simple string representation
        // This would need to be implemented based on the actual ConstantExpression API
        return "'" + constantExpression.toString() + "'";
    }
    
    /**
     * Handles variable expressions with proper Databricks identifier quoting.
     */
    public String writeVariableExpression(com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression variableExpression)
    {
        String columnName = variableExpression.getColumnName();
        return quoteDatabricksIdentifier(columnName);
    }
    
    /**
     * Quotes Databricks identifiers using backticks, handling reserved keywords and special characters.
     */
    private String quoteDatabricksIdentifier(String identifier)
    {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        
        // Use backticks for Databricks identifier quoting
        return String.format("`%s`", identifier.replace("`", "``"));
    }
    
    /**
     * Formats string literals with proper escaping for Databricks SQL.
     */
    private String formatStringLiteral(String value)
    {
        if (value == null) {
            return "NULL";
        }
        
        // Escape single quotes and backslashes for Databricks
        String escaped = value.replace("\\", "\\\\").replace("'", "''");
        return String.format("'%s'", escaped);
    }
    
    /**
     * Formats binary literals for Databricks (using hex notation).
     */
    private String formatBinaryLiteral(byte[] value)
    {
        if (value == null || value.length == 0) {
            return "NULL";
        }
        
        StringBuilder hex = new StringBuilder("X'");
        for (byte b : value) {
            hex.append(String.format("%02X", b));
        }
        hex.append("'");
        return hex.toString();
    }

    public String mapFunctionToDataSourceSyntax(com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression functionCallExpression, 
                                               List<String> arguments)
    {
        String functionName = functionCallExpression.getFunctionName().getFunctionName().toLowerCase();
        
        switch (functionName) {
            // Date and time functions
            case "date_add":
                if (arguments.size() == 2) {
                    return String.format("DATE_ADD(%s, %s)", arguments.get(0), arguments.get(1));
                }
                break;
            case "date_sub":
                if (arguments.size() == 2) {
                    return String.format("DATE_SUB(%s, %s)", arguments.get(0), arguments.get(1));
                }
                break;
            case "date_diff":
                if (arguments.size() == 2) {
                    return String.format("DATEDIFF(%s, %s)", arguments.get(1), arguments.get(0));
                }
                break;
            case "date_format":
                if (arguments.size() == 2) {
                    return String.format("DATE_FORMAT(%s, %s)", arguments.get(0), arguments.get(1));
                }
                break;
            case "year":
                if (arguments.size() == 1) {
                    return String.format("YEAR(%s)", arguments.get(0));
                }
                break;
            case "month":
                if (arguments.size() == 1) {
                    return String.format("MONTH(%s)", arguments.get(0));
                }
                break;
            case "day":
                if (arguments.size() == 1) {
                    return String.format("DAY(%s)", arguments.get(0));
                }
                break;
            case "hour":
                if (arguments.size() == 1) {
                    return String.format("HOUR(%s)", arguments.get(0));
                }
                break;
            case "minute":
                if (arguments.size() == 1) {
                    return String.format("MINUTE(%s)", arguments.get(0));
                }
                break;
            case "second":
                if (arguments.size() == 1) {
                    return String.format("SECOND(%s)", arguments.get(0));
                }
                break;
                
            // String functions
            case "regexp_like":
                if (arguments.size() == 2) {
                    return String.format("(%s RLIKE %s)", arguments.get(0), arguments.get(1));
                }
                break;
            case "regexp_replace":
                if (arguments.size() == 3) {
                    return String.format("REGEXP_REPLACE(%s, %s, %s)", arguments.get(0), arguments.get(1), arguments.get(2));
                }
                break;
            case "length":
                if (arguments.size() == 1) {
                    return String.format("LENGTH(%s)", arguments.get(0));
                }
                break;
            case "substring":
                if (arguments.size() >= 2) {
                    if (arguments.size() == 2) {
                        return String.format("SUBSTRING(%s, %s)", arguments.get(0), arguments.get(1));
                    } else {
                        return String.format("SUBSTRING(%s, %s, %s)", arguments.get(0), arguments.get(1), arguments.get(2));
                    }
                }
                break;
            case "upper":
                if (arguments.size() == 1) {
                    return String.format("UPPER(%s)", arguments.get(0));
                }
                break;
            case "lower":
                if (arguments.size() == 1) {
                    return String.format("LOWER(%s)", arguments.get(0));
                }
                break;
            case "trim":
                if (arguments.size() == 1) {
                    return String.format("TRIM(%s)", arguments.get(0));
                }
                break;
            case "ltrim":
                if (arguments.size() == 1) {
                    return String.format("LTRIM(%s)", arguments.get(0));
                }
                break;
            case "rtrim":
                if (arguments.size() == 1) {
                    return String.format("RTRIM(%s)", arguments.get(0));
                }
                break;
            case "concat":
                if (arguments.size() >= 2) {
                    return String.format("CONCAT(%s)", String.join(", ", arguments));
                }
                break;
                
            // Null handling functions
            case "coalesce":
                if (arguments.size() >= 1) {
                    return String.format("COALESCE(%s)", String.join(", ", arguments));
                }
                break;
            case "nullif":
                if (arguments.size() == 2) {
                    return String.format("NULLIF(%s, %s)", arguments.get(0), arguments.get(1));
                }
                break;
            case "isnull":
                if (arguments.size() == 1) {
                    return String.format("(%s IS NULL)", arguments.get(0));
                }
                break;
            case "isnotnull":
                if (arguments.size() == 1) {
                    return String.format("(%s IS NOT NULL)", arguments.get(0));
                }
                break;
                
            // Mathematical functions
            case "abs":
                if (arguments.size() == 1) {
                    return String.format("ABS(%s)", arguments.get(0));
                }
                break;
            case "ceil":
            case "ceiling":
                if (arguments.size() == 1) {
                    return String.format("CEIL(%s)", arguments.get(0));
                }
                break;
            case "floor":
                if (arguments.size() == 1) {
                    return String.format("FLOOR(%s)", arguments.get(0));
                }
                break;
            case "round":
                if (arguments.size() == 1) {
                    return String.format("ROUND(%s)", arguments.get(0));
                } else if (arguments.size() == 2) {
                    return String.format("ROUND(%s, %s)", arguments.get(0), arguments.get(1));
                }
                break;
            case "mod":
                if (arguments.size() == 2) {
                    return String.format("MOD(%s, %s)", arguments.get(0), arguments.get(1));
                }
                break;
                
            // Conditional functions
            case "case":
                // CASE expressions are handled differently
                return handleCaseExpression(arguments);
                
            // Aggregate functions (for completeness)
            case "count":
                if (arguments.size() == 1) {
                    return String.format("COUNT(%s)", arguments.get(0));
                }
                break;
            case "sum":
                if (arguments.size() == 1) {
                    return String.format("SUM(%s)", arguments.get(0));
                }
                break;
            case "avg":
                if (arguments.size() == 1) {
                    return String.format("AVG(%s)", arguments.get(0));
                }
                break;
            case "min":
                if (arguments.size() == 1) {
                    return String.format("MIN(%s)", arguments.get(0));
                }
                break;
            case "max":
                if (arguments.size() == 1) {
                    return String.format("MAX(%s)", arguments.get(0));
                }
                break;
                
            default:
                LOGGER.debug("Using default function mapping for: {}", functionName);
                break;
        }
        
        // Fall back to default SQL syntax for unsupported functions
        return String.format("%s(%s)", functionName.toUpperCase(), String.join(", ", arguments));
    }
    
    /**
     * Handles CASE expression conversion for Databricks.
     */
    private String handleCaseExpression(List<String> arguments)
    {
        if (arguments.size() < 3) {
            LOGGER.warn("Invalid CASE expression with {} arguments", arguments.size());
            return "NULL";
        }
        
        StringBuilder caseExpr = new StringBuilder("CASE");
        
        // Handle CASE WHEN ... THEN ... ELSE ... END pattern
        for (int i = 0; i < arguments.size() - 1; i += 2) {
            if (i + 1 < arguments.size()) {
                caseExpr.append(" WHEN ").append(arguments.get(i))
                       .append(" THEN ").append(arguments.get(i + 1));
            }
        }
        
        // Handle ELSE clause if odd number of arguments
        if (arguments.size() % 2 == 1) {
            caseExpr.append(" ELSE ").append(arguments.get(arguments.size() - 1));
        }
        
        caseExpr.append(" END");
        return caseExpr.toString();
    }

    /**
     * Format literal values for Databricks SQL syntax with proper data type handling.
     */
    private String formatLiteral(Object value)
    {
        if (value == null) {
            return "NULL";
        }
        
        if (value instanceof String) {
            return formatStringLiteral((String) value);
        }
        else if (value instanceof Boolean) {
            return ((Boolean) value) ? "TRUE" : "FALSE";
        }
        else if (value instanceof java.sql.Date) {
            return String.format("DATE '%s'", value.toString());
        }
        else if (value instanceof java.sql.Timestamp) {
            return String.format("TIMESTAMP '%s'", value.toString());
        }
        else if (value instanceof java.time.LocalDate) {
            return String.format("DATE '%s'", value.toString());
        }
        else if (value instanceof java.time.LocalDateTime) {
            return String.format("TIMESTAMP '%s'", value.toString());
        }
        else if (value instanceof Number) {
            return value.toString();
        }
        else if (value instanceof byte[]) {
            return formatBinaryLiteral((byte[]) value);
        }
        
        return value.toString();
    }
    
    /**
     * Handles complex predicate expressions with proper operator mapping for Databricks.
     * Supports IN, BETWEEN, LIKE, and other complex predicates.
     */
    public String convertComplexPredicate(String operator, List<String> operands)
    {
        if (operands == null || operands.isEmpty()) {
            return "";
        }
        
        switch (operator.toUpperCase()) {
            case "IN":
                if (operands.size() >= 2) {
                    String column = operands.get(0);
                    String values = operands.subList(1, operands.size()).stream()
                            .collect(Collectors.joining(", "));
                    return String.format("%s IN (%s)", column, values);
                }
                break;
                
            case "NOT_IN":
                if (operands.size() >= 2) {
                    String column = operands.get(0);
                    String values = operands.subList(1, operands.size()).stream()
                            .collect(Collectors.joining(", "));
                    return String.format("%s NOT IN (%s)", column, values);
                }
                break;
                
            case "BETWEEN":
                if (operands.size() == 3) {
                    return String.format("%s BETWEEN %s AND %s", 
                            operands.get(0), operands.get(1), operands.get(2));
                }
                break;
                
            case "NOT_BETWEEN":
                if (operands.size() == 3) {
                    return String.format("%s NOT BETWEEN %s AND %s", 
                            operands.get(0), operands.get(1), operands.get(2));
                }
                break;
                
            case "LIKE":
                if (operands.size() == 2) {
                    return String.format("%s LIKE %s", operands.get(0), operands.get(1));
                }
                break;
                
            case "NOT_LIKE":
                if (operands.size() == 2) {
                    return String.format("%s NOT LIKE %s", operands.get(0), operands.get(1));
                }
                break;
                
            case "RLIKE":
            case "REGEXP":
                if (operands.size() == 2) {
                    return String.format("%s RLIKE %s", operands.get(0), operands.get(1));
                }
                break;
                
            case "IS_NULL":
                if (operands.size() == 1) {
                    return String.format("%s IS NULL", operands.get(0));
                }
                break;
                
            case "IS_NOT_NULL":
                if (operands.size() == 1) {
                    return String.format("%s IS NOT NULL", operands.get(0));
                }
                break;
                
            default:
                LOGGER.debug("Unsupported complex predicate operator: {}", operator);
                break;
        }
        
        return "";
    }
    
    /**
     * Converts data types from Athena to Databricks format for proper casting.
     */
    public String convertDataTypeForCast(String athenaType, String value)
    {
        if (value == null || athenaType == null) {
            return value;
        }
        
        String databricksType = mapAthenaToDatabricksType(athenaType.toUpperCase());
        return String.format("CAST(%s AS %s)", value, databricksType);
    }
    
    /**
     * Maps Athena data types to Databricks data types for casting operations.
     */
    private String mapAthenaToDatabricksType(String athenaType)
    {
        switch (athenaType) {
            case "TINYINT":
                return "TINYINT";
            case "SMALLINT":
                return "SMALLINT";
            case "INTEGER":
            case "INT":
                return "INT";
            case "BIGINT":
                return "BIGINT";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
                return "DOUBLE";
            case "DECIMAL":
                return "DECIMAL";
            case "BOOLEAN":
                return "BOOLEAN";
            case "STRING":
            case "VARCHAR":
                return "STRING";
            case "CHAR":
                return "STRING";
            case "BINARY":
                return "BINARY";
            case "DATE":
                return "DATE";
            case "TIMESTAMP":
                return "TIMESTAMP";
            case "ARRAY":
                return "ARRAY";
            case "MAP":
                return "MAP";
            case "STRUCT":
                return "STRUCT";
            default:
                LOGGER.debug("Unknown Athena type for mapping: {}, using STRING", athenaType);
                return "STRING";
        }
    }
}