-- Databricks Athena Connector Validation Queries
-- Execute these queries in Amazon Athena to validate connector functionality

-- 1. Basic Connectivity Test
-- This query tests basic connection to Databricks
SELECT 1 as connectivity_test;

-- 2. Schema Discovery Test
-- List all databases/schemas available through the connector
SHOW SCHEMAS IN "lambda:databricks-connector";

-- 3. Table Discovery Test
-- List tables in a specific schema (replace 'default' with your schema)
SHOW TABLES IN "lambda:databricks-connector"."default";

-- 4. Basic Data Retrieval Test
-- Select data from a table (replace with your actual table name)
SELECT * FROM "lambda:databricks-connector"."default"."your_table_name" LIMIT 10;

-- 5. Column Metadata Test
-- Describe table structure
DESCRIBE "lambda:databricks-connector"."default"."your_table_name";

-- 6. Predicate Pushdown Test
-- Test WHERE clause pushdown (should be executed on Databricks side)
SELECT count(*) FROM "lambda:databricks-connector"."default"."your_table_name" 
WHERE your_date_column >= date('2023-01-01');

-- 7. Column Projection Test
-- Test that only required columns are selected
SELECT column1, column2 FROM "lambda:databricks-connector"."default"."your_table_name" LIMIT 5;

-- 8. Aggregation Test
-- Test aggregation functions
SELECT 
    count(*) as total_rows,
    count(DISTINCT your_column) as distinct_values,
    min(your_numeric_column) as min_value,
    max(your_numeric_column) as max_value,
    avg(your_numeric_column) as avg_value
FROM "lambda:databricks-connector"."default"."your_table_name";

-- 9. Join Test (if you have multiple tables)
-- Test joining tables from Databricks with other data sources
SELECT d.*, s3.* 
FROM "lambda:databricks-connector"."default"."databricks_table" d
JOIN "s3-data-source"."database"."s3_table" s3 
ON d.join_key = s3.join_key
LIMIT 10;

-- 10. Data Type Compatibility Test
-- Test various Databricks data types
SELECT 
    string_column,
    int_column,
    bigint_column,
    double_column,
    boolean_column,
    date_column,
    timestamp_column,
    decimal_column,
    array_column,
    map_column,
    struct_column
FROM "lambda:databricks-connector"."default"."data_types_test_table" 
LIMIT 5;

-- 11. Partition Pruning Test (if table is partitioned)
-- Test that partition filters are pushed down
SELECT * FROM "lambda:databricks-connector"."default"."partitioned_table" 
WHERE partition_column = 'specific_value'
LIMIT 10;

-- 12. Complex Query Test
-- Test complex query with multiple operations
WITH databricks_summary AS (
    SELECT 
        category,
        count(*) as record_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount
    FROM "lambda:databricks-connector"."default"."sales_table"
    WHERE date_column >= date('2023-01-01')
    GROUP BY category
)
SELECT 
    category,
    record_count,
    total_amount,
    avg_amount,
    total_amount / sum(total_amount) OVER () * 100 as percentage_of_total
FROM databricks_summary
ORDER BY total_amount DESC;
