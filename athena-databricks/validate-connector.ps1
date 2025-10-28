# Validate Databricks Athena Connector Functionality
# This script provides validation steps and sample queries for testing the connector

Write-Host "Databricks Athena Connector Validation Script" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host ""

# Check if deployment artifact exists
$ProjectDir = Get-Location
$TargetDir = Join-Path $ProjectDir "target"
$ArtifactName = "athena-databricks-2022.47.1"
$ShadedJar = Join-Path $TargetDir "$ArtifactName.jar"

if (Test-Path $ShadedJar) {
    $JarInfo = Get-Item $ShadedJar
    $JarSize = $JarInfo.Length
    $JarSizeMB = [math]::Round($JarSize / 1024 / 1024, 2)
    Write-Host "* Deployment artifact found: $ShadedJar" -ForegroundColor Green
    Write-Host "  Size: ${JarSizeMB}MB (${JarSize} bytes)" -ForegroundColor Cyan
} else {
    Write-Host "X Deployment artifact not found. Run build-package.ps1 first." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Validation Steps:" -ForegroundColor Yellow
Write-Host ""

Write-Host "1. CONNECTOR REGISTRATION WITH ATHENA" -ForegroundColor Cyan
Write-Host "   Prerequisites:" -ForegroundColor White
Write-Host "   - AWS CLI configured with appropriate permissions" -ForegroundColor Gray
Write-Host "   - S3 bucket for Lambda deployment (if JAR > 50MB)" -ForegroundColor Gray
Write-Host "   - IAM role with required permissions" -ForegroundColor Gray
Write-Host ""
Write-Host "   Commands to register connector:" -ForegroundColor White
Write-Host "   # Upload JAR to S3 (if needed)" -ForegroundColor Gray
Write-Host "   aws s3 cp $ShadedJar s3://your-bucket/athena-connectors/" -ForegroundColor Gray
Write-Host ""
Write-Host "   # Deploy using CloudFormation" -ForegroundColor Gray
Write-Host "   aws cloudformation deploy --template-file athena-databricks.yaml \\" -ForegroundColor Gray
Write-Host "     --stack-name athena-databricks-connector \\" -ForegroundColor Gray
Write-Host "     --parameter-overrides \\" -ForegroundColor Gray
Write-Host "       LambdaCodeS3Bucket=your-bucket \\" -ForegroundColor Gray
Write-Host "       LambdaCodeS3Key=athena-connectors/$ArtifactName.jar \\" -ForegroundColor Gray
Write-Host "       DatabricksServerHostname=your-workspace.cloud.databricks.com \\" -ForegroundColor Gray
Write-Host "       DatabricksHttpPath=/sql/1.0/warehouses/your-warehouse-id \\" -ForegroundColor Gray
Write-Host "       DatabricksToken=your-access-token \\" -ForegroundColor Gray
Write-Host "     --capabilities CAPABILITY_IAM" -ForegroundColor Gray
Write-Host ""

Write-Host "2. SAMPLE QUERIES FOR FUNCTIONALITY TESTING" -ForegroundColor Cyan
Write-Host ""

# Create sample queries file
$SampleQueriesPath = Join-Path $ProjectDir "sample-validation-queries.sql"
$SampleQueries = @"
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
"@

$SampleQueries | Out-File -FilePath $SampleQueriesPath -Encoding UTF8
Write-Host "   Sample validation queries created: $SampleQueriesPath" -ForegroundColor Green
Write-Host ""

Write-Host "3. ERROR HANDLING AND EDGE CASES" -ForegroundColor Cyan
Write-Host ""

# Create error handling test file
$ErrorTestsPath = Join-Path $ProjectDir "error-handling-tests.sql"
$ErrorTests = @"
-- Error Handling and Edge Cases Validation
-- These queries test error scenarios and edge cases

-- 1. Invalid Schema Test
-- Should return appropriate error message
SHOW TABLES IN "lambda:databricks-connector"."nonexistent_schema";

-- 2. Invalid Table Test
-- Should return appropriate error message
SELECT * FROM "lambda:databricks-connector"."default"."nonexistent_table";

-- 3. Invalid Column Test
-- Should return appropriate error message
SELECT nonexistent_column FROM "lambda:databricks-connector"."default"."your_table_name";

-- 4. Connection Timeout Test
-- Test with a very large table to potentially trigger timeout
SELECT count(*) FROM "lambda:databricks-connector"."default"."very_large_table";

-- 5. Data Type Conversion Edge Cases
-- Test edge cases for data type conversions
SELECT 
    CAST(string_column AS INTEGER) as converted_int,
    CAST(timestamp_column AS DATE) as converted_date,
    CAST(decimal_column AS VARCHAR) as converted_string
FROM "lambda:databricks-connector"."default"."your_table_name"
WHERE string_column IS NOT NULL
LIMIT 5;

-- 6. NULL Value Handling
-- Test NULL value handling across different data types
SELECT 
    string_column,
    int_column,
    date_column,
    boolean_column
FROM "lambda:databricks-connector"."default"."your_table_name"
WHERE string_column IS NULL OR int_column IS NULL
LIMIT 10;

-- 7. Large Result Set Test
-- Test handling of large result sets (may hit Lambda memory limits)
SELECT * FROM "lambda:databricks-connector"."default"."large_table" LIMIT 100000;

-- 8. Complex Data Types Test
-- Test handling of arrays, maps, and structs
SELECT 
    array_column[1] as first_array_element,
    map_column['key1'] as map_value,
    struct_column.field1 as struct_field
FROM "lambda:databricks-connector"."default"."complex_types_table"
LIMIT 5;
"@

$ErrorTests | Out-File -FilePath $ErrorTestsPath -Encoding UTF8
Write-Host "   Error handling test queries created: $ErrorTestsPath" -ForegroundColor Green
Write-Host ""

Write-Host "4. PERFORMANCE VALIDATION" -ForegroundColor Cyan
Write-Host "   - Monitor query execution times in Athena console" -ForegroundColor White
Write-Host "   - Check CloudWatch logs for Lambda function performance" -ForegroundColor White
Write-Host "   - Verify predicate pushdown is working (check Databricks query history)" -ForegroundColor White
Write-Host "   - Test with different data sizes and complexity" -ForegroundColor White
Write-Host ""

Write-Host "5. MONITORING AND LOGGING" -ForegroundColor Cyan
Write-Host "   CloudWatch Log Groups to monitor:" -ForegroundColor White
Write-Host "   - /aws/lambda/athena-databricks-connector" -ForegroundColor Gray
Write-Host "   - /aws/lambda/athena-databricks-connector-metadata" -ForegroundColor Gray
Write-Host "   - /aws/lambda/athena-databricks-connector-record" -ForegroundColor Gray
Write-Host ""
Write-Host "   Key metrics to watch:" -ForegroundColor White
Write-Host "   - Lambda duration and memory usage" -ForegroundColor Gray
Write-Host "   - Error rates and timeout occurrences" -ForegroundColor Gray
Write-Host "   - Databricks query execution times" -ForegroundColor Gray
Write-Host ""

Write-Host "6. VALIDATION CHECKLIST" -ForegroundColor Cyan
Write-Host ""
$ChecklistPath = Join-Path $ProjectDir "validation-checklist.md"
$Checklist = @"
# Databricks Athena Connector Validation Checklist

## Pre-Deployment Validation
- [ ] JAR artifact built successfully
- [ ] JAR size within Lambda limits (< 250MB)
- [ ] All required dependencies included
- [ ] CloudFormation templates validated
- [ ] IAM permissions configured correctly

## Deployment Validation
- [ ] Lambda function deployed successfully
- [ ] Function handler configured correctly
- [ ] Environment variables set properly
- [ ] VPC configuration (if required) working
- [ ] Secrets Manager integration (if used) functional

## Connectivity Validation
- [ ] Basic connectivity test passes
- [ ] Authentication with Databricks successful
- [ ] SSL/TLS connection established
- [ ] Network connectivity from Lambda to Databricks verified

## Metadata Operations
- [ ] Schema discovery works correctly
- [ ] Table listing functions properly
- [ ] Column metadata retrieval accurate
- [ ] Data type mapping correct
- [ ] Partition information detected (if applicable)

## Data Retrieval Operations
- [ ] Basic SELECT queries execute successfully
- [ ] Predicate pushdown working (WHERE clauses)
- [ ] Column projection working (SELECT specific columns)
- [ ] Aggregation functions working
- [ ] JOIN operations with other data sources
- [ ] Complex queries with subqueries and CTEs

## Data Type Support
- [ ] String/VARCHAR types
- [ ] Numeric types (INT, BIGINT, DOUBLE, DECIMAL)
- [ ] Date and timestamp types
- [ ] Boolean types
- [ ] Array types
- [ ] Map types
- [ ] Struct types
- [ ] NULL value handling

## Performance Validation
- [ ] Query execution times acceptable
- [ ] Lambda memory usage within limits
- [ ] Lambda timeout not exceeded
- [ ] Predicate pushdown reducing data transfer
- [ ] Partition pruning working (if applicable)

## Error Handling
- [ ] Invalid schema names handled gracefully
- [ ] Invalid table names handled gracefully
- [ ] Invalid column names handled gracefully
- [ ] Connection failures handled properly
- [ ] Timeout scenarios handled correctly
- [ ] Data type conversion errors handled

## Edge Cases
- [ ] Empty result sets handled
- [ ] Large result sets handled
- [ ] Special characters in identifiers
- [ ] Reserved words as identifiers
- [ ] Case sensitivity handling
- [ ] Unicode data handling

## Monitoring and Logging
- [ ] CloudWatch logs generated correctly
- [ ] Error messages are informative
- [ ] Performance metrics available
- [ ] Debugging information sufficient

## Security Validation
- [ ] Credentials not exposed in logs
- [ ] Network traffic encrypted
- [ ] IAM permissions follow least privilege
- [ ] Secrets Manager integration secure (if used)

## Documentation and Maintenance
- [ ] Deployment documentation complete
- [ ] Configuration examples provided
- [ ] Troubleshooting guide available
- [ ] Performance tuning recommendations documented
"@

$Checklist | Out-File -FilePath $ChecklistPath -Encoding UTF8
Write-Host "   Validation checklist created: $ChecklistPath" -ForegroundColor Green
Write-Host ""

Write-Host "VALIDATION COMPLETE!" -ForegroundColor Green
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "1. Deploy the connector using CloudFormation templates" -ForegroundColor White
Write-Host "2. Execute the sample queries in Athena" -ForegroundColor White
Write-Host "3. Work through the validation checklist" -ForegroundColor White
Write-Host "4. Monitor performance and error rates" -ForegroundColor White
Write-Host "5. Document any issues and resolutions" -ForegroundColor White
Write-Host ""
Write-Host "Files created for validation:" -ForegroundColor Cyan
Write-Host "- $SampleQueriesPath" -ForegroundColor Gray
Write-Host "- $ErrorTestsPath" -ForegroundColor Gray
Write-Host "- $ChecklistPath" -ForegroundColor Gray