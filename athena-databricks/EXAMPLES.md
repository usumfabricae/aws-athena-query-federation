# Databricks Connector Examples and Use Cases

This document provides comprehensive examples of using the Amazon Athena Databricks Connector, including sample federated queries, performance optimization techniques, and best practices.

## Table of Contents

- [Basic Query Examples](#basic-query-examples)
- [Federated Query Use Cases](#federated-query-use-cases)
- [Predicate Pushdown Examples](#predicate-pushdown-examples)
- [Performance Optimization](#performance-optimization)
- [Advanced Use Cases](#advanced-use-cases)
- [Best Practices](#best-practices)

## Basic Query Examples

### Simple Table Queries

```sql
-- List all schemas in Databricks catalog
SHOW SCHEMAS IN databricks_catalog;

-- List tables in a specific schema
SHOW TABLES IN databricks_catalog.sales_db;

-- Describe table structure
DESCRIBE databricks_catalog.sales_db.customer_orders;

-- Simple SELECT query
SELECT customer_id, order_date, total_amount
FROM databricks_catalog.sales_db.customer_orders
LIMIT 100;
```

### Data Type Examples

```sql
-- Query demonstrating various Databricks data types
SELECT 
    customer_id,                    -- BIGINT
    customer_name,                  -- STRING
    is_premium,                     -- BOOLEAN
    account_balance,                -- DECIMAL(10,2)
    last_login,                     -- TIMESTAMP
    signup_date,                    -- DATE
    preferences,                    -- MAP<STRING, STRING>
    order_history                   -- ARRAY<STRUCT<order_id:BIGINT, amount:DECIMAL>>
FROM databricks_catalog.crm.customers
WHERE signup_date >= DATE '2024-01-01';
```

## Federated Query Use Cases

### Cross-Platform Analytics

```sql
-- Join Databricks data with S3 data for comprehensive analysis
SELECT 
    db.customer_id,
    db.customer_name,
    db.total_orders,
    s3.marketing_segment,
    s3.lifetime_value_prediction
FROM databricks_catalog.sales_db.customer_summary db
JOIN s3_catalog.marketing_data.customer_segments s3
    ON db.customer_id = s3.customer_id
WHERE db.total_orders > 10
    AND s3.marketing_segment = 'high_value';
```

### Real-time and Historical Data Combination

```sql
-- Combine real-time Databricks data with historical S3 archives
WITH recent_sales AS (
    SELECT customer_id, SUM(amount) as recent_total
    FROM databricks_catalog.sales_db.orders
    WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY customer_id
),
historical_sales AS (
    SELECT customer_id, SUM(amount) as historical_total
    FROM s3_catalog.archives.historical_orders
    WHERE year BETWEEN 2020 AND 2023
    GROUP BY customer_id
)
SELECT 
    r.customer_id,
    r.recent_total,
    h.historical_total,
    (r.recent_total + h.historical_total) as total_lifetime_value
FROM recent_sales r
FULL OUTER JOIN historical_sales h
    ON r.customer_id = h.customer_id;
```

### Multi-Source Reporting

```sql
-- Create comprehensive report combining multiple data sources
SELECT 
    'Databricks' as source,
    COUNT(*) as record_count,
    SUM(revenue) as total_revenue,
    AVG(revenue) as avg_revenue
FROM databricks_catalog.sales_db.daily_sales
WHERE sale_date >= DATE '2024-01-01'

UNION ALL

SELECT 
    'S3 Archive' as source,
    COUNT(*) as record_count,
    SUM(revenue) as total_revenue,
    AVG(revenue) as avg_revenue
FROM s3_catalog.archives.historical_sales
WHERE year = 2023

UNION ALL

SELECT 
    'RDS' as source,
    COUNT(*) as record_count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_revenue
FROM rds_catalog.production.transactions
WHERE transaction_date >= DATE '2024-01-01';
```

## Predicate Pushdown Examples

### Supported Predicate Types

```sql
-- Comparison operators (pushed to Databricks)
SELECT * FROM databricks_catalog.sales_db.orders
WHERE order_amount > 1000
    AND customer_tier = 'premium'
    AND order_date >= DATE '2024-01-01';

-- Range queries (pushed to Databricks)
SELECT * FROM databricks_catalog.inventory.products
WHERE price BETWEEN 50 AND 500
    AND category IN ('electronics', 'appliances')
    AND stock_quantity > 0;

-- Pattern matching (pushed to Databricks)
SELECT * FROM databricks_catalog.crm.customers
WHERE customer_name LIKE 'John%'
    AND email NOT LIKE '%@temp%'
    AND phone IS NOT NULL;
```

### Partition Pruning Examples

```sql
-- Leverage date partitioning for optimal performance
SELECT customer_id, SUM(order_amount) as total_spent
FROM databricks_catalog.sales_db.orders
WHERE year = 2024                    -- Partition filter (pushed)
    AND month = 1                    -- Partition filter (pushed)
    AND order_status = 'completed'   -- Regular filter (pushed)
GROUP BY customer_id
HAVING SUM(order_amount) > 5000;     -- Post-aggregation filter
```

### Complex Predicate Examples

```sql
-- Complex expressions with functions (some pushed, some local)
SELECT 
    customer_id,
    order_date,
    order_amount,
    CASE 
        WHEN order_amount > 1000 THEN 'high_value'
        WHEN order_amount > 500 THEN 'medium_value'
        ELSE 'low_value'
    END as order_category
FROM databricks_catalog.sales_db.orders
WHERE EXTRACT(YEAR FROM order_date) = 2024     -- Pushed to Databricks
    AND EXTRACT(MONTH FROM order_date) IN (1,2,3)  -- Pushed to Databricks
    AND order_amount * 1.1 > 550;              -- Expression pushed to Databricks
```

### Predicate Pushdown Verification

```sql
-- Use EXPLAIN to verify predicate pushdown
EXPLAIN (FORMAT JSON)
SELECT * FROM databricks_catalog.sales_db.orders
WHERE order_date >= DATE '2024-01-01'
    AND customer_tier = 'premium';

-- Look for "pushedFilters" in the execution plan
-- Pushed predicates will appear in the Databricks scan operation
```

## Performance Optimization

### Column Projection Optimization

```sql
-- Good: Select only needed columns
SELECT customer_id, order_date, total_amount
FROM databricks_catalog.sales_db.large_orders_table
WHERE order_date >= DATE '2024-01-01';

-- Avoid: Selecting all columns from large tables
-- SELECT * FROM databricks_catalog.sales_db.large_orders_table;
```

### Efficient Aggregations

```sql
-- Push aggregations to Databricks when possible
SELECT 
    customer_tier,
    COUNT(*) as order_count,
    SUM(order_amount) as total_revenue,
    AVG(order_amount) as avg_order_value
FROM databricks_catalog.sales_db.orders
WHERE order_date >= DATE '2024-01-01'
GROUP BY customer_tier
ORDER BY total_revenue DESC;
```

### Partition-Aware Queries

```sql
-- Optimal: Use partition columns in WHERE clause
SELECT product_id, SUM(quantity_sold) as total_sold
FROM databricks_catalog.sales_db.daily_sales
WHERE year = 2024 AND month = 1 AND day >= 15  -- All partition columns
GROUP BY product_id;

-- Suboptimal: Missing partition filters
-- SELECT product_id, SUM(quantity_sold) as total_sold
-- FROM databricks_catalog.sales_db.daily_sales
-- WHERE sale_amount > 100  -- No partition filters
-- GROUP BY product_id;
```

### Limit Usage for Exploration

```sql
-- Use LIMIT for data exploration
SELECT * FROM databricks_catalog.sales_db.customer_orders
ORDER BY order_date DESC
LIMIT 1000;

-- Sample data for analysis
SELECT * FROM databricks_catalog.sales_db.large_table
TABLESAMPLE BERNOULLI(1)  -- 1% sample
WHERE order_date >= DATE '2024-01-01';
```

## Advanced Use Cases

### Window Functions

```sql
-- Window functions with Databricks data
SELECT 
    customer_id,
    order_date,
    order_amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) as order_rank,
    SUM(order_amount) OVER (PARTITION BY customer_id) as customer_total,
    LAG(order_amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as previous_order
FROM databricks_catalog.sales_db.orders
WHERE order_date >= DATE '2024-01-01'
QUALIFY order_rank <= 5;  -- Top 5 recent orders per customer
```

### Complex Joins with Multiple Sources

```sql
-- Three-way join across different data sources
SELECT 
    c.customer_id,
    c.customer_name,
    db_orders.total_orders,
    db_orders.total_spent,
    s3_support.ticket_count,
    rds_profile.loyalty_points
FROM databricks_catalog.crm.customers c
LEFT JOIN (
    SELECT 
        customer_id,
        COUNT(*) as total_orders,
        SUM(order_amount) as total_spent
    FROM databricks_catalog.sales_db.orders
    WHERE order_date >= DATE '2024-01-01'
    GROUP BY customer_id
) db_orders ON c.customer_id = db_orders.customer_id
LEFT JOIN (
    SELECT 
        customer_id,
        COUNT(*) as ticket_count
    FROM s3_catalog.support.tickets
    WHERE created_date >= DATE '2024-01-01'
    GROUP BY customer_id
) s3_support ON c.customer_id = s3_support.customer_id
LEFT JOIN rds_catalog.loyalty.customer_points rds_profile
    ON c.customer_id = rds_profile.customer_id;
```

### Time Series Analysis

```sql
-- Time series analysis with Databricks Delta tables
WITH daily_metrics AS (
    SELECT 
        DATE(order_timestamp) as order_date,
        COUNT(*) as daily_orders,
        SUM(order_amount) as daily_revenue,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM databricks_catalog.sales_db.orders
    WHERE order_timestamp >= TIMESTAMP '2024-01-01 00:00:00'
    GROUP BY DATE(order_timestamp)
),
moving_averages AS (
    SELECT 
        order_date,
        daily_orders,
        daily_revenue,
        unique_customers,
        AVG(daily_revenue) OVER (
            ORDER BY order_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as revenue_7day_avg,
        AVG(daily_orders) OVER (
            ORDER BY order_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as orders_30day_avg
    FROM daily_metrics
)
SELECT 
    order_date,
    daily_revenue,
    revenue_7day_avg,
    daily_orders,
    orders_30day_avg,
    CASE 
        WHEN daily_revenue > revenue_7day_avg * 1.2 THEN 'High'
        WHEN daily_revenue < revenue_7day_avg * 0.8 THEN 'Low'
        ELSE 'Normal'
    END as revenue_trend
FROM moving_averages
ORDER BY order_date DESC;
```

### Data Quality Checks

```sql
-- Cross-source data quality validation
WITH databricks_summary AS (
    SELECT 
        'databricks' as source,
        COUNT(*) as record_count,
        COUNT(DISTINCT customer_id) as unique_customers,
        MIN(order_date) as min_date,
        MAX(order_date) as max_date,
        SUM(CASE WHEN order_amount IS NULL THEN 1 ELSE 0 END) as null_amounts
    FROM databricks_catalog.sales_db.orders
    WHERE order_date >= DATE '2024-01-01'
),
s3_summary AS (
    SELECT 
        's3_archive' as source,
        COUNT(*) as record_count,
        COUNT(DISTINCT customer_id) as unique_customers,
        MIN(order_date) as min_date,
        MAX(order_date) as max_date,
        SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amounts
    FROM s3_catalog.archives.orders_2024
)
SELECT * FROM databricks_summary
UNION ALL
SELECT * FROM s3_summary;
```

## Best Practices

### Query Design Best Practices

1. **Always Use Partition Filters**
```sql
-- Good: Include partition columns in WHERE clause
SELECT * FROM databricks_catalog.sales_db.orders
WHERE year = 2024 AND month = 1;

-- Avoid: Queries without partition filters on large tables
-- SELECT * FROM databricks_catalog.sales_db.orders WHERE customer_id = 12345;
```

2. **Optimize Column Selection**
```sql
-- Good: Select only required columns
SELECT customer_id, order_amount, order_date
FROM databricks_catalog.sales_db.orders;

-- Avoid: SELECT * on wide tables
-- SELECT * FROM databricks_catalog.sales_db.orders;
```

3. **Use Appropriate Data Types**
```sql
-- Good: Use specific data types in comparisons
SELECT * FROM databricks_catalog.sales_db.orders
WHERE order_amount > DECIMAL '1000.00'
    AND order_date >= DATE '2024-01-01';

-- Avoid: String comparisons for numeric/date data
-- WHERE order_amount > '1000' AND order_date >= '2024-01-01'
```

### Performance Best Practices

1. **Leverage Databricks Optimizations**
   - Use Delta Lake tables with Z-ordering
   - Implement proper partitioning strategy
   - Enable auto-optimize and auto-compaction

2. **Query Optimization**
   - Use LIMIT for exploratory queries
   - Implement proper indexing in Databricks
   - Consider materialized views for complex aggregations

3. **Connection Management**
   - Configure appropriate Lambda timeout (up to 15 minutes)
   - Set optimal memory allocation (minimum 1024MB)
   - Use connection pooling effectively

### Security Best Practices

1. **Credential Management**
```sql
-- Use AWS Secrets Manager for credentials
-- Configure environment variables:
-- databricks_secret_name=databricks/prod/credentials
```

2. **Access Control**
   - Implement least-privilege access in Databricks
   - Use service principals for production deployments
   - Regularly rotate access tokens

3. **Network Security**
   - Deploy Lambda in VPC for private Databricks clusters
   - Configure appropriate security groups
   - Use private endpoints when available

### Monitoring and Troubleshooting

1. **Query Performance Monitoring**
```sql
-- Monitor query execution with EXPLAIN
EXPLAIN (FORMAT JSON)
SELECT customer_id, SUM(order_amount)
FROM databricks_catalog.sales_db.orders
WHERE order_date >= DATE '2024-01-01'
GROUP BY customer_id;
```

2. **Error Handling**
   - Monitor Lambda CloudWatch logs
   - Set up CloudWatch alarms for failures
   - Implement proper retry logic

3. **Cost Optimization**
   - Use appropriate Databricks cluster sizing
   - Implement query result caching
   - Monitor data transfer costs

### Development Workflow

1. **Testing Strategy**
   - Start with small data samples
   - Test predicate pushdown effectiveness
   - Validate data type conversions

2. **Deployment Process**
   - Use CloudFormation for consistent deployments
   - Implement proper CI/CD pipelines
   - Test in staging environment first

3. **Documentation**
   - Document connection configurations
   - Maintain query examples and use cases
   - Keep troubleshooting guides updated

## Common Patterns and Anti-Patterns

### Recommended Patterns

```sql
-- Pattern: Efficient federated aggregation
SELECT 
    db.region,
    COUNT(db.customer_id) as databricks_customers,
    COUNT(s3.customer_id) as s3_customers,
    SUM(db.total_orders) as total_databricks_orders
FROM (
    SELECT region, customer_id, COUNT(*) as total_orders
    FROM databricks_catalog.sales_db.orders
    WHERE order_date >= DATE '2024-01-01'
    GROUP BY region, customer_id
) db
FULL OUTER JOIN (
    SELECT region, customer_id
    FROM s3_catalog.customer_data.profiles
) s3 ON db.customer_id = s3.customer_id
GROUP BY db.region;
```

### Anti-Patterns to Avoid

```sql
-- Anti-pattern: Cartesian joins
-- SELECT * FROM databricks_catalog.sales_db.orders o, s3_catalog.products.catalog p;

-- Anti-pattern: No filters on large tables
-- SELECT * FROM databricks_catalog.sales_db.large_fact_table;

-- Anti-pattern: Complex calculations without pushdown
-- SELECT * FROM databricks_catalog.sales_db.orders
-- WHERE UPPER(SUBSTRING(customer_name, 1, 3)) = 'JOH';
```

This comprehensive examples document provides practical guidance for using the Databricks connector effectively, with focus on performance optimization and best practices for federated queries.