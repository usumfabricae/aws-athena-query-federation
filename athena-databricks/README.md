# Amazon Athena Databricks Connector

This connector enables Amazon Athena to execute federated queries against Databricks clusters using the Databricks JDBC driver. It extends the AWS Athena Query Federation framework to provide seamless integration with Databricks data sources.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Usage](#usage)
- [Performance Optimization](#performance-optimization)
- [Troubleshooting](#troubleshooting)
- [Security](#security)
- [License](#license)

## Features

- **Federated Queries**: Query Databricks tables and views directly from Amazon Athena
- **Predicate Pushdown**: Automatically push WHERE clauses and filters to Databricks for optimal performance
- **Column Projection**: Only select required columns to minimize data transfer
- **Partition Support**: Leverage both Hive-style and Delta Lake partitioning for query optimization
- **Token Authentication**: Secure token-based authentication with Databricks clusters
- **AWS Integration**: Native integration with AWS Secrets Manager for credential management
- **Multi-Cluster Support**: Support for connecting to multiple Databricks clusters through multiplexing handlers
- **Data Type Support**: Comprehensive mapping of Databricks data types to Athena-compatible formats

## Prerequisites

Before installing the Databricks connector, ensure you have:

1. **AWS Account** with appropriate permissions for:
   - AWS Lambda
   - Amazon Athena
   - AWS Secrets Manager (if using for credential storage)
   - Amazon S3 (for spill bucket)

2. **Databricks Workspace** with:
   - Running cluster or SQL warehouse
   - Personal access token or service principal credentials
   - Network connectivity from AWS Lambda to Databricks (consider VPC configuration for private clusters)

3. **Development Environment**:
   - Java 17 or higher (required for Lambda runtime)
   - Apache Maven 3.6 or higher
   - AWS CLI configured with appropriate credentials

## Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/awslabs/aws-athena-query-federation.git
cd aws-athena-query-federation/athena-databricks
```

### Step 2: Build the Connector

The connector uses a Lambda layer architecture to overcome AWS Lambda's 250MB deployment package size limit:

```powershell
# Build the connector (excludes JDBC driver)
.\build-package.ps1
```

This creates a smaller JAR file in the `target` directory. The Databricks JDBC driver is deployed separately as a Lambda layer.

**Note**: The JDBC driver is deployed as a separate Lambda layer to stay within AWS Lambda size limits.

## Configuration

The connector supports multiple configuration methods for different deployment scenarios.

### Environment Variables

Configure the connector using Lambda environment variables:

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `DATABRICKS_HOST` | Yes | Databricks cluster hostname | `dbc-12345678-abcd.cloud.databricks.com` |
| `DATABRICKS_HTTP_PATH` | Yes | HTTP path for the cluster | `/sql/1.0/warehouses/abcd1234efgh5678` |
| `DATABRICKS_TOKEN` | Yes* | Databricks access token | `dapi1234567890abcdef...` |
| `TEST_CATALOG` | No | Default catalog name for testing | `main` |
| `TEST_SCHEMA` | No | Default schema name for testing | `default` |
| `default` | No | Default connection string (alternative to individual vars) | `databricks://host:443/path` |
| `spill_bucket` | Yes | S3 bucket for large result sets | `my-athena-spill-bucket` |
| `spill_prefix` | No | S3 prefix for spill files | `athena-spill/` |

*Required unless using AWS Secrets Manager

### AWS Secrets Manager Configuration

For enhanced security, store Databricks credentials in AWS Secrets Manager:

1. **Create a Secret**:
```bash
aws secretsmanager create-secret \
  --name "databricks/prod/credentials" \
  --description "Databricks production credentials" \
  --secret-string '{
    "username": "token",
    "password": "dapi1234567890abcdef...",
    "server_hostname": "dbc-12345678-abcd.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/abcd1234efgh5678"
  }'
```

2. **Configure Environment Variables**:
```bash
databricks_secret_name=databricks/prod/credentials
```

### Connection String Examples

The connector supports various Databricks connection scenarios:

#### SQL Warehouse Connection
```
jdbc:databricks://dbc-12345678-abcd.cloud.databricks.com:443/default;
transportMode=http;
ssl=1;
httpPath=/sql/1.0/warehouses/abcd1234efgh5678;
AuthMech=3;
UID=token;
PWD=dapi1234567890abcdef...
```

#### All-Purpose Cluster Connection
```
jdbc:databricks://dbc-12345678-abcd.cloud.databricks.com:443/default;
transportMode=http;
ssl=1;
httpPath=/sql/protocolv1/o/1234567890123456/0123-456789-abcde012;
AuthMech=3;
UID=token;
PWD=dapi1234567890abcdef...
```

## Deployment

The connector uses a Lambda layer architecture for deployment. See [LAYER_DEPLOYMENT.md](LAYER_DEPLOYMENT.md) for detailed instructions.

### Quick Deployment

Deploy both the JDBC layer and connector function:

```powershell
.\deploy-connector.ps1 `
    -S3Bucket "your-deployment-bucket" `
    -LambdaFunctionName "athena-databricks-connector" `
    -SecretNamePrefix "AthenaDatabricks" `
    -SpillBucket "your-spill-bucket" `
    -Region "us-east-1"
```

### Architecture

The deployment creates:
1. **Lambda Layer**: Contains the Databricks JDBC driver (~100MB)
2. **Lambda Function**: Contains the connector logic (references the layer)

### Multi-Cluster Support

The connector supports multiple Databricks clusters through the multiplexing handler. Configure additional clusters using environment variables with prefixes:
- `prod_default` for production connection string
- `staging_default` for staging connection string  
- `dev_default` for development connection string

### Manual Deployment Steps

For step-by-step deployment:

1. **Deploy JDBC Layer**:
```powershell
.\deploy-layer.ps1 -S3Bucket "your-deployment-bucket" -Region "us-east-1"
```

2. **Build and Deploy Function**:
```powershell
.\deploy-connector.ps1 -SkipLayerDeployment -ExistingLayerArn "arn:aws:lambda:us-east-1:123456789012:layer:databricks-jdbc-driver:1"
```

## Usage

### Register the Connector

After deployment, register the connector with Athena:

```sql
-- Using AWS Console or CLI
CREATE EXTERNAL FUNCTION athena_databricks_connector
RETURNS varchar
LAMBDA 'athena-databricks'
```

### Create a Catalog

Create an Athena data catalog that uses the Databricks connector:

```sql
-- This is typically done through the AWS Console
-- Navigate to Athena > Data sources > Create data source
-- Select "Query a data source" and choose your Lambda function
```

### Query Databricks Data

Once configured, you can query Databricks tables using standard SQL:

```sql
-- List available schemas
SHOW SCHEMAS IN databricks_catalog;

-- List tables in a schema
SHOW TABLES IN databricks_catalog.default;

-- Query a table
SELECT * FROM databricks_catalog.default.my_table
WHERE date_column >= '2024-01-01'
LIMIT 100;

-- Join with other data sources
SELECT d.*, s3.additional_data
FROM databricks_catalog.default.my_table d
JOIN s3_catalog.my_bucket.reference_data s3
ON d.id = s3.id;
```

## Performance Optimization

### Predicate Pushdown

The connector automatically pushes supported predicates to Databricks:

```sql
-- These filters will be pushed to Databricks
SELECT * FROM databricks_catalog.default.sales
WHERE sale_date >= '2024-01-01'
  AND region = 'US'
  AND amount > 1000;
```

**Supported Predicates**:
- Comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
- Logical operators: `AND`, `OR`, `NOT`
- Pattern matching: `LIKE`, `NOT LIKE`
- Range queries: `BETWEEN`, `IN`, `NOT IN`
- Null checks: `IS NULL`, `IS NOT NULL`

### Partition Pruning

Leverage Databricks partitioning for optimal performance:

```sql
-- Query partitioned table with partition filter
SELECT * FROM databricks_catalog.default.partitioned_sales
WHERE year = 2024 AND month = 1;
```

### Column Projection

Only select required columns to minimize data transfer:

```sql
-- Good: Only select needed columns
SELECT customer_id, sale_amount, sale_date
FROM databricks_catalog.default.sales;

-- Avoid: Selecting all columns when not needed
SELECT * FROM databricks_catalog.default.sales;
```

### Best Practices

1. **Use Partition Filters**: Always include partition columns in WHERE clauses
2. **Limit Result Sets**: Use LIMIT clauses for exploratory queries
3. **Optimize Data Types**: Use appropriate data types in Databricks tables
4. **Index Usage**: Leverage Databricks Delta Lake Z-ordering for better performance
5. **Connection Pooling**: The connector automatically manages connection pooling

## Troubleshooting

### Common Issues and Solutions

#### Connection Timeout
**Symptom**: Lambda function times out when connecting to Databricks
**Solutions**:
- Increase Lambda timeout (up to 15 minutes)
- Check network connectivity between Lambda and Databricks
- Verify Databricks cluster is running and accessible
- Consider VPC configuration for private clusters

```bash
# Check Lambda timeout
aws lambda get-function-configuration --function-name athena-databricks

# Update timeout
aws lambda update-function-configuration \
  --function-name athena-databricks \
  --timeout 900
```

#### Authentication Errors
**Symptom**: "Invalid credentials" or "Authentication failed" errors
**Solutions**:
- Verify Databricks token is valid and not expired
- Check AWS Secrets Manager permissions
- Ensure token has appropriate permissions in Databricks

```bash
# Test token validity
curl -H "Authorization: Bearer dapi1234567890abcdef..." \
  https://dbc-12345678-abcd.cloud.databricks.com/api/2.0/clusters/list
```

#### Memory Issues
**Symptom**: Lambda function runs out of memory
**Solutions**:
- Increase Lambda memory allocation (minimum 1024MB recommended)
- Configure spill bucket for large result sets
- Use column projection to reduce data transfer

```bash
# Update Lambda memory
aws lambda update-function-configuration \
  --function-name athena-databricks \
  --memory-size 2048
```

#### Network Connectivity
**Symptom**: Cannot reach Databricks cluster from Lambda
**Solutions**:
- Configure Lambda VPC settings if Databricks is in private network
- Check security groups and NACLs
- Verify DNS resolution

#### Data Type Conversion Errors
**Symptom**: Errors when querying certain columns
**Solutions**:
- Check Databricks table schema for unsupported data types
- Review connector logs for specific conversion errors
- Consider casting problematic columns in queries

### Debugging Steps

1. **Check Lambda Logs**:
```bash
aws logs describe-log-groups --log-group-name-prefix /aws/lambda/athena-databricks
aws logs get-log-events --log-group-name /aws/lambda/athena-databricks --log-stream-name [STREAM_NAME]
```

2. **Test Connection**:
```sql
-- Simple connectivity test
SELECT 1 as test_connection FROM databricks_catalog.information_schema.tables LIMIT 1;
```

3. **Verify Configuration**:
```bash
aws lambda get-function-configuration --function-name athena-databricks
```

4. **Check Permissions**:
```bash
aws iam get-role --role-name [LAMBDA_EXECUTION_ROLE]
aws secretsmanager describe-secret --secret-id databricks/prod/credentials
```

### Performance Troubleshooting

#### Slow Query Performance
- Enable query pushdown logging to verify predicates are being pushed
- Check Databricks query history for actual executed queries
- Review table partitioning strategy
- Consider Databricks cluster configuration (size, type)

#### High Memory Usage
- Monitor Lambda memory usage in CloudWatch
- Configure appropriate spill bucket settings
- Use column projection to reduce data transfer
- Consider breaking large queries into smaller chunks

### Error Codes Reference

| Error Code | Description | Solution |
|------------|-------------|----------|
| `INVALID_CREDENTIALS_EXCEPTION` | Authentication failed | Check token validity and permissions |
| `CONNECTION_TIMEOUT` | Cannot connect to Databricks | Verify network connectivity and cluster status |
| `UNSUPPORTED_DATA_TYPE` | Data type conversion failed | Review table schema and consider casting |
| `MEMORY_LIMIT_EXCEEDED` | Lambda out of memory | Increase memory allocation or use spill bucket |
| `QUERY_TIMEOUT` | Query execution timeout | Optimize query or increase Lambda timeout |

## Security

### Authentication
- **Token-based Authentication**: Use Databricks personal access tokens or service principal tokens
- **AWS Secrets Manager**: Store credentials securely in AWS Secrets Manager
- **IAM Integration**: Leverage AWS IAM roles for Lambda execution permissions

### Network Security
- **VPC Configuration**: Deploy Lambda in VPC for private Databricks clusters
- **Security Groups**: Configure appropriate inbound/outbound rules
- **SSL/TLS**: All connections use encrypted HTTPS transport

### Data Protection
- **Encryption in Transit**: All data transfer uses SSL/TLS encryption
- **Encryption at Rest**: Leverage Databricks and S3 encryption features
- **Access Control**: Respect Databricks table and column-level permissions

### Best Practices
1. **Rotate Tokens**: Regularly rotate Databricks access tokens
2. **Least Privilege**: Grant minimum required permissions
3. **Monitor Access**: Enable CloudTrail logging for audit trails
4. **Secure Storage**: Never store credentials in plain text

## Dependencies

This connector depends on:
- **athena-federation-sdk**: Core federation framework (version 2022.47.1)
- **athena-jdbc**: Base JDBC framework for database connectors
- **DatabricksJDBC42.jar**: Databricks JDBC driver (included in lib folder)
- **AWS SDK**: For integration with AWS services

## Contributing

Contributions are welcome! Please see the main repository's CONTRIBUTING.md for guidelines.

## License

This project is licensed under the Apache License 2.0. See the LICENSE file for details.