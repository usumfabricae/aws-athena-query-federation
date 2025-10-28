# Databricks Athena Connector Deployment Guide

This guide provides step-by-step instructions for deploying the Databricks Athena Connector to AWS Lambda.

## Prerequisites

Before deploying, ensure you have:

1. **AWS CLI** configured with appropriate permissions
2. **PowerShell** (for Windows) or **Bash** (for Linux/Mac)
3. **Java 17+** and **Maven 3.6+** for building (Java 17 required for Lambda runtime)
4. **S3 bucket** for storing the Lambda deployment package
5. **S3 bucket** for Athena query spill data
6. **Databricks workspace** with access token

## Quick Deployment

### Option 1: Using the Deployment Script (Recommended)

1. **Build and Deploy in One Step**:
```powershell
.\deploy-connector.ps1 `
    -S3Bucket "your-deployment-bucket" `
    -LambdaFunctionName "athena-databricks" `
    -DefaultConnectionString "databricks://your-connection-string" `
    -SecretNamePrefix "AthenaDatabricksFederation" `
    -SpillBucket "your-athena-spill-bucket"
```

### Option 2: Manual Step-by-Step Deployment

#### Step 1: Build the Connector
```powershell
# Build the JAR file
.\build-package.ps1
```

#### Step 2: Upload to S3
```bash
# Upload the JAR to your S3 bucket
aws s3 cp ./target/athena-databricks-2022.47.1.jar s3://your-deployment-bucket/
```

#### Step 3: Deploy with CloudFormation
```bash
aws cloudformation create-stack \
  --stack-name athena-databricks-connector \
  --template-body file://athena-databricks.yaml \
  --parameters \
    ParameterKey=LambdaFunctionName,ParameterValue=athena-databricks \
    ParameterKey=DefaultConnectionString,ParameterValue="databricks://your-connection-string" \
    ParameterKey=SecretNamePrefix,ParameterValue=AthenaDatabricksFederation \
    ParameterKey=SpillBucket,ParameterValue=your-athena-spill-bucket \
    ParameterKey=CodeS3Bucket,ParameterValue=your-deployment-bucket \
    ParameterKey=CodeS3Key,ParameterValue=athena-databricks-2022.47.1.jar \
  --capabilities CAPABILITY_IAM
```

## Configuration Parameters

**Note**: Both single-cluster (`athena-databricks.yaml`) and multi-cluster (`athena-databricks-mux.yaml`) templates have been updated to support optional VPC configuration and proper S3-based deployment.

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `LambdaFunctionName` | Yes | Name for the Lambda function | `athena-databricks` |
| `DefaultConnectionString` | Yes | Default Databricks connection | `databricks://hostname:443/default` |
| `SecretNamePrefix` | Yes | Prefix for Secrets Manager secrets | `AthenaDatabricksFederation` |
| `SpillBucket` | Yes | S3 bucket for query spill data | `my-athena-spill-bucket` |
| `CodeS3Bucket` | Yes | S3 bucket containing the JAR file | `my-deployment-bucket` |
| `CodeS3Key` | No | S3 key for the JAR file | `athena-databricks-2022.47.1.jar` |
| `SecurityGroupIds` | No | VPC security groups (comma-separated) | `sg-12345,sg-67890` |
| `SubnetIds` | No | VPC subnets (comma-separated) | `subnet-12345,subnet-67890` |

## Post-Deployment Configuration

### 1. Configure Databricks Credentials

Create a secret in AWS Secrets Manager:

```bash
aws secretsmanager create-secret \
  --name "AthenaDatabricksFederation/default" \
  --description "Databricks credentials for Athena connector" \
  --secret-string '{
    "username": "token",
    "password": "dapi1234567890abcdef...",
    "server_hostname": "dbc-12345678-abcd.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/abcd1234efgh5678"
  }'
```

**Note**: The connector supports both AWS Secrets Manager and direct environment variables for configuration. When using environment variables, use uppercase names like `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, and `DATABRICKS_TOKEN`. You can also use a single `default` connection string environment variable as an alternative.

### 2. Register with Amazon Athena

1. Open the **Amazon Athena Console**
2. Go to **Data sources** → **Create data source**
3. Select **Query a data source**
4. Choose **Lambda** as the data source
5. Select your deployed Lambda function
6. Configure the catalog name (e.g., `databricks_catalog`)

### 3. Test the Connection

Run a test query in Athena:

```sql
-- List schemas
SHOW SCHEMAS IN databricks_catalog;

-- List tables
SHOW TABLES IN databricks_catalog.default;

-- Test query
SELECT * FROM databricks_catalog.default.your_table LIMIT 10;
```

## Troubleshooting

### Common Issues

#### 1. CloudFormation Deployment Fails
- **Error**: `CodeUri is not a valid S3 Uri`
- **Solution**: Ensure the JAR file is uploaded to S3 and the bucket/key parameters are correct

#### 2. Lambda Function Timeout
- **Error**: Lambda function times out during execution
- **Solutions**:
  - Increase Lambda timeout (up to 900 seconds)
  - Increase Lambda memory (minimum 1024MB recommended)
  - Check network connectivity to Databricks

#### 3. Authentication Errors
- **Error**: "Invalid credentials" or "Authentication failed"
- **Solutions**:
  - Verify Databricks token is valid and not expired
  - Check AWS Secrets Manager permissions
  - Ensure secret name matches the configured prefix

#### 4. Network Connectivity Issues
- **Error**: Cannot connect to Databricks cluster
- **Solutions**:
  - Configure VPC settings if Databricks is in a private network
  - Check security groups and NACLs
  - Verify DNS resolution

### Checking Logs

Monitor Lambda execution logs:

```bash
# View recent logs
aws logs describe-log-groups --log-group-name-prefix /aws/lambda/athena-databricks

# Get log events
aws logs get-log-events \
  --log-group-name /aws/lambda/athena-databricks \
  --log-stream-name [STREAM_NAME]
```

### Updating the Connector

To update an existing deployment:

1. Build the new version
2. Upload to S3 (optionally with a new key)
3. Update the CloudFormation stack:

```bash
aws cloudformation update-stack \
  --stack-name athena-databricks-connector \
  --template-body file://athena-databricks.yaml \
  --parameters [same parameters as create] \
  --capabilities CAPABILITY_IAM
```

## Performance Optimization

### Lambda Configuration
- **Memory**: Start with 1024MB, increase if needed
- **Timeout**: Set to 900 seconds for complex queries
- **VPC**: Only use VPC if Databricks requires private connectivity

### Query Optimization
- Use partition filters in WHERE clauses
- Select only required columns
- Leverage Databricks query optimization features

### Monitoring
- Set up CloudWatch alarms for Lambda errors and duration
- Monitor Databricks cluster performance
- Track query execution patterns in Athena

## Security Best Practices

1. **Use AWS Secrets Manager** for all credentials
2. **Rotate tokens regularly** in Databricks
3. **Apply least privilege** IAM policies
4. **Enable VPC** for private network access
5. **Monitor access logs** in CloudTrail

## Support

For issues and questions:
- Check the [troubleshooting section](README.md#troubleshooting) in the README
- Review CloudWatch logs for detailed error information
- Consult the [AWS Athena Query Federation documentation](https://github.com/awslabs/aws-athena-query-federation)