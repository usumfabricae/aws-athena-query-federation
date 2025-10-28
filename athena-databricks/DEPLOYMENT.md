# Databricks Athena Connector - Lambda Layer Deployment

This guide explains how to deploy the Databricks Athena Connector using Lambda layers to overcome the 250MB deployment package size limit.

## Architecture

The deployment is split into two components:
1. **Lambda Layer**: Contains the Databricks JDBC driver (~100MB)
2. **Lambda Function**: Contains the connector logic (much smaller without the driver)

## Prerequisites

- AWS CLI configured with appropriate permissions
- PowerShell (Windows) or PowerShell Core (cross-platform)
- Maven 3.6+ 
- Java 17+
- S3 bucket for storing deployment artifacts

## Quick Start

### Option 1: Deploy Everything (Recommended)

Deploy both the layer and function in one command:

```powershell
.\deploy-connector.ps1 `
    -S3Bucket "your-deployment-bucket" `
    -LambdaFunctionName "athena-databricks-connector" `
    -SecretNamePrefix "AthenaDatabricks" `
    -SpillBucket "your-spill-bucket" `
    -Region "us-east-1"
```

### Option 2: Deploy Step by Step

#### Step 1: Deploy the JDBC Driver Layer

```powershell
.\deploy-layer.ps1 `
    -S3Bucket "your-deployment-bucket" `
    -LayerName "databricks-jdbc-driver" `
    -Region "us-east-1"
```

This will output a Layer ARN like: `arn:aws:lambda:us-east-1:123456789012:layer:databricks-jdbc-driver:1`

#### Step 2: Build the Connector (without JDBC driver)

```powershell
.\build-package.ps1
```

#### Step 3: Deploy the Connector Function

```powershell
.\deploy-connector.ps1 `
    -S3Bucket "your-deployment-bucket" `
    -LambdaFunctionName "athena-databricks-connector" `
    -SecretNamePrefix "AthenaDatabricks" `
    -SpillBucket "your-spill-bucket" `
    -Region "us-east-1" `
    -SkipLayerDeployment `
    -ExistingLayerArn "arn:aws:lambda:us-east-1:123456789012:layer:databricks-jdbc-driver:1"
```

## Parameters

### Required Parameters

- **S3Bucket**: S3 bucket for storing deployment artifacts
- **LambdaFunctionName**: Name for the Lambda function
- **SecretNamePrefix**: Prefix for Secrets Manager secrets containing Databricks credentials
- **SpillBucket**: S3 bucket for query result spilling

### Optional Parameters

- **Region**: AWS region (default: us-east-1)
- **LayerName**: Name for the Lambda layer (default: databricks-jdbc-driver)
- **StackName**: CloudFormation stack name (default: athena-databricks-connector-with-layer)
- **LambdaTimeout**: Function timeout in seconds (default: 900)
- **LambdaMemory**: Function memory in MB (default: 3008)
- **SecurityGroupIds**: VPC security groups (comma-separated)
- **SubnetIds**: VPC subnets (comma-separated)

## Files Created

This deployment approach creates these new files:

1. **deploy-layer.ps1** - Deploys the Databricks JDBC driver as a Lambda layer
2. **build-package.ps1** - Builds the connector JAR without the JDBC driver
3. **athena-databricks.yaml** - CloudFormation template that references the layer
4. **deploy-connector.ps1** - Complete deployment script for both layer and function

## Troubleshooting

### Layer Deployment Issues

- Ensure the S3 bucket exists and you have upload permissions
- Check that the Databricks JDBC driver is downloaded to `./lib/DatabricksJDBC42.jar`
- Verify AWS CLI is configured for the target region

### Function Deployment Issues

- Ensure the layer was deployed successfully and you have the correct ARN
- Check CloudFormation events in the AWS Console for detailed error messages
- Verify all required parameters are provided

### Size Validation

The scripts will warn you if:
- Layer size exceeds 50MB (will use S3 upload)
- Function JAR still exceeds limits (may need further optimization)

## Comparison with Original Deployment

| Aspect | Original (Single JAR) | Layer-based |
|--------|----------------------|-------------|
| JAR Size | ~150MB+ | ~50MB |
| Deployment Method | Direct upload or S3 | Layer + Function |
| Cold Start | Slightly faster | Slightly slower |
| Maintenance | Single artifact | Two artifacts |
| Reusability | None | Layer can be shared |

## Next Steps

After successful deployment:

1. Configure Databricks connection details in AWS Secrets Manager
2. Register the connector with Amazon Athena
3. Test with sample queries

For detailed configuration, see the main README.md file.