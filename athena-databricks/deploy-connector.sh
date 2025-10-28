#!/bin/bash

# Deploy Databricks Athena Connector with Lambda Layer
# This script builds, uploads, and deploys both the JDBC layer and connector function

set -e

# Default values
DEFAULT_CONNECTION_STRING="databricks://default"
SPILL_PREFIX="athena-spill"
S3_KEY="athena-databricks-2022.47.1.jar"
LAYER_NAME="databricks-jdbc-driver"
STACK_NAME="athena-databricks-connector-with-layer"
REGION="us-east-1"
LAMBDA_TIMEOUT=900
LAMBDA_MEMORY=3008
SECURITY_GROUP_IDS=""
SUBNET_IDS=""
SKIP_LAYER_DEPLOYMENT=false
EXISTING_LAYER_ARN=""

# Function to display usage
usage() {
    echo "Usage: $0 -b S3_BUCKET -f LAMBDA_FUNCTION_NAME -s SECRET_NAME_PREFIX -p SPILL_BUCKET [OPTIONS]"
    echo ""
    echo "Required:"
    echo "  -b S3_BUCKET              S3 bucket for storing deployment artifacts"
    echo "  -f LAMBDA_FUNCTION_NAME   Name for the Lambda function"
    echo "  -s SECRET_NAME_PREFIX     Prefix for Secrets Manager secrets"
    echo "  -p SPILL_BUCKET           S3 bucket for query result spilling"
    echo ""
    echo "Optional:"
    echo "  -c DEFAULT_CONNECTION     Default connection string (default: databricks://default)"
    echo "  -P SPILL_PREFIX           S3 prefix for spill files (default: athena-spill)"
    echo "  -k S3_KEY                 S3 key for JAR file (default: athena-databricks-2022.47.1.jar)"
    echo "  -l LAYER_NAME             Lambda layer name (default: databricks-jdbc-driver)"
    echo "  -n STACK_NAME             CloudFormation stack name"
    echo "  -r REGION                 AWS region (default: us-east-1)"
    echo "  -t LAMBDA_TIMEOUT         Lambda timeout in seconds (default: 900)"
    echo "  -m LAMBDA_MEMORY          Lambda memory in MB (default: 3008)"
    echo "  -g SECURITY_GROUP_IDS     VPC security groups (comma-separated)"
    echo "  -u SUBNET_IDS             VPC subnets (comma-separated)"
    echo "  -S                        Skip layer deployment"
    echo "  -L EXISTING_LAYER_ARN     Use existing layer ARN (requires -S)"
    echo "  -h                        Show this help message"
    exit 1
}

# Parse command line arguments
while getopts "b:f:s:p:c:P:k:l:n:r:t:m:g:u:SL:h" opt; do
    case $opt in
        b) S3_BUCKET="$OPTARG" ;;
        f) LAMBDA_FUNCTION_NAME="$OPTARG" ;;
        s) SECRET_NAME_PREFIX="$OPTARG" ;;
        p) SPILL_BUCKET="$OPTARG" ;;
        c) DEFAULT_CONNECTION_STRING="$OPTARG" ;;
        P) SPILL_PREFIX="$OPTARG" ;;
        k) S3_KEY="$OPTARG" ;;
        l) LAYER_NAME="$OPTARG" ;;
        n) STACK_NAME="$OPTARG" ;;
        r) REGION="$OPTARG" ;;
        t) LAMBDA_TIMEOUT="$OPTARG" ;;
        m) LAMBDA_MEMORY="$OPTARG" ;;
        g) SECURITY_GROUP_IDS="$OPTARG" ;;
        u) SUBNET_IDS="$OPTARG" ;;
        S) SKIP_LAYER_DEPLOYMENT=true ;;
        L) EXISTING_LAYER_ARN="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Check required parameters
if [ -z "$S3_BUCKET" ] || [ -z "$LAMBDA_FUNCTION_NAME" ] || [ -z "$SECRET_NAME_PREFIX" ] || [ -z "$SPILL_BUCKET" ]; then
    echo "Error: Missing required parameters"
    usage
fi

echo "=== Databricks Athena Connector Deployment (with Layer) ==="
echo "S3 Bucket: $S3_BUCKET"
echo "Lambda Function: $LAMBDA_FUNCTION_NAME"
echo "Layer Name: $LAYER_NAME"
echo "Stack Name: $STACK_NAME"
echo "Region: $REGION"
echo ""

LAYER_ARN="$EXISTING_LAYER_ARN"

# Step 1: Deploy Lambda Layer (if not skipped)
if [ "$SKIP_LAYER_DEPLOYMENT" = false ] && [ -z "$EXISTING_LAYER_ARN" ]; then
    echo "Step 1: Deploying Databricks JDBC Layer..."
    LAYER_OUTPUT=$(./deploy-layer.sh -b "$S3_BUCKET" -l "$LAYER_NAME" -r "$REGION")
    if [ $? -ne 0 ]; then
        echo "✗ Layer deployment failed"
        exit 1
    fi
    
    # Extract Layer ARN from output
    LAYER_ARN=$(echo "$LAYER_OUTPUT" | grep -o "arn:aws:lambda:.*:layer:.*:[0-9]*" | tail -1)
    if [ -z "$LAYER_ARN" ]; then
        echo "✗ Could not extract Layer ARN from deployment output"
        exit 1
    fi
    echo "✓ Layer deployed successfully: $LAYER_ARN"
elif [ -n "$EXISTING_LAYER_ARN" ]; then
    echo "Step 1: Using existing layer: $EXISTING_LAYER_ARN"
    LAYER_ARN="$EXISTING_LAYER_ARN"
else
    echo "Step 1: Skipping layer deployment (as requested)"
    echo "⚠ You must provide -L EXISTING_LAYER_ARN parameter when skipping layer deployment"
    exit 1
fi

# Step 2: Build the connector (without JDBC driver)
echo "Step 2: Building the connector (excluding JDBC driver)..."
./build-package.sh
if [ $? -ne 0 ]; then
    echo "✗ Build failed"
    exit 1
fi
echo "✓ Build completed successfully"

# Step 3: Upload connector JAR to S3
JAR_PATH="./target/athena-databricks-2022.47.1.jar"
if [ ! -f "$JAR_PATH" ]; then
    echo "✗ JAR file not found at $JAR_PATH"
    exit 1
fi

echo "Step 3: Uploading connector JAR to S3..."
aws s3 cp "$JAR_PATH" "s3://$S3_BUCKET/$S3_KEY" --region "$REGION"
if [ $? -ne 0 ]; then
    echo "✗ S3 upload failed"
    echo "Make sure the S3 bucket exists and you have upload permissions"
    exit 1
fi
echo "✓ JAR uploaded to s3://$S3_BUCKET/$S3_KEY"

# Step 4: Deploy with CloudFormation
echo "Step 4: Deploying with CloudFormation..."

# Build parameters
PARAMETERS="ParameterKey=LambdaFunctionName,ParameterValue=$LAMBDA_FUNCTION_NAME"
PARAMETERS="$PARAMETERS ParameterKey=DefaultConnectionString,ParameterValue=$DEFAULT_CONNECTION_STRING"
PARAMETERS="$PARAMETERS ParameterKey=SecretNamePrefix,ParameterValue=$SECRET_NAME_PREFIX"
PARAMETERS="$PARAMETERS ParameterKey=SpillBucket,ParameterValue=$SPILL_BUCKET"
PARAMETERS="$PARAMETERS ParameterKey=SpillPrefix,ParameterValue=$SPILL_PREFIX"
PARAMETERS="$PARAMETERS ParameterKey=CodeS3Bucket,ParameterValue=$S3_BUCKET"
PARAMETERS="$PARAMETERS ParameterKey=CodeS3Key,ParameterValue=$S3_KEY"
PARAMETERS="$PARAMETERS ParameterKey=DatabricksJdbcLayerArn,ParameterValue=$LAYER_ARN"
PARAMETERS="$PARAMETERS ParameterKey=LambdaTimeout,ParameterValue=$LAMBDA_TIMEOUT"
PARAMETERS="$PARAMETERS ParameterKey=LambdaMemory,ParameterValue=$LAMBDA_MEMORY"

# Add VPC parameters if provided
if [ -n "$SECURITY_GROUP_IDS" ]; then
    PARAMETERS="$PARAMETERS ParameterKey=SecurityGroupIds,ParameterValue=$SECURITY_GROUP_IDS"
fi
if [ -n "$SUBNET_IDS" ]; then
    PARAMETERS="$PARAMETERS ParameterKey=SubnetIds,ParameterValue=$SUBNET_IDS"
fi

# Check if stack exists
STACK_EXISTS=false
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" --query "Stacks[0].StackStatus" --output text >/dev/null 2>&1
if [ $? -eq 0 ]; then
    STACK_EXISTS=true
fi

if [ "$STACK_EXISTS" = true ]; then
    echo "Updating existing stack..."
    aws cloudformation update-stack \
        --stack-name "$STACK_NAME" \
        --template-body file://athena-databricks.yaml \
        --parameters $PARAMETERS \
        --capabilities CAPABILITY_IAM \
        --region "$REGION"
else
    echo "Creating new stack..."
    aws cloudformation create-stack \
        --stack-name "$STACK_NAME" \
        --template-body file://athena-databricks.yaml \
        --parameters $PARAMETERS \
        --capabilities CAPABILITY_IAM \
        --region "$REGION"
fi

if [ $? -ne 0 ]; then
    echo "✗ CloudFormation deployment failed"
    echo "Check the CloudFormation console for detailed error information"
    exit 1
fi

echo "✓ CloudFormation deployment initiated"
echo "Waiting for stack deployment to complete..."

# Wait for stack completion
aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME" --region "$REGION" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✓ Stack creation completed successfully"
else
    aws cloudformation wait stack-update-complete --stack-name "$STACK_NAME" --region "$REGION" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "✓ Stack update completed successfully"
    else
        echo "⚠ Stack deployment may still be in progress"
        echo "Check the CloudFormation console for status"
    fi
fi

echo ""
echo "=== Deployment Summary ==="
echo "✓ JDBC Layer deployed: $LAYER_ARN"
echo "✓ Connector built and packaged (without JDBC driver)"
echo "✓ JAR uploaded to S3: s3://$S3_BUCKET/$S3_KEY"
echo "✓ CloudFormation stack deployed: $STACK_NAME"
echo "✓ Lambda function created: $LAMBDA_FUNCTION_NAME"
echo ""
echo "Architecture:"
echo "- Lambda Layer: Contains Databricks JDBC driver"
echo "- Lambda Function: Contains connector logic (references layer)"
echo ""
echo "Next steps:"
echo "1. Configure your Databricks connection details in AWS Secrets Manager"
echo "2. Register the connector with Amazon Athena"
echo "3. Test the connector with sample queries"
echo ""
echo "For troubleshooting, check:"
echo "- CloudWatch logs: /aws/lambda/$LAMBDA_FUNCTION_NAME"
echo "- CloudFormation events in the AWS Console"