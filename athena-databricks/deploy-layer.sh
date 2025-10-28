#!/bin/bash

# Deploy Databricks JDBC Driver as Lambda Layer
# This script creates a Lambda layer containing the Databricks JDBC driver

set -e

# Default values
LAYER_NAME="databricks-jdbc-driver"
REGION="us-east-1"
S3_KEY_PREFIX="layers/databricks-jdbc"
DESCRIPTION="Databricks JDBC Driver for Athena Federation"

# Function to display usage
usage() {
    echo "Usage: $0 -b S3_BUCKET [-l LAYER_NAME] [-r REGION] [-p S3_KEY_PREFIX] [-d DESCRIPTION]"
    echo ""
    echo "Required:"
    echo "  -b S3_BUCKET        S3 bucket for storing layer artifacts"
    echo ""
    echo "Optional:"
    echo "  -l LAYER_NAME       Layer name (default: databricks-jdbc-driver)"
    echo "  -r REGION           AWS region (default: us-east-1)"
    echo "  -p S3_KEY_PREFIX    S3 key prefix (default: layers/databricks-jdbc)"
    echo "  -d DESCRIPTION      Layer description"
    echo "  -h                  Show this help message"
    exit 1
}

# Parse command line arguments
while getopts "b:l:r:p:d:h" opt; do
    case $opt in
        b) S3_BUCKET="$OPTARG" ;;
        l) LAYER_NAME="$OPTARG" ;;
        r) REGION="$OPTARG" ;;
        p) S3_KEY_PREFIX="$OPTARG" ;;
        d) DESCRIPTION="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Check required parameters
if [ -z "$S3_BUCKET" ]; then
    echo "Error: S3_BUCKET is required"
    usage
fi

echo "=== Databricks JDBC Driver Layer Deployment ==="
echo "Layer Name: $LAYER_NAME"
echo "S3 Bucket: $S3_BUCKET"
echo "Region: $REGION"
echo ""

# Step 1: Create layer directory structure
LAYER_DIR="./layer-build"
JAVA_LIB_DIR="$LAYER_DIR/java/lib"

echo "Step 1: Preparing layer directory..."
if [ -d "$LAYER_DIR" ]; then
    rm -rf "$LAYER_DIR"
fi
mkdir -p "$JAVA_LIB_DIR"

# Step 2: Download Databricks JDBC driver if not present
DRIVER_PATH="./lib/DatabricksJDBC42.jar"
if [ ! -f "$DRIVER_PATH" ]; then
    echo "Step 2: Downloading Databricks JDBC driver..."
    if [ -f "./download-databricks-driver.sh" ]; then
        ./download-databricks-driver.sh
        if [ $? -ne 0 ]; then
            echo "✗ Failed to download Databricks driver"
            exit 1
        fi
        echo "✓ Databricks driver downloaded"
    else
        echo "✗ Databricks driver not found and download script missing"
        echo "Please ensure DatabricksJDBC42.jar is available in ./lib/"
        exit 1
    fi
else
    echo "Step 2: Using existing Databricks JDBC driver"
fi

# Step 3: Copy driver to layer structure
echo "Step 3: Copying driver to layer structure..."
cp "$DRIVER_PATH" "$JAVA_LIB_DIR/"
if [ $? -ne 0 ]; then
    echo "✗ Failed to copy driver"
    exit 1
fi
echo "✓ Driver copied to layer structure"

# Step 4: Create layer ZIP package
LAYER_ZIP="databricks-jdbc-layer.zip"
echo "Step 4: Creating layer ZIP package..."
if [ -f "$LAYER_ZIP" ]; then
    rm "$LAYER_ZIP"
fi

cd "$LAYER_DIR"
zip -r "../$LAYER_ZIP" . > /dev/null
cd ..

if [ $? -ne 0 ]; then
    echo "✗ Failed to create ZIP"
    exit 1
fi

ZIP_SIZE=$(stat -f%z "$LAYER_ZIP" 2>/dev/null || stat -c%s "$LAYER_ZIP" 2>/dev/null)
ZIP_SIZE_MB=$(echo "scale=2; $ZIP_SIZE / 1024 / 1024" | bc)
echo "✓ Layer ZIP created: $LAYER_ZIP (${ZIP_SIZE_MB} MB)"

# Check size limits
if [ "$ZIP_SIZE" -gt 52428800 ]; then  # 50MB limit for direct upload
    echo "⚠ Layer size exceeds 50MB - will use S3 upload"
fi

# Step 5: Upload layer to S3
S3_KEY="$S3_KEY_PREFIX/databricks-jdbc-layer.zip"
echo "Step 5: Uploading layer to S3..."
aws s3 cp "$LAYER_ZIP" "s3://$S3_BUCKET/$S3_KEY" --region "$REGION"
if [ $? -ne 0 ]; then
    echo "✗ S3 upload failed"
    exit 1
fi
echo "✓ Layer uploaded to s3://$S3_BUCKET/$S3_KEY"

# Step 6: Publish Lambda layer
echo "Step 6: Publishing Lambda layer..."
LAYER_OUTPUT=$(aws lambda publish-layer-version \
    --layer-name "$LAYER_NAME" \
    --description "$DESCRIPTION" \
    --content "S3Bucket=$S3_BUCKET,S3Key=$S3_KEY" \
    --compatible-runtimes java17 java11 java8.al2 \
    --region "$REGION" \
    --output json)

if [ $? -ne 0 ]; then
    echo "✗ Layer publication failed"
    exit 1
fi

LAYER_ARN=$(echo "$LAYER_OUTPUT" | jq -r '.LayerArn')
LAYER_VERSION=$(echo "$LAYER_OUTPUT" | jq -r '.Version')

echo "✓ Layer published successfully"
echo "  Layer ARN: $LAYER_ARN"
echo "  Version: $LAYER_VERSION"

# Step 7: Clean up temporary files
echo "Step 7: Cleaning up..."
rm -rf "$LAYER_DIR"
rm "$LAYER_ZIP"
echo "✓ Cleanup completed"

echo ""
echo "=== Layer Deployment Summary ==="
echo "✓ Layer Name: $LAYER_NAME"
echo "✓ Layer ARN: $LAYER_ARN"
echo "✓ Version: $LAYER_VERSION"
echo "✓ Compatible Runtimes: java17, java11, java8.al2"
echo ""
echo "Next steps:"
echo "1. Update your CloudFormation template to reference this layer"
echo "2. Rebuild your Lambda function without the JDBC driver"
echo "3. Deploy the updated Lambda function"
echo ""
echo "Layer ARN to use in CloudFormation:"
echo "$LAYER_ARN"