#!/bin/bash

# Build Databricks Athena Connector (Layer Version)
# This script builds the connector JAR excluding the Databricks JDBC driver
# The JDBC driver should be deployed as a separate Lambda layer

set -e

# Default values
SKIP_TESTS=false
CLEAN=true

# Function to display usage
usage() {
    echo "Usage: $0 [-s] [-n] [-h]"
    echo ""
    echo "Options:"
    echo "  -s    Skip tests during build"
    echo "  -n    Skip clean (no clean)"
    echo "  -h    Show this help message"
    exit 1
}

# Parse command line arguments
while getopts "snh" opt; do
    case $opt in
        s) SKIP_TESTS=true ;;
        n) CLEAN=false ;;
        h) usage ;;
        *) usage ;;
    esac
done

echo "=== Building Databricks Athena Connector (Layer Version) ==="
echo "This build excludes the Databricks JDBC driver from the JAR"
echo "The driver should be deployed as a separate Lambda layer"
echo ""

# Check if Maven is available
if ! command -v mvn &> /dev/null; then
    echo "✗ Maven is required but not found in PATH"
    echo "Please install Maven and ensure it's in your PATH"
    exit 1
fi
echo "✓ Maven found"

# Step 1: Clean previous builds
if [ "$CLEAN" = true ]; then
    echo "Step 1: Cleaning previous builds..."
    mvn clean -q
    if [ $? -ne 0 ]; then
        echo "✗ Clean failed"
        exit 1
    fi
    echo "✓ Clean completed"
else
    echo "Step 1: Skipping clean (as requested)"
fi

# Step 2: Download Databricks driver (for compilation, but exclude from JAR)
echo "Step 2: Ensuring Databricks JDBC driver is available..."
DRIVER_PATH="./lib/DatabricksJDBC42.jar"
if [ ! -f "$DRIVER_PATH" ]; then
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
    echo "✓ Databricks driver already available"
fi

# Step 3: Build with Maven (excluding JDBC driver from final JAR)
if [ "$SKIP_TESTS" = true ]; then
    MAVEN_GOALS="compile package -DskipTests"
    echo "Step 3: Building connector (skipping tests)..."
else
    MAVEN_GOALS="compile package"
    echo "Step 3: Building connector (including tests)..."
fi

# The JDBC driver has 'provided' scope, so it won't be included in the JAR
mvn $MAVEN_GOALS -q
if [ $? -ne 0 ]; then
    echo "✗ Build failed"
    echo "Try running with verbose output: mvn $MAVEN_GOALS"
    exit 1
fi
echo "✓ Build completed successfully"

# Step 4: Verify build output
JAR_PATH="./target/athena-databricks-2022.47.1.jar"
if [ ! -f "$JAR_PATH" ]; then
    echo "✗ Expected JAR file not found at $JAR_PATH"
    echo "Check the Maven build output for errors"
    exit 1
fi

# Step 5: Check JAR size and contents
JAR_SIZE=$(stat -f%z "$JAR_PATH" 2>/dev/null || stat -c%s "$JAR_PATH" 2>/dev/null)
JAR_SIZE_MB=$(echo "scale=2; $JAR_SIZE / 1024 / 1024" | bc)
echo "✓ JAR created: $JAR_PATH (${JAR_SIZE_MB} MB)"

# Check if JDBC driver is excluded
if jar -tf "$JAR_PATH" | grep -q -i "DatabricksJDBC\|databricks.*jdbc"; then
    echo "⚠ WARNING: JDBC driver may still be included in JAR"
    echo "Verify the Maven configuration is correctly excluding the driver"
else
    echo "✓ JDBC driver successfully excluded from JAR"
fi

# Size validation
if [ "$JAR_SIZE" -gt 262144000 ]; then  # 250MB limit
    echo "✗ JAR size still exceeds AWS Lambda limit of 250MB"
    echo "Additional optimization may be required"
    exit 1
elif [ "$JAR_SIZE" -gt 52428800 ]; then  # 50MB limit for direct upload
    echo "⚠ JAR size exceeds 50MB - will require S3 upload for deployment"
else
    echo "✓ JAR size is within direct upload limits"
fi

echo ""
echo "=== Build Summary ==="
echo "✓ Connector built successfully"
echo "✓ JAR file: $JAR_PATH (${JAR_SIZE_MB} MB)"
echo "✓ JDBC driver excluded (deploy as separate layer)"
echo ""
echo "Next steps:"
echo "1. Deploy the Databricks JDBC driver as a Lambda layer using deploy-layer.sh"
echo "2. Update CloudFormation template to reference the layer"
echo "3. Deploy the Lambda function using the updated template"