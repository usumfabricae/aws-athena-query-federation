#!/bin/bash

# Download Databricks JDBC Driver
# This script downloads the Databricks JDBC driver from the official repository

set -e

# Configuration
DRIVER_VERSION="2.6.36"
DRIVER_URL="https://repo1.maven.org/maven2/com/databricks/databricks-jdbc/${DRIVER_VERSION}/databricks-jdbc-${DRIVER_VERSION}.jar"
LIB_DIR="./lib"
DRIVER_PATH="$LIB_DIR/DatabricksJDBC42.jar"

echo "=== Downloading Databricks JDBC Driver ==="
echo "Version: $DRIVER_VERSION"
echo "Destination: $DRIVER_PATH"
echo ""

# Create lib directory if it doesn't exist
if [ ! -d "$LIB_DIR" ]; then
    echo "Creating lib directory..."
    mkdir -p "$LIB_DIR"
fi

# Check if driver already exists
if [ -f "$DRIVER_PATH" ]; then
    echo "Driver already exists at $DRIVER_PATH"
    EXISTING_SIZE=$(stat -f%z "$DRIVER_PATH" 2>/dev/null || stat -c%s "$DRIVER_PATH" 2>/dev/null)
    EXISTING_SIZE_MB=$(echo "scale=2; $EXISTING_SIZE / 1024 / 1024" | bc)
    echo "Existing file size: ${EXISTING_SIZE_MB} MB"
    
    read -p "Do you want to re-download? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Using existing driver"
        exit 0
    fi
fi

# Download the driver
echo "Downloading Databricks JDBC driver..."
if command -v curl &> /dev/null; then
    curl -L -o "$DRIVER_PATH" "$DRIVER_URL"
elif command -v wget &> /dev/null; then
    wget -O "$DRIVER_PATH" "$DRIVER_URL"
else
    echo "✗ Neither curl nor wget found. Please install one of them."
    exit 1
fi

if [ $? -ne 0 ]; then
    echo "✗ Download failed"
    exit 1
fi

# Verify download
if [ ! -f "$DRIVER_PATH" ]; then
    echo "✗ Driver file not found after download"
    exit 1
fi

DRIVER_SIZE=$(stat -f%z "$DRIVER_PATH" 2>/dev/null || stat -c%s "$DRIVER_PATH" 2>/dev/null)
DRIVER_SIZE_MB=$(echo "scale=2; $DRIVER_SIZE / 1024 / 1024" | bc)

echo "✓ Download completed successfully"
echo "✓ Driver saved to: $DRIVER_PATH"
echo "✓ File size: ${DRIVER_SIZE_MB} MB"

# Verify it's a valid JAR file
if command -v file &> /dev/null; then
    FILE_TYPE=$(file "$DRIVER_PATH")
    if echo "$FILE_TYPE" | grep -q -i "java\|jar\|zip"; then
        echo "✓ File appears to be a valid JAR archive"
    else
        echo "⚠ Warning: File may not be a valid JAR archive"
        echo "File type: $FILE_TYPE"
    fi
fi

echo ""
echo "The Databricks JDBC driver is now ready for use."
echo "You can proceed with building and deploying the connector."