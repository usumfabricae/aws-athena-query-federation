# Deploy Databricks JDBC Driver as Lambda Layer
# This script creates a Lambda layer containing the Databricks JDBC driver

param(
    [Parameter(Mandatory=$true)]
    [string]$S3Bucket,
    
    [Parameter(Mandatory=$false)]
    [string]$LayerName = "databricks-jdbc-driver",
    
    [Parameter(Mandatory=$false)]
    [string]$Region = "us-east-1",
    
    [Parameter(Mandatory=$false)]
    [string]$S3KeyPrefix = "layers/databricks-jdbc",
    
    [Parameter(Mandatory=$false)]
    [string]$Description = "Databricks JDBC Driver for Athena Federation"
)

Write-Host "=== Databricks JDBC Driver Layer Deployment ===" -ForegroundColor Green
Write-Host "Layer Name: $LayerName" -ForegroundColor Cyan
Write-Host "S3 Bucket: $S3Bucket" -ForegroundColor Cyan
Write-Host "Region: $Region" -ForegroundColor Cyan
Write-Host ""

# Step 1: Create layer directory structure
$LayerDir = ".\layer-build"
$JavaLibDir = "$LayerDir\java\lib"

Write-Host "Step 1: Preparing layer directory..." -ForegroundColor Yellow
if (Test-Path $LayerDir) {
    Remove-Item -Recurse -Force $LayerDir
}
New-Item -ItemType Directory -Path $JavaLibDir -Force | Out-Null

# Step 2: Download Databricks JDBC driver if not present
$DriverPath = ".\lib\DatabricksJDBC42.jar"
if (!(Test-Path $DriverPath)) {
    Write-Host "Step 2: Downloading Databricks JDBC driver..." -ForegroundColor Yellow
    try {
        & .\download-databricks-driver.ps1
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to download Databricks driver"
        }
        Write-Host "SUCCESS: Databricks driver downloaded" -ForegroundColor Green
    }
    catch {
        Write-Host "ERROR: Failed to download driver: $_" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "Step 2: Using existing Databricks JDBC driver" -ForegroundColor Yellow
}

# Step 3: Copy driver to layer structure
Write-Host "Step 3: Copying driver to layer structure..." -ForegroundColor Yellow
try {
    Copy-Item $DriverPath $JavaLibDir -Force
    Write-Host "SUCCESS: Driver copied to layer structure" -ForegroundColor Green
}
catch {
    Write-Host "ERROR: Failed to copy driver: $_" -ForegroundColor Red
    exit 1
}

# Step 4: Create layer ZIP package
$LayerZip = "databricks-jdbc-layer.zip"
Write-Host "Step 4: Creating layer ZIP package..." -ForegroundColor Yellow
try {
    if (Test-Path $LayerZip) {
        Remove-Item $LayerZip -Force
    }
    
    # Use PowerShell's Compress-Archive
    Compress-Archive -Path "$LayerDir\*" -DestinationPath $LayerZip -Force
    
    $ZipSize = (Get-Item $LayerZip).Length
    $ZipSizeMB = [math]::Round($ZipSize / 1MB, 2)
    Write-Host "SUCCESS: Layer ZIP created: $LayerZip ($ZipSizeMB MB)" -ForegroundColor Green
    
    # Check size limits
    if ($ZipSize -gt 52428800) {  # 50MB limit for direct upload
        Write-Host "WARNING: Layer size exceeds 50MB - will use S3 upload" -ForegroundColor Yellow
    }
}
catch {
    Write-Host "ERROR: Failed to create ZIP: $_" -ForegroundColor Red
    exit 1
}

# Step 5: Upload layer to S3
$S3Key = "$S3KeyPrefix/databricks-jdbc-layer.zip"
Write-Host "Step 5: Uploading layer to S3..." -ForegroundColor Yellow
try {
    aws s3 cp $LayerZip "s3://$S3Bucket/$S3Key" --region $Region
    if ($LASTEXITCODE -ne 0) {
        throw "S3 upload failed"
    }
    Write-Host "SUCCESS: Layer uploaded to s3://$S3Bucket/$S3Key" -ForegroundColor Green
}
catch {
    Write-Host "ERROR: S3 upload failed: $_" -ForegroundColor Red
    exit 1
}

# Step 6: Publish Lambda layer
Write-Host "Step 6: Publishing Lambda layer..." -ForegroundColor Yellow
try {
    $LayerOutput = aws lambda publish-layer-version `
        --layer-name $LayerName `
        --description $Description `
        --content "S3Bucket=$S3Bucket,S3Key=$S3Key" `
        --compatible-runtimes java17 java11 java8.al2 `
        --region $Region `
        --output json
    
    if ($LASTEXITCODE -ne 0) {
        throw "Layer publication failed"
    }
    
    $LayerInfo = $LayerOutput | ConvertFrom-Json
    $LayerArn = $LayerInfo.LayerArn
    $LayerVersion = $LayerInfo.Version
    
    Write-Host "SUCCESS: Layer published successfully" -ForegroundColor Green
    Write-Host "  Layer ARN: $LayerArn" -ForegroundColor White
    Write-Host "  Version: $LayerVersion" -ForegroundColor White
}
catch {
    Write-Host "ERROR: Layer publication failed: $_" -ForegroundColor Red
    exit 1
}

# Step 7: Clean up temporary files
Write-Host "Step 7: Cleaning up..." -ForegroundColor Yellow
try {
    Remove-Item -Recurse -Force $LayerDir
    Remove-Item $LayerZip -Force
    Write-Host "SUCCESS: Cleanup completed" -ForegroundColor Green
}
catch {
    Write-Host "WARNING: Cleanup warning: $_" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== Layer Deployment Summary ===" -ForegroundColor Green
Write-Host "SUCCESS: Layer Name: $LayerName" -ForegroundColor Green
Write-Host "SUCCESS: Layer ARN: $LayerArn" -ForegroundColor Green
Write-Host "SUCCESS: Version: $LayerVersion" -ForegroundColor Green
Write-Host "SUCCESS: Compatible Runtimes: java17, java11, java8.al2" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Update your CloudFormation template to reference this layer" -ForegroundColor White
Write-Host "2. Rebuild your Lambda function without the JDBC driver" -ForegroundColor White
Write-Host "3. Deploy the updated Lambda function" -ForegroundColor White
Write-Host ""
Write-Host "Layer ARN to use in CloudFormation:" -ForegroundColor Cyan
Write-Host "$LayerArn" -ForegroundColor Yellow