# Deploy Databricks Athena Connector with Lambda Layer
# This script builds, uploads, and deploys both the JDBC layer and connector function

param(
    [Parameter(Mandatory=$true)]
    [string]$S3Bucket,
    
    [Parameter(Mandatory=$true)]
    [string]$LambdaFunctionName,
    
    [Parameter(Mandatory=$false)]
    [string]$DefaultConnectionString = "databricks://default",
    
    [Parameter(Mandatory=$true)]
    [string]$SecretNamePrefix,
    
    [Parameter(Mandatory=$true)]
    [string]$SpillBucket,
    
    [string]$SpillPrefix = "athena-spill",
    [string]$S3Key = "athena-databricks-2022.47.1.jar",
    [string]$LayerName = "databricks-jdbc-driver",
    [string]$StackName = "athena-databricks-connector-with-layer",
    [string]$Region = "us-east-1",
    [int]$LambdaTimeout = 900,
    [int]$LambdaMemory = 3008,
    [string]$SecurityGroupIds = "",
    [string]$SubnetIds = "",
    [switch]$SkipLayerDeployment = $false,
    [string]$ExistingLayerArn = ""
)

Write-Host "=== Databricks Athena Connector Deployment (with Layer) ===" -ForegroundColor Green
Write-Host "S3 Bucket: $S3Bucket" -ForegroundColor Cyan
Write-Host "Lambda Function: $LambdaFunctionName" -ForegroundColor Cyan
Write-Host "Layer Name: $LayerName" -ForegroundColor Cyan
Write-Host "Stack Name: $StackName" -ForegroundColor Cyan
Write-Host "Region: $Region" -ForegroundColor Cyan
Write-Host ""

$LayerArn = $ExistingLayerArn

# Step 1: Deploy Lambda Layer (if not skipped)
if (!$SkipLayerDeployment -and $ExistingLayerArn -eq "") {
    Write-Host "Step 1: Deploying Databricks JDBC Layer..." -ForegroundColor Yellow
    try {
        $LayerOutput = & .\deploy-layer.ps1 -S3Bucket $S3Bucket -LayerName $LayerName -Region $Region
        if ($LASTEXITCODE -ne 0) {
            throw "Layer deployment failed"
        }
        
        # Extract Layer ARN from output
        $LayerArnMatch = $LayerOutput | Select-String -Pattern "arn:aws:lambda:.*:layer:.*:\d+"
        if ($LayerArnMatch) {
            $LayerArn = $LayerArnMatch.Matches[0].Value
            Write-Host "✓ Layer deployed successfully: $LayerArn" -ForegroundColor Green
        } else {
            throw "Could not extract Layer ARN from deployment output"
        }
    }
    catch {
        Write-Host "✗ Layer deployment failed: $_" -ForegroundColor Red
        exit 1
    }
} elseif ($ExistingLayerArn -ne "") {
    Write-Host "Step 1: Using existing layer: $ExistingLayerArn" -ForegroundColor Yellow
    $LayerArn = $ExistingLayerArn
} else {
    Write-Host "Step 1: Skipping layer deployment (as requested)" -ForegroundColor Yellow
    Write-Host "⚠ You must provide -ExistingLayerArn parameter when skipping layer deployment" -ForegroundColor Red
    exit 1
}

# Step 2: Build the connector (without JDBC driver)
Write-Host "Step 2: Building the connector (excluding JDBC driver)..." -ForegroundColor Yellow
try {
    & .\build-package.ps1
    if ($LASTEXITCODE -ne 0) {
        throw "Build failed"
    }
    Write-Host "✓ Build completed successfully" -ForegroundColor Green
}
catch {
    Write-Host "✗ Build failed: $_" -ForegroundColor Red
    exit 1
}

# Step 3: Upload connector JAR to S3
$JarPath = ".\target\athena-databricks-2022.47.1.jar"
if (!(Test-Path $JarPath)) {
    Write-Host "✗ JAR file not found at $JarPath" -ForegroundColor Red
    exit 1
}

Write-Host "Step 3: Uploading connector JAR to S3..." -ForegroundColor Yellow
try {
    aws s3 cp $JarPath "s3://$S3Bucket/$S3Key" --region $Region
    if ($LASTEXITCODE -ne 0) {
        throw "S3 upload failed"
    }
    Write-Host "✓ JAR uploaded to s3://$S3Bucket/$S3Key" -ForegroundColor Green
}
catch {
    Write-Host "✗ S3 upload failed: $_" -ForegroundColor Red
    Write-Host "Make sure the S3 bucket exists and you have upload permissions" -ForegroundColor Red
    exit 1
}

# Step 4: Deploy with CloudFormation
Write-Host "Step 4: Deploying with CloudFormation..." -ForegroundColor Yellow

# Build parameters
$Parameters = @(
    "ParameterKey=LambdaFunctionName,ParameterValue=$LambdaFunctionName",
    "ParameterKey=DefaultConnectionString,ParameterValue=$DefaultConnectionString",
    "ParameterKey=SecretNamePrefix,ParameterValue=$SecretNamePrefix",
    "ParameterKey=SpillBucket,ParameterValue=$SpillBucket",
    "ParameterKey=SpillPrefix,ParameterValue=$SpillPrefix",
    "ParameterKey=CodeS3Bucket,ParameterValue=$S3Bucket",
    "ParameterKey=CodeS3Key,ParameterValue=$S3Key",
    "ParameterKey=DatabricksJdbcLayerArn,ParameterValue=$LayerArn",
    "ParameterKey=LambdaTimeout,ParameterValue=$LambdaTimeout",
    "ParameterKey=LambdaMemory,ParameterValue=$LambdaMemory"
)

# Add VPC parameters if provided
if ($SecurityGroupIds -ne "") {
    $Parameters += "ParameterKey=SecurityGroupIds,ParameterValue=$SecurityGroupIds"
}
if ($SubnetIds -ne "") {
    $Parameters += "ParameterKey=SubnetIds,ParameterValue=$SubnetIds"
}

try {
    # Check if stack exists
    $StackExists = $false
    try {
        aws cloudformation describe-stacks --stack-name $StackName --region $Region --query "Stacks[0].StackStatus" --output text 2>$null
        $StackExists = $LASTEXITCODE -eq 0
    }
    catch {
        $StackExists = $false
    }

    if ($StackExists) {
        Write-Host "Updating existing stack..." -ForegroundColor Yellow
        aws cloudformation update-stack `
            --stack-name $StackName `
            --template-body file://athena-databricks.yaml `
            --parameters $Parameters `
            --capabilities CAPABILITY_IAM `
            --region $Region
    }
    else {
        Write-Host "Creating new stack..." -ForegroundColor Yellow
        aws cloudformation create-stack `
            --stack-name $StackName `
            --template-body file://athena-databricks.yaml `
            --parameters $Parameters `
            --capabilities CAPABILITY_IAM `
            --region $Region
    }

    if ($LASTEXITCODE -ne 0) {
        throw "CloudFormation deployment failed"
    }

    Write-Host "✓ CloudFormation deployment initiated" -ForegroundColor Green
    Write-Host "Waiting for stack deployment to complete..." -ForegroundColor Yellow

    # Wait for stack completion
    aws cloudformation wait stack-create-complete --stack-name $StackName --region $Region 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Stack creation completed successfully" -ForegroundColor Green
    }
    else {
        aws cloudformation wait stack-update-complete --stack-name $StackName --region $Region 2>$null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ Stack update completed successfully" -ForegroundColor Green
        }
        else {
            Write-Host "⚠ Stack deployment may still be in progress" -ForegroundColor Yellow
            Write-Host "Check the CloudFormation console for status" -ForegroundColor Yellow
        }
    }
}
catch {
    Write-Host "✗ CloudFormation deployment failed: $_" -ForegroundColor Red
    Write-Host "Check the CloudFormation console for detailed error information" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== Deployment Summary ===" -ForegroundColor Green
Write-Host "✓ JDBC Layer deployed: $LayerArn" -ForegroundColor Green
Write-Host "✓ Connector built and packaged (without JDBC driver)" -ForegroundColor Green
Write-Host "✓ JAR uploaded to S3: s3://$S3Bucket/$S3Key" -ForegroundColor Green
Write-Host "✓ CloudFormation stack deployed: $StackName" -ForegroundColor Green
Write-Host "✓ Lambda function created: $LambdaFunctionName" -ForegroundColor Green
Write-Host ""
Write-Host "Architecture:" -ForegroundColor Cyan
Write-Host "- Lambda Layer: Contains Databricks JDBC driver" -ForegroundColor White
Write-Host "- Lambda Function: Contains connector logic (references layer)" -ForegroundColor White
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Configure your Databricks connection details in AWS Secrets Manager" -ForegroundColor White
Write-Host "2. Register the connector with Amazon Athena" -ForegroundColor White
Write-Host "3. Test the connector with sample queries" -ForegroundColor White
Write-Host ""
Write-Host "For troubleshooting, check:" -ForegroundColor Cyan
Write-Host "- CloudWatch logs: /aws/lambda/$LambdaFunctionName" -ForegroundColor White
Write-Host "- CloudFormation events in the AWS Console" -ForegroundColor White