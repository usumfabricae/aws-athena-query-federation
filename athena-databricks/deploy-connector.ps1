# Deploy Databricks Athena Connector to AWS Lambda
# This script builds, uploads to S3, and deploys the connector using CloudFormation

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
    [string]$StackName = "athena-databricks-connector",
    [string]$Region = "us-east-1",
    [int]$LambdaTimeout = 900,
    [int]$LambdaMemory = 3008,
    [string]$SecurityGroupIds = "",
    [string]$SubnetIds = ""
)

Write-Host "=== Databricks Athena Connector Deployment ===" -ForegroundColor Green
Write-Host "S3 Bucket: $S3Bucket" -ForegroundColor Cyan
Write-Host "Lambda Function: $LambdaFunctionName" -ForegroundColor Cyan
Write-Host "Stack Name: $StackName" -ForegroundColor Cyan
Write-Host "Region: $Region" -ForegroundColor Cyan
Write-Host ""

# Step 1: Build the connector
Write-Host "Step 1: Building the connector..." -ForegroundColor Yellow
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

# Step 2: Upload to S3
$JarPath = ".\target\athena-databricks-2022.47.1.jar"
if (!(Test-Path $JarPath)) {
    Write-Host "✗ JAR file not found at $JarPath" -ForegroundColor Red
    exit 1
}

Write-Host "Step 2: Uploading JAR to S3..." -ForegroundColor Yellow
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

# Step 3: Deploy with CloudFormation
Write-Host "Step 3: Deploying with CloudFormation..." -ForegroundColor Yellow

# Build parameters
$Parameters = @(
    "ParameterKey=LambdaFunctionName,ParameterValue=$LambdaFunctionName",
    "ParameterKey=DefaultConnectionString,ParameterValue=$DefaultConnectionString",
    "ParameterKey=SecretNamePrefix,ParameterValue=$SecretNamePrefix",
    "ParameterKey=SpillBucket,ParameterValue=$SpillBucket",
    "ParameterKey=SpillPrefix,ParameterValue=$SpillPrefix",
    "ParameterKey=CodeS3Bucket,ParameterValue=$S3Bucket",
    "ParameterKey=CodeS3Key,ParameterValue=$S3Key",
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
Write-Host "✓ Connector built and packaged" -ForegroundColor Green
Write-Host "✓ JAR uploaded to S3: s3://$S3Bucket/$S3Key" -ForegroundColor Green
Write-Host "✓ CloudFormation stack deployed: $StackName" -ForegroundColor Green
Write-Host "✓ Lambda function created: $LambdaFunctionName" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Configure your Databricks connection details in AWS Secrets Manager" -ForegroundColor White
Write-Host "2. Register the connector with Amazon Athena" -ForegroundColor White
Write-Host "3. Test the connector with sample queries" -ForegroundColor White
Write-Host ""
Write-Host "For troubleshooting, check:" -ForegroundColor Cyan
Write-Host "- CloudWatch logs: /aws/lambda/$LambdaFunctionName" -ForegroundColor White
Write-Host "- CloudFormation events in the AWS Console" -ForegroundColor White