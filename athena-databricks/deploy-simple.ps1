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
    [string]$Region = "eu-south-1"
)

Write-Host "=== Databricks Athena Connector Deployment ===" -ForegroundColor Green
Write-Host "S3 Bucket: $S3Bucket"
Write-Host "Lambda Function: $LambdaFunctionName"
Write-Host "Stack Name: $StackName"
Write-Host "Region: $Region"
Write-Host ""

# Step 1: Ensure Databricks JDBC driver is available
Write-Host "Step 1: Checking Databricks JDBC driver..." -ForegroundColor Yellow
$LibDir = "..\..\lib"
$DatabricksJar = Join-Path $LibDir "DatabricksJDBC42.jar"
if (!(Test-Path $DatabricksJar)) {
    Write-Host "Downloading Databricks JDBC driver..." -ForegroundColor Yellow
    & .\download-databricks-driver.ps1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to download Databricks JDBC driver" -ForegroundColor Red
        exit 1
    }
}

# Step 2: Build the connector
Write-Host "Step 2: Building the connector..." -ForegroundColor Yellow
$buildResult = & .\build-package.ps1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed" -ForegroundColor Red
    exit 1
}
Write-Host "Build completed successfully" -ForegroundColor Green

# Step 2: Check if JAR exists
$JarPath = ".\target\athena-databricks-2022.47.1.jar"
if (!(Test-Path $JarPath)) {
    Write-Host "JAR file not found at $JarPath" -ForegroundColor Red
    exit 1
}

# Step 3: Upload to S3
Write-Host "Step 3: Uploading JAR to S3..." -ForegroundColor Yellow
$uploadResult = aws s3 cp $JarPath "s3://$S3Bucket/$S3Key" --region $Region
if ($LASTEXITCODE -ne 0) {
    Write-Host "S3 upload failed" -ForegroundColor Red
    Write-Host "Make sure the S3 bucket exists and you have upload permissions" -ForegroundColor Red
    exit 1
}
Write-Host "JAR uploaded to s3://$S3Bucket/$S3Key" -ForegroundColor Green

# Step 4: Deploy with CloudFormation
Write-Host "Step 4: Deploying with CloudFormation..." -ForegroundColor Yellow

$Parameters = @(
    "ParameterKey=LambdaFunctionName,ParameterValue=$LambdaFunctionName",
    "ParameterKey=DefaultConnectionString,ParameterValue=$DefaultConnectionString",
    "ParameterKey=SecretNamePrefix,ParameterValue=$SecretNamePrefix",
    "ParameterKey=SpillBucket,ParameterValue=$SpillBucket",
    "ParameterKey=SpillPrefix,ParameterValue=$SpillPrefix",
    "ParameterKey=CodeS3Bucket,ParameterValue=$S3Bucket",
    "ParameterKey=CodeS3Key,ParameterValue=$S3Key",
    "ParameterKey=LambdaTimeout,ParameterValue=900",
    "ParameterKey=LambdaMemory,ParameterValue=3008"
)

# Check if stack exists
$StackExists = $false
$checkResult = aws cloudformation describe-stacks --stack-name $StackName --region $Region --query "Stacks[0].StackStatus" --output text 2>$null
if ($LASTEXITCODE -eq 0) {
    $StackExists = $true
}

if ($StackExists) {
    Write-Host "Updating existing stack..." -ForegroundColor Yellow
    $deployResult = aws cloudformation update-stack --stack-name $StackName --template-body file://athena-databricks.yaml --parameters $Parameters --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --region $Region
} else {
    Write-Host "Creating new stack..." -ForegroundColor Yellow
    $deployResult = aws cloudformation create-stack --stack-name $StackName --template-body file://athena-databricks.yaml --parameters $Parameters --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --region $Region
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "CloudFormation deployment failed" -ForegroundColor Red
    Write-Host "Check the CloudFormation console for detailed error information" -ForegroundColor Red
    exit 1
}

Write-Host "CloudFormation deployment initiated" -ForegroundColor Green
Write-Host "Waiting for stack deployment to complete..." -ForegroundColor Yellow

# Wait for completion
if ($StackExists) {
    aws cloudformation wait stack-update-complete --stack-name $StackName --region $Region
} else {
    aws cloudformation wait stack-create-complete --stack-name $StackName --region $Region
}

if ($LASTEXITCODE -eq 0) {
    Write-Host "Stack deployment completed successfully" -ForegroundColor Green
} else {
    Write-Host "Stack deployment may still be in progress" -ForegroundColor Yellow
    Write-Host "Check the CloudFormation console for status" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== Deployment Summary ===" -ForegroundColor Green
Write-Host "Connector built and packaged" -ForegroundColor Green
Write-Host "JAR uploaded to S3: s3://$S3Bucket/$S3Key" -ForegroundColor Green
Write-Host "CloudFormation stack deployed: $StackName" -ForegroundColor Green
Write-Host "Lambda function created: $LambdaFunctionName" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Configure your Databricks connection details in AWS Secrets Manager" -ForegroundColor White
Write-Host "2. Register the connector with Amazon Athena" -ForegroundColor White
Write-Host "3. Test the connector with sample queries" -ForegroundColor White