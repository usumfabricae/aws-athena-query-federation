# Build Databricks Athena Connector (Layer Version)
# This script builds the connector JAR excluding the Databricks JDBC driver
# The JDBC driver should be deployed as a separate Lambda layer

param(
    [Parameter(Mandatory=$false)]
    [switch]$SkipTests = $false,
    
    [Parameter(Mandatory=$false)]
    [switch]$Clean = $true
)

Write-Host "=== Building Databricks Athena Connector (Layer Version) ===" -ForegroundColor Green
Write-Host "This build excludes the Databricks JDBC driver from the JAR" -ForegroundColor Cyan
Write-Host "The driver should be deployed as a separate Lambda layer" -ForegroundColor Cyan
Write-Host ""

# Check if Maven is available
try {
    $MavenVersion = mvn --version 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Maven not found"
    }
    Write-Host "✓ Maven found" -ForegroundColor Green
}
catch {
    Write-Host "✗ Maven is required but not found in PATH" -ForegroundColor Red
    Write-Host "Please install Maven and ensure it's in your PATH" -ForegroundColor Red
    exit 1
}

# Step 1: Clean previous builds
if ($Clean) {
    Write-Host "Step 1: Cleaning previous builds..." -ForegroundColor Yellow
    try {
        mvn clean -q
        if ($LASTEXITCODE -ne 0) {
            throw "Maven clean failed"
        }
        Write-Host "✓ Clean completed" -ForegroundColor Green
    }
    catch {
        Write-Host "✗ Clean failed: $_" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "Step 1: Skipping clean (as requested)" -ForegroundColor Yellow
}

# Step 2: Download Databricks driver (for compilation, but exclude from JAR)
Write-Host "Step 2: Ensuring Databricks JDBC driver is available..." -ForegroundColor Yellow
$DriverPath = ".\lib\DatabricksJDBC42.jar"
if (!(Test-Path $DriverPath)) {
    try {
        & .\download-databricks-driver.ps1
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to download Databricks driver"
        }
        Write-Host "✓ Databricks driver downloaded" -ForegroundColor Green
    }
    catch {
        Write-Host "✗ Failed to download driver: $_" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "✓ Databricks driver already available" -ForegroundColor Green
}

# Step 3: Build with Maven (excluding JDBC driver from final JAR)
$MavenGoals = "compile package"
if ($SkipTests) {
    $MavenGoals += " -DskipTests"
    Write-Host "Step 3: Building connector (skipping tests)..." -ForegroundColor Yellow
} else {
    Write-Host "Step 3: Building connector (including tests)..." -ForegroundColor Yellow
}

try {
    # The JDBC driver has 'provided' scope, so it won't be included in the JAR
    mvn $MavenGoals -q
    if ($LASTEXITCODE -ne 0) {
        throw "Maven build failed"
    }
    Write-Host "✓ Build completed successfully" -ForegroundColor Green
}
catch {
    Write-Host "✗ Build failed: $_" -ForegroundColor Red
    Write-Host "Try running with verbose output: mvn $MavenGoals" -ForegroundColor Red
    exit 1
}

# Step 4: Verify build output
$JarPath = ".\target\athena-databricks-2022.47.1.jar"
if (!(Test-Path $JarPath)) {
    Write-Host "✗ Expected JAR file not found at $JarPath" -ForegroundColor Red
    Write-Host "Check the Maven build output for errors" -ForegroundColor Red
    exit 1
}

# Step 5: Check JAR size and contents
try {
    $JarSize = (Get-Item $JarPath).Length
    $JarSizeMB = [math]::Round($JarSize / 1MB, 2)
    Write-Host "✓ JAR created: $JarPath ($JarSizeMB MB)" -ForegroundColor Green
    
    # Check if JDBC driver is excluded
    $JarContents = jar -tf $JarPath | Select-String -Pattern "DatabricksJDBC|databricks.*jdbc" -Quiet
    if ($JarContents) {
        Write-Host "⚠ WARNING: JDBC driver may still be included in JAR" -ForegroundColor Yellow
        Write-Host "Verify the Maven profile is correctly excluding the driver" -ForegroundColor Yellow
    } else {
        Write-Host "✓ JDBC driver successfully excluded from JAR" -ForegroundColor Green
    }
    
    # Size validation
    if ($JarSize -gt 262144000) {  # 250MB limit
        Write-Host "✗ JAR size still exceeds AWS Lambda limit of 250MB" -ForegroundColor Red
        Write-Host "Additional optimization may be required" -ForegroundColor Red
        exit 1
    } elseif ($JarSize -gt 52428800) {  # 50MB limit for direct upload
        Write-Host "⚠ JAR size exceeds 50MB - will require S3 upload for deployment" -ForegroundColor Yellow
    } else {
        Write-Host "✓ JAR size is within direct upload limits" -ForegroundColor Green
    }
}
catch {
    Write-Host "✗ Failed to analyze JAR: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== Build Summary ===" -ForegroundColor Green
Write-Host "✓ Connector built successfully" -ForegroundColor Green
Write-Host "✓ JAR file: $JarPath ($JarSizeMB MB)" -ForegroundColor Green
Write-Host "✓ JDBC driver excluded (deploy as separate layer)" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Deploy the Databricks JDBC driver as a Lambda layer using deploy-layer.ps1" -ForegroundColor White
Write-Host "2. Update CloudFormation template to reference the layer" -ForegroundColor White
Write-Host "3. Deploy the Lambda function using the updated template" -ForegroundColor White