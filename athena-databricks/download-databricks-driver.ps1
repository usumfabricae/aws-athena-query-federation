# Download Databricks JDBC Driver
# This script helps you get the Databricks JDBC driver required for the connector

Write-Host "=== Databricks JDBC Driver Setup ===" -ForegroundColor Green

$LibDir = "..\..\lib"
$DriverPath = Join-Path $LibDir "DatabricksJDBC42.jar"

# Check if driver already exists
if (Test-Path $DriverPath) {
    Write-Host "✓ Databricks JDBC driver already exists!" -ForegroundColor Green
    $FileInfo = Get-Item $DriverPath
    $FileSizeMB = [math]::Round($FileInfo.Length / 1024 / 1024, 2)
    Write-Host "  Location: $DriverPath" -ForegroundColor Cyan
    Write-Host "  File size: ${FileSizeMB}MB" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "You can now build the connector using: .\build-package.ps1" -ForegroundColor Yellow
    exit 0
}

Write-Host "✗ Databricks JDBC driver not found" -ForegroundColor Red
Write-Host "  Expected location: $DriverPath" -ForegroundColor Yellow
Write-Host ""

Write-Host "=== Manual Download Instructions ===" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Visit the Databricks JDBC Driver download page:" -ForegroundColor White
Write-Host "   https://www.databricks.com/spark/jdbc-drivers-download" -ForegroundColor Cyan
Write-Host ""
Write-Host "2. Download the latest JDBC driver ZIP file" -ForegroundColor White
Write-Host "   (File name will be like: DatabricksJDBC42-x.x.x.xxxx.zip)" -ForegroundColor Gray
Write-Host ""
Write-Host "3. Extract the ZIP file and find 'DatabricksJDBC42.jar'" -ForegroundColor White
Write-Host ""
Write-Host "4. Copy the JAR file to:" -ForegroundColor White
Write-Host "   $DriverPath" -ForegroundColor Cyan
Write-Host ""
Write-Host "5. Run this script again to verify the installation" -ForegroundColor White
Write-Host ""

Write-Host "=== Alternative: Try Automatic Download ===" -ForegroundColor Yellow
Write-Host ""
$Response = Read-Host "Would you like to try automatic download? (y/n)"

if ($Response -eq "y" -or $Response -eq "Y") {
    Write-Host ""
    Write-Host "Attempting automatic download..." -ForegroundColor Yellow
    
    $DownloadUrl = "https://databricks-bi-artifacts.s3.us-east-1.amazonaws.com/simbaspark-drivers/jdbc/2.6.36/DatabricksJDBC42-2.6.36.1043.zip"
    $ZipPath = Join-Path $LibDir "DatabricksJDBC42.zip"
    
    try {
        Write-Host "Downloading from Databricks..." -ForegroundColor Yellow
        Invoke-WebRequest -Uri $DownloadUrl -OutFile $ZipPath -UseBasicParsing
        
        Write-Host "Extracting JAR file..." -ForegroundColor Yellow
        Add-Type -AssemblyName System.IO.Compression.FileSystem
        $Zip = [System.IO.Compression.ZipFile]::OpenRead($ZipPath)
        
        $JarEntry = $Zip.Entries | Where-Object { $_.Name -eq "DatabricksJDBC42.jar" }
        
        if ($JarEntry) {
            $JarStream = $JarEntry.Open()
            $FileStream = [System.IO.File]::Create($DriverPath)
            $JarStream.CopyTo($FileStream)
            $FileStream.Close()
            $JarStream.Close()
            
            Write-Host "✓ Successfully downloaded and extracted!" -ForegroundColor Green
            
            $FileInfo = Get-Item $DriverPath
            $FileSizeMB = [math]::Round($FileInfo.Length / 1024 / 1024, 2)
            Write-Host "  File size: ${FileSizeMB}MB" -ForegroundColor Cyan
        }
        else {
            throw "JAR file not found in ZIP"
        }
        
        $Zip.Dispose()
        Remove-Item $ZipPath -Force
        
        Write-Host ""
        Write-Host "✓ Databricks JDBC driver is ready!" -ForegroundColor Green
        Write-Host "You can now build the connector using: .\build-package.ps1" -ForegroundColor Yellow
        
    }
    catch {
        Write-Host "✗ Automatic download failed: $_" -ForegroundColor Red
        Write-Host ""
        Write-Host "Please follow the manual download instructions above." -ForegroundColor Yellow
        exit 1
    }
}
else {
    Write-Host "Please follow the manual download instructions above." -ForegroundColor Yellow
    exit 1
}