Write-Host "=== Databricks JDBC Driver Setup ===" -ForegroundColor Green

$LibDir = "..\..\lib"
$DriverPath = Join-Path $LibDir "DatabricksJDBC42.jar"

if (Test-Path $DriverPath) {
    Write-Host "Databricks JDBC driver already exists!" -ForegroundColor Green
    $FileInfo = Get-Item $DriverPath
    $FileSizeMB = [math]::Round($FileInfo.Length / 1024 / 1024, 2)
    Write-Host "File size: ${FileSizeMB}MB" -ForegroundColor Cyan
    exit 0
}

Write-Host "Databricks JDBC driver not found" -ForegroundColor Red
Write-Host "Expected location: $DriverPath" -ForegroundColor Yellow
Write-Host ""
Write-Host "Manual Download Instructions:" -ForegroundColor Yellow
Write-Host "1. Go to: https://www.databricks.com/spark/jdbc-drivers-download" -ForegroundColor White
Write-Host "2. Download the JDBC driver ZIP file" -ForegroundColor White
Write-Host "3. Extract DatabricksJDBC42.jar from the ZIP" -ForegroundColor White
Write-Host "4. Place it at: $DriverPath" -ForegroundColor White
Write-Host ""

# Try automatic download
Write-Host "Attempting automatic download..." -ForegroundColor Yellow
$DownloadUrl = "https://databricks-bi-artifacts.s3.us-east-1.amazonaws.com/simbaspark-drivers/jdbc/2.6.36/DatabricksJDBC42-2.6.36.1043.zip"
$ZipPath = Join-Path $LibDir "temp.zip"

try {
    Invoke-WebRequest -Uri $DownloadUrl -OutFile $ZipPath -UseBasicParsing -ErrorAction Stop
    
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $Zip = [System.IO.Compression.ZipFile]::OpenRead($ZipPath)
    
    $JarEntry = $Zip.Entries | Where-Object { $_.Name -eq "DatabricksJDBC42.jar" }
    
    if ($JarEntry) {
        $JarStream = $JarEntry.Open()
        $FileStream = [System.IO.File]::Create($DriverPath)
        $JarStream.CopyTo($FileStream)
        $FileStream.Close()
        $JarStream.Close()
        Write-Host "Successfully downloaded!" -ForegroundColor Green
    }
    
    $Zip.Dispose()
    Remove-Item $ZipPath -Force -ErrorAction SilentlyContinue
    
} catch {
    Write-Host "Automatic download failed. Please download manually." -ForegroundColor Red
    exit 1
}