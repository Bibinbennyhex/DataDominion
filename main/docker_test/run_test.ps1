param(
    [switch]$KeepRunning,
    [switch]$IncludePerformance,
    [ValidateSet("TINY","SMALL","MEDIUM","LARGE")]
    [string]$PerformanceScale = "TINY"
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

Write-Host "Starting docker services..."
docker compose up -d

try {
    Write-Host "Waiting for services to become ready..."
    Start-Sleep -Seconds 15

    Write-Host "Running main pipeline docker tests..."
    $testArgs = @("python3", "/workspace/main/docker_test/tests/run_all_tests.py")
    if ($IncludePerformance) {
        $testArgs += @("--include-performance", "--performance-scale", $PerformanceScale)
    }
    docker compose exec -T spark-iceberg-main @testArgs

    Write-Host "Tests completed successfully."
}
finally {
    if (-not $KeepRunning) {
        Write-Host "Stopping docker services..."
        docker compose down
    }
    else {
        Write-Host "Keeping docker services running (KeepRunning enabled)."
    }
}
