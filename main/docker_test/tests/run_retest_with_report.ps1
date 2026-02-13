$ErrorActionPreference = "Stop"

$compose = "main/docker_test/docker-compose.yml"
$tests = @(
    @{ name = "simple_test.py"; args = @() },
    @{ name = "test_main_all_cases.py"; args = @() },
    @{ name = "test_main_base_ts_propagation.py"; args = @() },
    @{ name = "run_backfill_test.py"; args = @() },
    @{ name = "test_all_scenarios.py"; args = @() },
    @{ name = "test_all_scenarios_v942.py"; args = @() },
    @{ name = "test_bulk_historical_load.py"; args = @() },
    @{ name = "test_complex_scenarios.py"; args = @() },
    @{ name = "test_comprehensive_50_cases.py"; args = @() },
    @{ name = "test_comprehensive_edge_cases.py"; args = @() },
    @{ name = "test_consecutive_backfill.py"; args = @() },
    @{ name = "test_duplicate_records.py"; args = @() },
    @{ name = "test_full_46_columns.py"; args = @() },
    @{ name = "test_long_backfill_gaps.py"; args = @() },
    @{ name = "test_non_continuous_backfill.py"; args = @() },
    @{ name = "test_null_update_case_iii.py"; args = @() },
    @{ name = "test_null_update_other_cases.py"; args = @() },
    @{ name = "test_null_update.py"; args = @() },
    @{ name = "test_idempotency.py"; args = @() },
    @{ name = "test_latest_summary_consistency.py"; args = @() },
    @{ name = "test_recovery.py"; args = @() },
    @{ name = "test_aggressive_idempotency.py"; args = @("--scale", "TINY", "--cycles", "2", "--reruns", "2") }
)

$logPath = "main/docker_test/tests/_retest_run.log"
$resultPath = "main/docker_test/tests/_retest_results.json"

if (Test-Path $logPath) {
    Remove-Item $logPath -Force
}

$results = @()

foreach ($test in $tests) {
    $name = $test.name
    $args = $test.args
    $display = if ($args.Count -gt 0) { "$name $($args -join ' ')" } else { $name }

    $start = Get-Date
    Add-Content -Path $logPath -Value ("=" * 100)
    Add-Content -Path $logPath -Value ("RUNNING $display")
    Add-Content -Path $logPath -Value ("=" * 100)

    $dockerArgs = @(
        "compose", "-f", $compose,
        "exec", "-T", "spark-iceberg-main",
        "python3", "/workspace/main/docker_test/tests/$name"
    ) + $args

    & docker @dockerArgs >> $logPath 2>&1
    $exitCode = $LASTEXITCODE
    $end = Get-Date

    $durationSec = [Math]::Round(($end - $start).TotalSeconds, 2)
    $status = if ($exitCode -eq 0) { "PASS" } else { "FAIL" }

    $results += [PSCustomObject]@{
        test = $display
        status = $status
        exit_code = $exitCode
        duration_sec = $durationSec
        started_at = $start.ToString("s")
        ended_at = $end.ToString("s")
    }
}

$results | ConvertTo-Json -Depth 4 | Set-Content -Path $resultPath
$results | Format-Table -AutoSize
