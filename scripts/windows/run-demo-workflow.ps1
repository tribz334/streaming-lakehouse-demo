$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$runDir = Join-Path $root "dolphinscheduler/runs"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null

$steps = @(
  @{ name = "stop_flink_jobs"; command = "./scripts/windows/stop-flink-jobs.ps1" },
  @{ name = "init_flink_ddl"; command = "./scripts/windows/init-flink-ddl.ps1" },
  @{ name = "run_ads_batches"; command = "./scripts/windows/run-ads-batches.ps1" },
  @{ name = "sync_starrocks_olap"; command = "./scripts/windows/sync-starrocks-olap.ps1" },
  @{ name = "register_schemas"; command = "./scripts/windows/register-schemas.ps1" },
  @{ name = "export_governance_metadata"; command = "./scripts/windows/export-governance-metadata.ps1" },
  @{ name = "export_datahub_mcp"; command = "./scripts/windows/export-datahub-mcp.ps1" },
  @{ name = "generate_ops_dashboard"; command = "./scripts/windows/generate-ops-dashboard.ps1" },
  @{ name = "submit_streaming_jobs"; command = "./scripts/windows/submit-streaming-jobs.ps1" },
  @{ name = "verify_stack"; command = "./scripts/windows/verify-stack.ps1" }
)

$startedAt = Get-Date
$runId = $startedAt.ToString("yyyyMMdd-HHmmss")
$stepResults = @()
$status = "SUCCESS"
Write-Host "Starting local demo workflow at $($startedAt.ToString("s"))"

try {
  foreach ($step in $steps) {
    $stepStarted = Get-Date
    Write-Host ""
    Write-Host ">>> [$($step.name)] $($step.command)"
    try {
      Invoke-Expression $step.command
      $stepStatus = "SUCCESS"
      $errorMessage = $null
    } catch {
      $stepStatus = "FAILED"
      $errorMessage = $_.Exception.Message
      $status = "FAILED"
      throw
    } finally {
      $stepEnded = Get-Date
      $duration = [int]($stepEnded - $stepStarted).TotalSeconds
      $stepResults += [pscustomobject]@{
        name = $step.name
        command = $step.command
        status = $stepStatus
        started_at = $stepStarted.ToString("s")
        ended_at = $stepEnded.ToString("s")
        duration_seconds = $duration
        error = $errorMessage
      }
      Write-Host "<<< [$($step.name)] finished in ${duration}s"
    }
  }
} finally {
  $endedAt = Get-Date
  $totalDuration = [int]($endedAt - $startedAt).TotalSeconds
  $run = [ordered]@{
    run_id = $runId
    workflow = "ad-lakehouse-demo"
    status = $status
    started_at = $startedAt.ToString("s")
    ended_at = $endedAt.ToString("s")
    duration_seconds = $totalDuration
    steps = $stepResults
  }
  $runPath = Join-Path $runDir "$runId.json"
  $latestPath = Join-Path $runDir "latest-run.json"
  $run | ConvertTo-Json -Depth 8 | Set-Content -Path $runPath -Encoding UTF8
  $run | ConvertTo-Json -Depth 8 | Set-Content -Path $latestPath -Encoding UTF8
  try {
    ./scripts/windows/generate-scheduler-dashboard.ps1
  } catch {
    Write-Warning "Scheduler dashboard generation failed: $($_.Exception.Message)"
  }
  try {
    ./scripts/windows/generate-ops-dashboard.ps1
  } catch {
    Write-Warning "Ops dashboard final refresh failed: $($_.Exception.Message)"
  }
}

Write-Host ""
Write-Host "Local demo workflow finished in ${totalDuration}s."
