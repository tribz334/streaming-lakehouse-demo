$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

try {
  $jobs = (Invoke-RestMethod "http://127.0.0.1:8082/jobs").jobs
} catch {
  Write-Host "Flink REST endpoint is not available; no jobs to stop."
  exit 0
}

$active = @($jobs | Where-Object { $_.status -in @("CREATED", "RUNNING", "FAILING", "RESTARTING", "SUSPENDED") })
if ($active.Count -eq 0) {
  Write-Host "No active Flink jobs to stop."
  exit 0
}

foreach ($job in $active) {
  Write-Host "Stopping Flink job $($job.id) [$($job.status)] ..."
  try {
    Invoke-RestMethod -Method Patch "http://127.0.0.1:8082/jobs/$($job.id)" | Out-Null
  } catch {
    Write-Host "Cancel request failed for $($job.id): $($_.Exception.Message)"
  }
}

$deadline = (Get-Date).AddSeconds(60)
do {
  Start-Sleep -Seconds 2
  $remaining = @((Invoke-RestMethod "http://127.0.0.1:8082/jobs").jobs | Where-Object {
    $_.status -in @("CREATED", "RUNNING", "FAILING", "RESTARTING", "SUSPENDED", "CANCELLING")
  })
} while ($remaining.Count -gt 0 -and (Get-Date) -lt $deadline)

if ($remaining.Count -gt 0) {
  throw "Timed out waiting for Flink jobs to stop: $($remaining.id -join ', ')"
}

Write-Host "All active Flink jobs stopped."
