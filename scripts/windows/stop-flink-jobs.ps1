$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

try {
  $response = Invoke-RestMethod -Uri "http://127.0.0.1:18082/jobs" -TimeoutSec 5
} catch {
  Write-Host "Flink REST is not ready; no running jobs were cancelled."
  exit 0
}

$active = @($response.jobs | Where-Object { $_.status -in @("RUNNING", "RESTARTING", "CREATED", "FAILING") })
foreach ($job in $active) {
  docker compose exec -T flink-jobmanager flink cancel $job.id
  if ($LASTEXITCODE -ne 0) { throw "Failed to cancel Flink job $($job.id)" }
}

$deadline = (Get-Date).AddSeconds(30)
do {
  $remaining = @((Invoke-RestMethod -Uri "http://127.0.0.1:18082/jobs" -TimeoutSec 5).jobs |
    Where-Object { $_.status -in @("RUNNING", "RESTARTING", "CREATED", "FAILING", "CANCELLING") })
  if ($remaining.Count -eq 0) { break }
  Start-Sleep -Seconds 1
} while ((Get-Date) -lt $deadline)

if ($remaining.Count -gt 0) {
  throw "Timed out waiting for $($remaining.Count) Flink jobs to stop"
}
Write-Host "All active Flink jobs are stopped."
