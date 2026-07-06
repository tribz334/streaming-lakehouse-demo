param(
  [switch]$SkipStreamingSubmit,
  [int]$WarmupSeconds = 30
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

Write-Host "Starting USTC streaming lakehouse demo..."

./scripts/windows/start-core.ps1
./scripts/windows/init-flink-ddl.ps1

if (-not $SkipStreamingSubmit) {
  ./scripts/windows/submit-streaming-jobs.ps1
  Write-Host "Waiting ${WarmupSeconds}s for streaming jobs to materialize data..."
  Start-Sleep -Seconds $WarmupSeconds
} else {
  Write-Host "Skipping streaming job submission because -SkipStreamingSubmit was set."
}

docker compose --profile governance up -d apicurio
docker compose --profile metastore up -d hive-metastore
docker compose --profile ops up -d prometheus
./scripts/windows/register-schemas.ps1

docker compose --profile olap up -d starrocks starrocks-be
./scripts/windows/init-starrocks.ps1

./scripts/windows/stop-flink-jobs.ps1
./scripts/windows/run-ads-batches.ps1
./scripts/windows/sync-starrocks-olap.ps1
if (-not $SkipStreamingSubmit) {
  ./scripts/windows/submit-streaming-jobs.ps1
}

docker compose --profile olap --profile bi up -d superset
docker compose --profile olap --profile bi exec -T superset python /app/pythonpath/bootstrap_datasets.py

./scripts/windows/export-governance-metadata.ps1
./scripts/windows/export-datahub-mcp.ps1
./scripts/windows/generate-ops-dashboard.ps1
./scripts/windows/generate-scheduler-dashboard.ps1

docker compose --profile scheduler up -d --build dolphinscheduler
$schedulerDeadline = (Get-Date).AddMinutes(2)
do {
  Start-Sleep -Seconds 3
  try {
    $schedulerReady = (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:12345/dolphinscheduler/ui/" -TimeoutSec 5).StatusCode -eq 200
  } catch {
    $schedulerReady = $false
  }
} while (-not $schedulerReady -and (Get-Date) -lt $schedulerDeadline)
if (-not $schedulerReady) { throw "DolphinScheduler did not become ready within two minutes." }
./scripts/windows/bootstrap-dolphinscheduler.ps1

./scripts/windows/verify-stack.ps1

Write-Host ""
Write-Host "Demo is ready."
Write-Host "  Flink UI:             http://127.0.0.1:8082"
Write-Host "  StarRocks FE:         http://127.0.0.1:8030"
Write-Host "  Superset:             http://127.0.0.1:8088  admin / admin"
Write-Host "  Prometheus:           http://127.0.0.1:19090"
Write-Host "  Apicurio Registry:    http://127.0.0.1:8081/apis/registry/v3/system/info"
Write-Host "  DolphinScheduler:     http://127.0.0.1:12345/dolphinscheduler/ui/  admin / dolphinscheduler123"
Write-Host "  Local ops dashboard:  $root\ops-dashboard\index.html"
Write-Host "  Scheduler dashboard:  $root\dolphinscheduler\dashboard\index.html"
