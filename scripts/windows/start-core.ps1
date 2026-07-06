$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

& (Join-Path $PSScriptRoot "download-flink-jars.ps1")
docker compose --profile core build flink-jobmanager event-generator
docker compose --profile core up -d --no-build

Write-Host ""
Write-Host "Core stack requested. Useful URLs:"
Write-Host "  Flink Web UI    http://127.0.0.1:8082"
Write-Host "  StarRocks FE    http://127.0.0.1:8030"
Write-Host "  Kafka external  localhost:29092"
Write-Host ""
Write-Host "Next:"
Write-Host "  ./scripts/windows/init-flink-ddl.ps1"
Write-Host "  ./scripts/windows/submit-streaming-jobs.ps1"
