param(
  [switch]$SkipStreamingSubmit,
  [int]$WarmupSeconds = 30,
  [switch]$WithOps,
  [switch]$WithOlap
)

$ErrorActionPreference = "Stop"
if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $true
}
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

try {
  docker info | Out-Null
} catch {
  throw "Docker Desktop Linux engine is not running. Start Docker Desktop first, then rerun ./scripts/windows/start-multi-node.ps1."
}

Write-Host "Starting USTC streaming lakehouse demo..."
Write-Host "Topology:"
Write-Host "  node-1: MySQL, Kafka broker/controller, Hive Metastore, Flink JM/TM, producer, optional StarRocks FE/BE"
Write-Host ""

& (Join-Path $PSScriptRoot "download-flink-jars.ps1")

$compose = @("compose")
& docker @compose build flink-jobmanager event-generator-node-1
& docker @compose up -d --no-build --remove-orphans

./scripts/windows/init-flink-ddl.ps1
./scripts/windows/submit-cdc-pipeline.ps1

if (-not $SkipStreamingSubmit) {
  ./scripts/windows/submit-streaming-jobs.ps1
  Write-Host "Waiting ${WarmupSeconds}s for streaming jobs and producers to materialize data..."
  Start-Sleep -Seconds $WarmupSeconds
} else {
  Write-Host "Skipping streaming job submission because -SkipStreamingSubmit was set."
}

if ($WithOps) {
  & docker @compose --profile ops up -d prometheus loki alloy grafana
}

if ($WithOlap) {
  & docker @compose --profile olap up -d `
    starrocks starrocks-be-node-1
  ./scripts/windows/init-starrocks.ps1
}

./scripts/windows/verify-multi-node.ps1

Write-Host ""
Write-Host "Multi-node core demo is ready."
Write-Host "  Flink UI:        http://127.0.0.1:8082"
Write-Host "  Kafka external:  localhost:29092"
Write-Host "  Hive Metastore:  thrift://127.0.0.1:19083"
if ($WithOps) {
  Write-Host "  Prometheus:      http://127.0.0.1:19090"
  Write-Host "  Grafana:         http://127.0.0.1:13000"
  Write-Host "  Alloy:           http://127.0.0.1:12346"
}
if ($WithOlap) {
  Write-Host "  StarRocks FE:    http://127.0.0.1:8030"
}
