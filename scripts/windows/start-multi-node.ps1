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

Write-Host "Starting USTC streaming lakehouse multi-node demo..."
Write-Host "Topology:"
Write-Host "  node-1: MySQL, Kafka broker/controller, Flink JM/TM, producer, control services"
Write-Host "  node-2: Kafka broker/controller, Flink TM, producer, optional StarRocks BE"
Write-Host "  node-3: Kafka broker/controller, Flink TM, producer, optional StarRocks BE"
Write-Host ""

& (Join-Path $PSScriptRoot "download-flink-jars.ps1")

$compose = @("compose", "-f", "docker-compose.yml", "-f", "docker-compose.three-node.yml")
& docker @compose --profile core --profile multi-node build flink-jobmanager event-generator
& docker @compose --profile core --profile multi-node up -d --no-build

./scripts/windows/init-flink-ddl.ps1

if (-not $SkipStreamingSubmit) {
  ./scripts/windows/submit-streaming-jobs.ps1
  Write-Host "Waiting ${WarmupSeconds}s for streaming jobs and producers to materialize data..."
  Start-Sleep -Seconds $WarmupSeconds
} else {
  Write-Host "Skipping streaming job submission because -SkipStreamingSubmit was set."
}

if ($WithOps) {
  & docker @compose --profile core --profile multi-node --profile ops up -d prometheus grafana loki
}

if ($WithOlap) {
  & docker @compose --profile core --profile multi-node --profile olap --profile multi-node-olap up -d `
    starrocks starrocks-be starrocks-be-node-2 starrocks-be-node-3
}

./scripts/windows/verify-multi-node.ps1

Write-Host ""
Write-Host "Multi-node core demo is ready."
Write-Host "  Flink UI:        http://127.0.0.1:8082"
Write-Host "  Kafka external:  localhost:29092"
if ($WithOps) {
  Write-Host "  Prometheus:      http://127.0.0.1:19090"
}
if ($WithOlap) {
  Write-Host "  StarRocks FE:    http://127.0.0.1:8030"
}
