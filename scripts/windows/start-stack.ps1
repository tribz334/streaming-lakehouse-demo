param([switch]$SkipStreamingSubmit,[int]$WarmupSeconds=20,[switch]$WithBi)
$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root
docker info | Out-Null
& (Join-Path $PSScriptRoot "download-flink-jars.ps1")
docker compose build flink-jobmanager event-generator
docker compose up -d mysql zookeeper fluss-coordinator fluss-tablet starrocks starrocks-be flink-jobmanager flink-taskmanager
& (Join-Path $PSScriptRoot "apply-mysql-migrations.ps1")
& (Join-Path $PSScriptRoot "init-starrocks.ps1")
& (Join-Path $PSScriptRoot "init-flink-ddl.ps1")
if (-not $SkipStreamingSubmit) {
  & (Join-Path $PSScriptRoot "submit-streaming-jobs.ps1")
}
docker compose up -d event-generator
if ($WarmupSeconds -gt 0) { Start-Sleep -Seconds $WarmupSeconds }
if ($WithBi) { docker compose --profile bi up -d superset }
Write-Host "Ready: Flink 1.20.3 http://127.0.0.1:18082, Fluss localhost:19123, StarRocks http://127.0.0.1:18030"
