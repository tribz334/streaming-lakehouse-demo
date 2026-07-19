$ErrorActionPreference = "Stop"
if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $true
}
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

try {
  docker info | Out-Null
} catch {
  throw "Docker Desktop Linux engine is not running. Start Docker Desktop first, then rerun ./scripts/windows/verify-multi-node.ps1."
}

Write-Host "Compose multi-node services:"
$compose = @("compose")
& docker @compose ps `
  mysql `
  kafka-node-1 `
  hive-metastore `
  flink-jobmanager `
  flink-taskmanager-node-1 `
  event-generator-node-1

$hmsContainer = & docker compose ps -q hive-metastore
$hmsHealth = if ([string]::IsNullOrWhiteSpace($hmsContainer)) {
  "missing"
} else {
  & docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}" $hmsContainer
}
if ($LASTEXITCODE -ne 0 -or $hmsHealth -ne "healthy") {
  throw "Hive Metastore is not healthy: $hmsHealth"
}
Write-Host "Hive Metastore thrift service is healthy on localhost:19083."

Write-Host ""
Write-Host "Flink cluster overview:"
try {
  $overview = Invoke-RestMethod -Uri "http://127.0.0.1:8082/overview" -TimeoutSec 10
  $taskManagers = Invoke-RestMethod -Uri "http://127.0.0.1:8082/taskmanagers" -TimeoutSec 10
  Write-Host ("TaskManagers={0}, slotsTotal={1}, slotsAvailable={2}, jobsRunning={3}" -f `
    $overview.taskmanagers, `
    $overview.'slots-total', `
    $overview.'slots-available', `
    $overview.'jobs-running')

  $taskManagers.taskmanagers |
    Select-Object id, path, slotsNumber, freeSlots |
    Format-Table -AutoSize

  if ([int]$overview.taskmanagers -ne 1) {
    throw "Expected exactly 1 Flink TaskManager, got $($overview.taskmanagers)."
  }
  if ($overview.'flink-version' -ne "2.2.0") {
    throw "Expected Flink 2.2.0, got $($overview.'flink-version')."
  }
  $jobs = Invoke-RestMethod -Uri "http://127.0.0.1:8082/jobs/overview" -TimeoutSec 10
  $cdc = @($jobs.jobs | Where-Object {
    $_.name -eq "mysql-cdc-to-paimon" -and $_.state -eq "RUNNING"
  })
  if ($cdc.Count -ne 1) {
    throw "Expected exactly one running mysql-cdc-to-paimon job, got $($cdc.Count)."
  }
  Write-Host "Flink 2.2.0 and MySQL CDC pipeline are running."
} catch {
  Write-Warning "Flink multi-node verification failed: $($_.Exception.Message)"
  throw
}

Write-Host ""
Write-Host "Kafka topic status:"
& docker @compose exec -T kafka-node-1 /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka-node-1:9092 `
  --describe `
  --topic ods_log

$topicDescription = & docker @compose exec -T kafka-node-1 `
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-node-1:9092 --describe --topic ods_log
if (($topicDescription -join "`n") -notmatch "ReplicationFactor:\s*1") {
  throw "Expected ods_log replication factor 1."
}
$partitionLines = @($topicDescription | Where-Object { $_ -match "Partition:" })
if ($partitionLines.Count -lt 6) {
  throw "Expected at least 6 ods_log partitions, got $($partitionLines.Count)."
}

$starrocksContainer = & docker @compose --profile olap ps -q starrocks
if (-not [string]::IsNullOrWhiteSpace($starrocksContainer)) {
  Write-Host ""
  Write-Host "StarRocks topology:"
  $backends = @(& docker @compose --profile olap exec -T starrocks bash -lc `
    "mysql -N -h127.0.0.1 -P9030 -uroot -e 'SHOW BACKENDS'")
  $backendRows = @($backends | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
  if ($backendRows.Count -ne 1) {
    throw "Expected exactly 1 StarRocks BE, got $($backendRows.Count)."
  }
  Write-Host "Frontends=1, Backends=1"

  Write-Host "StarRocks real-time metric table:"
  & docker @compose --profile olap exec -T starrocks bash -lc `
    "mysql -h127.0.0.1 -P9030 -uroot -e 'SELECT COUNT(*) AS metric_rows, MAX(window_start) AS latest_window FROM ad_ads.realtime_ad_metrics_10s'"
}

Write-Host ""
Write-Host "Producer node logs:"
foreach ($service in @("event-generator-node-1")) {
  Write-Host "[$service]"
  & docker @compose logs --tail 8 $service
}

Write-Host ""
Write-Host "Multi-node verification passed."
