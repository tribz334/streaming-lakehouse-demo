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
$compose = @("compose", "-f", "docker-compose.yml", "-f", "docker-compose.three-node.yml")
& docker @compose --profile core --profile multi-node ps `
  mysql `
  kafka-node-1 `
  kafka-node-2 `
  kafka-node-3 `
  flink-jobmanager `
  flink-taskmanager-node-1 `
  flink-taskmanager-node-2 `
  flink-taskmanager-node-3 `
  event-generator-node-1 `
  event-generator-node-2 `
  event-generator-node-3

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

  if ([int]$overview.taskmanagers -lt 3) {
    throw "Expected at least 3 Flink TaskManagers, got $($overview.taskmanagers)."
  }
} catch {
  Write-Warning "Flink multi-node verification failed: $($_.Exception.Message)"
  throw
}

Write-Host ""
Write-Host "Kafka topic status:"
& docker @compose --profile core --profile multi-node exec -T kafka-node-1 /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka-node-1:9092 `
  --describe `
  --topic ods_log

$topicDescription = & docker @compose --profile core --profile multi-node exec -T kafka-node-1 `
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-node-1:9092 --describe --topic ods_log
if (($topicDescription -join "`n") -notmatch "ReplicationFactor:\s*3") {
  throw "Expected ods_log replication factor 3."
}
$partitionLines = @($topicDescription | Where-Object { $_ -match "Partition:" })
if ($partitionLines.Count -lt 6) {
  throw "Expected at least 6 ods_log partitions, got $($partitionLines.Count)."
}

$relayTopicDescription = & docker @compose --profile core --profile multi-node exec -T kafka-node-1 `
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-node-1:9092 --describe --topic dws_ad_metric_stream_10s_sr
if (($relayTopicDescription -join "`n") -notmatch "ReplicationFactor:\s*3") {
  Write-Warning "The existing relay topic has fewer than 3 replicas. Recreate or reassign this legacy topic before a Kafka fault-injection test."
}

$starrocksContainer = & docker @compose --profile olap ps -q starrocks
if (-not [string]::IsNullOrWhiteSpace($starrocksContainer)) {
  Write-Host ""
  Write-Host "StarRocks real-time metric load:"
  $routineLoad = & docker @compose --profile olap exec -T starrocks bash -lc `
    "mysql -N -h127.0.0.1 -P9030 -uroot -e 'SHOW ROUTINE LOAD FOR ad_ads.sync_dws_ad_metric_stream_10s'"
  if (($routineLoad -join "`n") -notmatch "\sRUNNING\s") {
    throw "StarRocks metric Routine Load is not RUNNING."
  }
  & docker @compose --profile olap exec -T starrocks bash -lc `
    "mysql -h127.0.0.1 -P9030 -uroot -e 'SELECT COUNT(*) AS metric_rows, MAX(window_start) AS latest_window FROM ad_ads.realtime_ad_metrics_snapshot'"
}

Write-Host ""
Write-Host "Producer node logs:"
foreach ($service in @("event-generator-node-1", "event-generator-node-2", "event-generator-node-3")) {
  Write-Host "[$service]"
  & docker @compose --profile core --profile multi-node logs --tail 8 $service
}

Write-Host ""
Write-Host "Multi-node verification passed."
