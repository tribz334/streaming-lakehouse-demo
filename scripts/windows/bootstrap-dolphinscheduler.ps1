param(
  [string]$BaseUrl = "http://127.0.0.1:12345/dolphinscheduler",
  [string]$UserName = "admin",
  [string]$Password = "dolphinscheduler123",
  [switch]$NoTrigger,
  [switch]$TriggerOffline,
  [switch]$TriggerRealtime
)

$ErrorActionPreference = "Stop"
$projectName = "ustc-streaming-lakehouse-demo"
$offlineWorkflowName = "ad_lakehouse_daily_offline"
$realtimeWorkflowName = "ad_lakehouse_realtime_operations"
$obsoleteWorkflowNames = @("lakehouse_component_smoke_test", "ad_lakehouse_daily_refresh")
$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession

function Invoke-DsApi {
  param(
    [Parameter(Mandatory = $true)][ValidateSet("GET", "POST", "PUT", "DELETE")][string]$Method,
    [Parameter(Mandatory = $true)][string]$Path,
    [hashtable]$Query = @{}
  )

  $uri = "$BaseUrl$Path"
  if ($Method -in @("GET", "DELETE")) {
    $pairs = foreach ($key in $Query.Keys) {
      "{0}={1}" -f [uri]::EscapeDataString($key), [uri]::EscapeDataString([string]$Query[$key])
    }
    if ($pairs.Count -gt 0) { $uri += "?" + ($pairs -join "&") }
    $response = Invoke-RestMethod -Method $Method -Uri $uri -WebSession $session
  } else {
    $response = Invoke-RestMethod -Method $Method -Uri $uri -Body $Query -WebSession $session -ContentType "application/x-www-form-urlencoded"
  }
  if ($response.code -ne 0) {
    throw "DolphinScheduler API failed: $($response.msg) [$Path]"
  }
  return $response.data
}

function New-BatchSqlCommand {
  param([Parameter(Mandatory = $true)][string]$SqlFile)

  $safeName = $SqlFile.Replace(".", "_").Replace("-", "_")
  return (@'
set -euo pipefail
JM="$(docker ps --filter label=com.docker.compose.project=ustc_lakehouse --filter label=com.docker.compose.service=flink-jobmanager -q | head -n1)"
test -n "$JM"
log="/tmp/ds___SAFE_NAME__.log"
set +e
docker exec "$JM" bash -lc "cat /opt/flink/usrlib/sql/00_catalogs_and_tables.sql /opt/flink/usrlib/sql/01_model_tables.sql /opt/flink/usrlib/sql/__SQL_FILE__ > /tmp/ds___SAFE_NAME__ && /opt/flink/bin/sql-client.sh -f /tmp/ds___SAFE_NAME__" >"$log" 2>&1
status=$?
set -e
cat "$log"
test "$status" -eq 0
if grep -Fq '[ERROR]' "$log"; then
  echo "Flink SQL Client reported a statement error." >&2
  exit 1
fi
'@).Replace("__SQL_FILE__", $SqlFile).Replace("__SAFE_NAME__", $safeName)
}

function New-StreamingSqlCommand {
  param([Parameter(Mandatory = $true)][string]$SqlFile)

  $safeName = $SqlFile.Replace(".", "_").Replace("-", "_")
  return (@'
set -euo pipefail
JM="$(docker ps --filter label=com.docker.compose.project=ustc_lakehouse --filter label=com.docker.compose.service=flink-jobmanager -q | head -n1)"
test -n "$JM"
docker exec -d "$JM" bash -lc "cat /opt/flink/usrlib/sql/00_catalogs_and_tables.sql /opt/flink/usrlib/sql/01_model_tables.sql /opt/flink/usrlib/sql/__SQL_FILE__ > /tmp/ds___SAFE_NAME__ && nohup /opt/flink/bin/sql-client.sh -f /tmp/ds___SAFE_NAME__ > /tmp/ds___SAFE_NAME__.log 2>&1"
sleep 8
'@).Replace("__SQL_FILE__", $SqlFile).Replace("__SAFE_NAME__", $safeName)
}

function New-TaskSpec {
  param(
    [string]$Name,
    [string]$Description,
    [string]$Script,
    [int]$X,
    [int]$Y
  )
  return [ordered]@{ name = $Name; description = $Description; script = $Script; x = $X; y = $Y }
}

function Set-WorkflowDefinition {
  param(
    [string]$ProjectCode,
    [string]$Name,
    [string]$Description,
    [object[]]$Tasks,
    [object[]]$Edges,
    [string]$ExecutionType = "SERIAL_WAIT"
  )

  $codes = @(Invoke-DsApi GET "/projects/$ProjectCode/task-definition/gen-task-codes" @{ genNum = $Tasks.Count })
  if ($codes.Count -ne $Tasks.Count) {
    throw "Expected $($Tasks.Count) task codes for $Name, got $($codes.Count)."
  }

  $codeByName = @{}
  $definitions = @()
  # DolphinScheduler UI expects a location array. A code-keyed object is
  # accepted by the API but cannot be rendered by the DAG canvas.
  $locations = @()
  for ($i = 0; $i -lt $Tasks.Count; $i++) {
    $task = $Tasks[$i]
    $taskCode = [long]$codes[$i]
    $codeByName[$task.name] = $taskCode
    $definitions += [ordered]@{
      code = $taskCode
      name = $task.name
      version = 1
      description = $task.description
      delayTime = 0
      taskType = "SHELL"
      taskParams = [ordered]@{ localParams = @(); rawScript = $task.script; resourceList = @() }
      flag = "YES"
      taskPriority = "MEDIUM"
      workerGroup = "default"
      failRetryTimes = 1
      failRetryInterval = 1
      timeoutFlag = "OPEN"
      timeoutNotifyStrategy = "WARN"
      timeout = 7200
    }
    $locations += [ordered]@{ taskCode = $taskCode; x = $task.x; y = $task.y }
  }

  $hasIncoming = @{}
  foreach ($edge in $Edges) { $hasIncoming[$edge.to] = $true }
  $relations = @()
  foreach ($task in $Tasks) {
    if (-not $hasIncoming.ContainsKey($task.name)) {
      $relations += [ordered]@{
        name = ""; preTaskCode = 0; preTaskVersion = 0
        postTaskCode = [long]$codeByName[$task.name]; postTaskVersion = 1
        conditionType = "NONE"; conditionParams = @{}
      }
    }
  }
  foreach ($edge in $Edges) {
    $relations += [ordered]@{
      name = ""
      preTaskCode = [long]$codeByName[$edge.from]; preTaskVersion = 1
      postTaskCode = [long]$codeByName[$edge.to]; postTaskVersion = 1
      conditionType = "NONE"; conditionParams = @{}
    }
  }

  $payload = @{
    name = $Name
    description = $Description
    globalParams = "[]"
    locations = ($locations | ConvertTo-Json -Compress -Depth 6)
    timeout = 0
    taskRelationJson = ($relations | ConvertTo-Json -Compress -Depth 8)
    taskDefinitionJson = ($definitions | ConvertTo-Json -Compress -Depth 12)
    executionType = $ExecutionType
  }

  $page = Invoke-DsApi GET "/projects/$ProjectCode/workflow-definition" @{
    pageNo = 1; pageSize = 100; searchVal = $Name
  }
  $existing = @($page.totalList | Where-Object { $_.name -eq $Name }) | Select-Object -First 1
  if ($existing) {
    $workflowCode = [string]$existing.code
    # A definition must be offline before its DAG can be updated. Take any
    # attached schedules offline first; Enable-DailySchedule restores them.
    $schedulePage = Invoke-DsApi GET "/projects/$ProjectCode/schedules" @{
      workflowDefinitionCode = $workflowCode; pageNo = 1; pageSize = 100
    }
    foreach ($schedule in @($schedulePage.totalList)) {
      if ($schedule.releaseState -eq "ONLINE") {
        Invoke-DsApi POST "/projects/$ProjectCode/schedules/$($schedule.id)/offline" @{} | Out-Null
      }
    }
    Invoke-DsApi POST "/projects/$ProjectCode/workflow-definition/$workflowCode/release" @{
      releaseState = "OFFLINE"; name = $Name
    } | Out-Null
    $payload.releaseState = "OFFLINE"
    Invoke-DsApi PUT "/projects/$ProjectCode/workflow-definition/$workflowCode" $payload | Out-Null
    Write-Host "Updated workflow DAG: $Name ($workflowCode)"
  } else {
    $created = Invoke-DsApi POST "/projects/$ProjectCode/workflow-definition" $payload
    $workflowCode = [string]$created.code
    Write-Host "Created workflow: $Name ($workflowCode)"
  }

  Invoke-DsApi POST "/projects/$ProjectCode/workflow-definition/$workflowCode/release" @{
    releaseState = "ONLINE"; name = $Name
  } | Out-Null
  return $workflowCode
}

function Enable-DailySchedule {
  param([string]$ProjectCode, [string]$WorkflowCode)

  $page = Invoke-DsApi GET "/projects/$ProjectCode/schedules" @{
    workflowDefinitionCode = $WorkflowCode; pageNo = 1; pageSize = 20
  }
  $schedule = @($page.totalList) | Select-Object -First 1
  if (-not $schedule) {
    $scheduleJson = [ordered]@{
      startTime = (Get-Date).ToString("yyyy-MM-dd 00:00:00")
      endTime = "2099-12-31 23:59:59"
      crontab = "0 0 2 * * ?"
      timezoneId = "Asia/Shanghai"
    } | ConvertTo-Json -Compress
    $schedule = Invoke-DsApi POST "/projects/$ProjectCode/schedules" @{
      workflowDefinitionCode = $WorkflowCode
      schedule = $scheduleJson
      warningType = "FAILURE"
      warningGroupId = 0
      failureStrategy = "END"
      workerGroup = "default"
      tenantCode = "default"
      workflowInstancePriority = "MEDIUM"
    }
    Write-Host "Created daily 02:00 schedule for workflow $WorkflowCode"
  }
  Invoke-DsApi POST "/projects/$ProjectCode/schedules/$($schedule.id)/online" @{} | Out-Null
  Write-Host "Offline schedule is online: scheduleId=$($schedule.id) cron=0 0 2 * * ?"
}

function Start-Workflow {
  param([string]$ProjectCode, [string]$WorkflowCode)
  Invoke-DsApi POST "/projects/$ProjectCode/executors/start-workflow-instance" @{
    workflowDefinitionCode = $WorkflowCode
    scheduleTime = ""
    failureStrategy = "END"
    warningType = "NONE"
    workflowInstancePriority = "MEDIUM"
    workerGroup = "default"
    tenantCode = "default"
    execType = "START_PROCESS"
    runMode = "RUN_MODE_SERIAL"
    startParams = "{}"
    dryRun = 0
  } | Out-Null
  Write-Host "Workflow execution requested: $WorkflowCode"
}

Write-Host "Logging in to DolphinScheduler..."
Invoke-DsApi POST "/login" @{ userName = $UserName; userPassword = $Password } | Out-Null

$projects = Invoke-DsApi GET "/projects" @{ pageNo = 1; pageSize = 100; searchVal = $projectName }
$project = @($projects.totalList | Where-Object { $_.name -eq $projectName }) | Select-Object -First 1
if (-not $project) {
  $project = Invoke-DsApi POST "/projects" @{
    projectName = $projectName
    description = "USTC streaming lakehouse thesis demo"
  }
}
$projectCode = [string]$project.code

# Remove the earlier smoke-test/one-workflow placeholders; the thesis defines
# two business workflows with different lifecycle and scheduling semantics.
foreach ($obsoleteName in $obsoleteWorkflowNames) {
  $page = Invoke-DsApi GET "/projects/$projectCode/workflow-definition" @{
    pageNo = 1; pageSize = 100; searchVal = $obsoleteName
  }
  $obsolete = @($page.totalList | Where-Object { $_.name -eq $obsoleteName }) | Select-Object -First 1
  if ($obsolete) {
    Invoke-DsApi POST "/projects/$projectCode/workflow-definition/$($obsolete.code)/release" @{
      releaseState = "OFFLINE"; name = $obsoleteName
    } | Out-Null
    Invoke-DsApi DELETE "/projects/$projectCode/workflow-definition/$($obsolete.code)" | Out-Null
    Write-Host "Removed obsolete workflow: $obsoleteName"
  }
}

$prepareCatalogs = @'
set -euo pipefail
JM="$(docker ps --filter label=com.docker.compose.project=ustc_lakehouse --filter label=com.docker.compose.service=flink-jobmanager -q | head -n1)"
test -n "$JM"
docker exec "$JM" bash -lc "/opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/00_catalogs_and_tables.sql"
docker exec "$JM" bash -lc "/opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/01_model_tables.sql"
'@

$verifyCdcDimensions = @'
set -euo pipefail
overview=$(curl -fsS http://flink-jobmanager:8081/jobs/overview)
printf '%s' "$overview" | jq -e '.jobs[] | select(.name == "mysql-cdc-to-paimon" and .state == "RUNNING")' >/dev/null
'@

$offlineReceipt = @'
set -euo pipefail
code=$(curl -sS -o /dev/null -w '%{http_code}' http://starrocks:8030/)
test "$code" = '401' -o "$code" = '200'
mkdir -p /workspace/dolphinscheduler/runs
printf 'offline workflow completed at %s; StarRocks HTTP=%s\n' "$(date -Iseconds)" "$code" > /workspace/dolphinscheduler/runs/offline-workflow-execution.txt
cat /workspace/dolphinscheduler/runs/offline-workflow-execution.txt
'@

$offlineTasks = @(
  (New-TaskSpec "prepare_catalogs" "Initialize Flink/Paimon catalogs and thesis tables" $prepareCatalogs 80 220),
  (New-TaskSpec "ods_snapshot_check" "Validate the bounded latest ODS Paimon snapshot" (New-BatchSqlCommand "06_offline_ods_check.sql") 300 100),
  (New-TaskSpec "verify_cdc_dimensions" "Verify that MySQL CDC continuously maintains DIM tables" $verifyCdcDimensions 300 340),
  (New-TaskSpec "dws_theme_load" "Build the streamlined offline DWS layer" (New-BatchSqlCommand "08_offline_dws.sql") 820 220),
  (New-TaskSpec "dm_layer_load" "Build attribution and anti-fraud DM features" (New-BatchSqlCommand "09_offline_dm.sql") 1080 340),
  (New-TaskSpec "ads_retention" "Calculate advertiser retention ADS" (New-BatchSqlCommand "10_ads_retention.sql") 1320 60),
  (New-TaskSpec "ads_attribution" "Calculate 30-day last-click attribution ADS" (New-BatchSqlCommand "11_ads_attribution.sql") 1320 220),
  (New-TaskSpec "ads_antifraud" "Calculate anti-fraud signal ADS" (New-BatchSqlCommand "12_ads_fraud.sql") 1320 420),
  (New-TaskSpec "ads_creative_offline" "Build the creative-grain offline BI serving dataset" (New-BatchSqlCommand "13_ads_creative_offline.sql") 1320 580),
  (New-TaskSpec "publish_offline_receipt" "Confirm StarRocks reachability and write the offline receipt" $offlineReceipt 1820 220)
)
$offlineEdges = @(
  [ordered]@{ from = "prepare_catalogs"; to = "ods_snapshot_check" },
  [ordered]@{ from = "prepare_catalogs"; to = "verify_cdc_dimensions" },
  [ordered]@{ from = "ods_snapshot_check"; to = "dws_theme_load" },
  [ordered]@{ from = "verify_cdc_dimensions"; to = "dws_theme_load" },
  [ordered]@{ from = "dws_theme_load"; to = "dm_layer_load" },
  [ordered]@{ from = "ods_snapshot_check"; to = "ads_retention" },
  [ordered]@{ from = "verify_cdc_dimensions"; to = "ads_retention" },
  [ordered]@{ from = "dm_layer_load"; to = "ads_attribution" },
  [ordered]@{ from = "dm_layer_load"; to = "ads_antifraud" },
  [ordered]@{ from = "dws_theme_load"; to = "ads_creative_offline" },
  [ordered]@{ from = "ads_retention"; to = "publish_offline_receipt" },
  [ordered]@{ from = "ads_attribution"; to = "publish_offline_receipt" },
  [ordered]@{ from = "ads_antifraud"; to = "publish_offline_receipt" },
  [ordered]@{ from = "ads_creative_offline"; to = "publish_offline_receipt" }
)

$stopRealtimeJobs = @'
set -euo pipefail
ids=$(curl -fsS http://flink-jobmanager:8081/jobs/overview | jq -r '.jobs[] | select(.state == "RUNNING" or .state == "CREATED" or .state == "RESTARTING") | .jid')
for id in $ids; do
  curl -fsS -X PATCH "http://flink-jobmanager:8081/jobs/${id}?mode=cancel" >/dev/null
  echo "cancel requested: $id"
done
sleep 5
'@

$verifyRealtimeJobs = @'
set -euo pipefail
sleep 10
overview=$(curl -fsS http://flink-jobmanager:8081/jobs/overview)
running=$(printf '%s' "$overview" | jq '[.jobs[] | select(.state == "RUNNING")] | length')
test "$running" -ge 5
mkdir -p /workspace/dolphinscheduler/runs
printf 'realtime workflow completed at %s; running_jobs=%s\n' "$(date -Iseconds)" "$running" > /workspace/dolphinscheduler/runs/realtime-workflow-execution.txt
cat /workspace/dolphinscheduler/runs/realtime-workflow-execution.txt
'@

$prepareRealtimeResources = @'
set -euo pipefail
KAFKA="$(docker ps --filter label=com.docker.compose.project=ustc_lakehouse --filter label=com.docker.compose.service=kafka-node-1 -q | head -n1)"
test -n "$KAFKA"
docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic ods_log --partitions 3 --replication-factor 1
docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic dws_ad_metric_stream_10s_sr --partitions 3 --replication-factor 1
'@

$startMysqlCdc = @'
set -euo pipefail
JM="$(docker ps --filter label=com.docker.compose.project=ustc_lakehouse --filter label=com.docker.compose.service=flink-jobmanager -q | head -n1)"
test -n "$JM"
docker exec -d "$JM" bash -lc "nohup /opt/flink-cdc/bin/flink-cdc.sh /opt/flink-cdc/pipelines/mysql-to-paimon.yaml --flink-home /opt/flink -t remote -Drest.address=flink-jobmanager -Drest.port=8081 >/tmp/mysql-cdc-to-paimon.log 2>&1 &"
for i in $(seq 1 40); do
  if curl -fsS http://flink-jobmanager:8081/jobs/overview | jq -e '.jobs[] | select(.name == "mysql-cdc-to-paimon" and .state == "RUNNING")' >/dev/null; then
    exit 0
  fi
  sleep 3
done
docker exec "$JM" bash -lc "tail -n 120 /tmp/mysql-cdc-to-paimon.log"
exit 1
'@

$realtimeTasks = @(
  (New-TaskSpec "stop_existing_stream_jobs" "Stop current streaming jobs for release or schema evolution" $stopRealtimeJobs 100 220),
  (New-TaskSpec "prepare_realtime_resources" "Ensure the Kafka ODS topic exists before starting ingestion" $prepareRealtimeResources 320 220),
  (New-TaskSpec "start_ods_stream" "Start Kafka-to-Paimon ODS streaming ingestion" (New-StreamingSqlCommand "02_realtime_ods.sql") 560 100),
  (New-TaskSpec "start_mysql_cdc" "Start MySQL snapshot and binlog synchronization into Paimon" $startMysqlCdc 560 340),
  (New-TaskSpec "start_dwd_stream" "Start ODS-to-DWD streaming enrichment" (New-StreamingSqlCommand "03_realtime_dwd.sql") 820 220),
  (New-TaskSpec "start_dws_metric_stream" "Start ten-second operational DWS aggregation" (New-StreamingSqlCommand "04_realtime_dws_metrics.sql") 1080 220),
  (New-TaskSpec "start_starrocks_metric_relay" "Continuously relay the operational DWS changelog to StarRocks" (New-StreamingSqlCommand "05_realtime_starrocks_relay.sql") 1340 220),
  (New-TaskSpec "verify_realtime_jobs" "Verify CDC and four persistent real-time jobs and write the receipt" $verifyRealtimeJobs 1600 220)
)
$realtimeEdges = @(
  [ordered]@{ from = "stop_existing_stream_jobs"; to = "prepare_realtime_resources" },
  [ordered]@{ from = "prepare_realtime_resources"; to = "start_ods_stream" },
  [ordered]@{ from = "prepare_realtime_resources"; to = "start_mysql_cdc" },
  [ordered]@{ from = "start_ods_stream"; to = "start_dwd_stream" },
  [ordered]@{ from = "start_mysql_cdc"; to = "start_dwd_stream" },
  [ordered]@{ from = "start_dwd_stream"; to = "start_dws_metric_stream" },
  [ordered]@{ from = "start_dws_metric_stream"; to = "start_starrocks_metric_relay" },
  [ordered]@{ from = "start_starrocks_metric_relay"; to = "verify_realtime_jobs" }
)

$offlineCode = Set-WorkflowDefinition $projectCode $offlineWorkflowName "Daily 02:00 bounded ODS/DIM/DWD/DWS/DM/ADS load" $offlineTasks $offlineEdges "SERIAL_WAIT"
$realtimeCode = Set-WorkflowDefinition $projectCode $realtimeWorkflowName "Manual stop-and-restart operations for persistent ODS/DIM/DWD/DWS streaming jobs" $realtimeTasks $realtimeEdges "SERIAL_WAIT"

Enable-DailySchedule $projectCode $offlineCode

if (-not $NoTrigger) {
  if ($TriggerOffline) { Start-Workflow $projectCode $offlineCode }
  if ($TriggerRealtime) { Start-Workflow $projectCode $realtimeCode }
}

Write-Host "Registered thesis workflows:"
Write-Host "  offline=$offlineWorkflowName code=$offlineCode schedule=02:00 Asia/Shanghai"
Write-Host "  realtime=$realtimeWorkflowName code=$realtimeCode schedule=manual"
Write-Host "Open $BaseUrl/ui/"
