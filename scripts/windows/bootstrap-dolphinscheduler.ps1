param(
  [string]$BaseUrl = "http://127.0.0.1:12345/dolphinscheduler",
  [string]$UserName = "admin",
  [string]$Password = "dolphinscheduler123",
  [switch]$NoTrigger
)

$ErrorActionPreference = "Stop"
$projectName = "ustc-streaming-lakehouse-demo"
$workflowName = "lakehouse_component_smoke_test"
$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession

function Invoke-DsApi {
  param(
    [Parameter(Mandatory = $true)][ValidateSet("GET", "POST", "PUT", "DELETE")][string]$Method,
    [Parameter(Mandatory = $true)][string]$Path,
    [hashtable]$Query = @{}
  )

  $pairs = foreach ($key in $Query.Keys) {
    "{0}={1}" -f [uri]::EscapeDataString($key), [uri]::EscapeDataString([string]$Query[$key])
  }
  $uri = "$BaseUrl$Path"
  if ($pairs.Count -gt 0) { $uri += "?" + ($pairs -join "&") }
  $response = Invoke-RestMethod -Method $Method -Uri $uri -WebSession $session
  if ($response.code -ne 0) {
    throw "DolphinScheduler API failed: $($response.msg) [$Path]"
  }
  return $response.data
}

Write-Host "Logging in to DolphinScheduler..."
Invoke-DsApi POST "/login" @{
  userName = $UserName
  userPassword = $Password
} | Out-Null

$projects = Invoke-DsApi GET "/projects" @{ pageNo = 1; pageSize = 100; searchVal = $projectName }
$project = @($projects.totalList | Where-Object { $_.name -eq $projectName }) | Select-Object -First 1
if (-not $project) {
  Write-Host "Creating project $projectName..."
  $project = Invoke-DsApi POST "/projects" @{
    projectName = $projectName
    description = "USTC streaming lakehouse thesis demo"
  }
}
$projectCode = [string]$project.code

$workflowPage = Invoke-DsApi GET "/projects/$projectCode/workflow-definition" @{
  pageNo = 1
  pageSize = 100
  searchVal = $workflowName
}
$existing = @($workflowPage.totalList | Where-Object { $_.name -eq $workflowName }) | Select-Object -First 1
if ($existing) {
  $workflowCode = [string]$existing.code
  Write-Host "Workflow already exists: $workflowName ($workflowCode)"
} else {
  $codes = @(Invoke-DsApi GET "/projects/$projectCode/task-definition/gen-task-codes" @{ genNum = 3 })
  if ($codes.Count -ne 3) { throw "Expected three task codes, got $($codes.Count)." }

  $starrocksCommand = @'
set -eu
code=$(curl -sS -o /dev/null -w '%{http_code}' http://starrocks:8030/)
test "$code" = '401' -o "$code" = '200'
printf 'DolphinScheduler executed the DAG at %s; StarRocks HTTP=%s\n' "$(date -Iseconds)" "$code" > /workspace/dolphinscheduler/runs/dolphinscheduler-execution.txt
cat /workspace/dolphinscheduler/runs/dolphinscheduler-execution.txt
'@
  $commands = @(
    "set -eu`ncurl -fsS http://flink-jobmanager:8081/overview >/tmp/flink-overview.json`necho 'Flink REST API is ready'",
    "set -eu`ncurl -fsS http://prometheus:9090/-/ready`necho 'Prometheus is ready'",
    $starrocksCommand
  )
  $names = @("check_flink", "check_prometheus", "check_starrocks_and_write_receipt")
  $definitions = @()
  for ($i = 0; $i -lt 3; $i++) {
    $definitions += [ordered]@{
      code = [long]$codes[$i]
      name = $names[$i]
      version = 1
      description = "USTC lakehouse integration check"
      delayTime = 0
      taskType = "SHELL"
      taskParams = [ordered]@{ localParams = @(); rawScript = $commands[$i]; resourceList = @() }
      flag = "YES"
      taskPriority = "MEDIUM"
      workerGroup = "default"
      failRetryTimes = 1
      failRetryInterval = 1
      timeoutFlag = "CLOSE"
      timeoutNotifyStrategy = "WARN"
      timeout = 0
    }
  }

  $relations = @(
    [ordered]@{ name = ""; preTaskCode = 0; preTaskVersion = 0; postTaskCode = [long]$codes[0]; postTaskVersion = 1; conditionType = "NONE"; conditionParams = @{} },
    [ordered]@{ name = ""; preTaskCode = [long]$codes[0]; preTaskVersion = 1; postTaskCode = [long]$codes[1]; postTaskVersion = 1; conditionType = "NONE"; conditionParams = @{} },
    [ordered]@{ name = ""; preTaskCode = [long]$codes[1]; preTaskVersion = 1; postTaskCode = [long]$codes[2]; postTaskVersion = 1; conditionType = "NONE"; conditionParams = @{} }
  )
  $locations = [ordered]@{}
  for ($i = 0; $i -lt 3; $i++) {
    $locations[[string]$codes[$i]] = [ordered]@{ x = 160 + (260 * $i); y = 180 }
  }

  Write-Host "Creating workflow $workflowName..."
  $created = Invoke-DsApi POST "/projects/$projectCode/workflow-definition" @{
    name = $workflowName
    description = "DolphinScheduler-native Linux DAG checking Flink, Prometheus, and StarRocks"
    globalParams = "[]"
    locations = ($locations | ConvertTo-Json -Compress -Depth 5)
    timeout = 0
    taskRelationJson = ($relations | ConvertTo-Json -Compress -Depth 8)
    taskDefinitionJson = ($definitions | ConvertTo-Json -Compress -Depth 10)
    executionType = "SERIAL_WAIT"
  }
  $workflowCode = [string]$created.code
}

Invoke-DsApi POST "/projects/$projectCode/workflow-definition/$workflowCode/release" @{
  releaseState = "ONLINE"
  name = $workflowName
} | Out-Null
Write-Host "Workflow is online: project=$projectCode workflow=$workflowCode"

if (-not $NoTrigger) {
  Invoke-DsApi POST "/projects/$projectCode/executors/start-workflow-instance" @{
    workflowDefinitionCode = $workflowCode
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
  Write-Host "Workflow execution requested. Open $BaseUrl/ui/"
}
