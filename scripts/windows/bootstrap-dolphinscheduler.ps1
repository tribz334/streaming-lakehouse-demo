param(
  [string]$BaseUrl = 'http://127.0.0.1:12345/dolphinscheduler',
  [string]$UserName = 'admin',
  [string]$Password = 'dolphinscheduler123',
  [string]$BizDate = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd'),
  [switch]$TriggerRealtime,
  [switch]$TriggerOffline
)

$ErrorActionPreference = 'Stop'
$projectName = 'ad_lakehouse'
$realtimeWorkflowName = 'realtime_ad_pipeline'
$offlineWorkflowName = 'offline_daily_pipeline'
$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession

function Invoke-DsApi {
  param(
    [Parameter(Mandatory = $true)][ValidateSet('GET','POST','PUT','DELETE')][string]$Method,
    [Parameter(Mandatory = $true)][string]$Path,
    [hashtable]$Query = @{}
  )
  $uri = "$BaseUrl$Path"
  if ($Method -in @('GET','DELETE')) {
    $pairs = foreach ($key in $Query.Keys) {
      '{0}={1}' -f [uri]::EscapeDataString($key), [uri]::EscapeDataString([string]$Query[$key])
    }
    if ($pairs.Count -gt 0) { $uri += '?' + ($pairs -join '&') }
    $response = Invoke-RestMethod -Method $Method -Uri $uri -WebSession $session
  } else {
    $response = Invoke-RestMethod -Method $Method -Uri $uri -Body $Query `
      -WebSession $session -ContentType 'application/x-www-form-urlencoded'
  }
  if ($response.code -ne 0) { throw "DolphinScheduler API failed: $($response.msg) [$Path]" }
  return $response.data
}

function New-TaskSpec {
  param(
    [string]$Name,
    [string]$Description,
    [string]$Command,
    [int]$X,
    [int]$Y,
    [int]$Timeout = 900
  )
  [ordered]@{
    name = $Name; description = $Description; command = $Command
    x = $X; y = $Y; timeout = $Timeout
  }
}

function Set-WorkflowDefinition {
  param(
    [string]$ProjectCode,
    [string]$Name,
    [string]$Description,
    [object[]]$Tasks,
    [object[]]$Edges,
    [string]$GlobalParams = '[]'
  )

  $codes = @(Invoke-DsApi GET "/projects/$ProjectCode/task-definition/gen-task-codes" @{ genNum = $Tasks.Count })
  if ($codes.Count -ne $Tasks.Count) {
    throw "Expected $($Tasks.Count) task codes for $Name, got $($codes.Count)."
  }

  $codeByName = @{}
  $definitions = @()
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
      taskType = 'SHELL'
      taskParams = [ordered]@{ localParams = @(); rawScript = $task.command; resourceList = @() }
      flag = 'YES'
      taskPriority = 'MEDIUM'
      workerGroup = 'default'
      failRetryTimes = 2
      failRetryInterval = 1
      timeoutFlag = 'OPEN'
      timeoutNotifyStrategy = 'WARN'
      timeout = [int][Math]::Ceiling($task.timeout / 60.0) # API uses minutes
    }
    $locations += [ordered]@{ taskCode = $taskCode; x = $task.x; y = $task.y }
  }

  $hasIncoming = @{}
  foreach ($edge in $Edges) { $hasIncoming[$edge.to] = $true }
  $relations = @()
  foreach ($task in $Tasks) {
    if (-not $hasIncoming.ContainsKey($task.name)) {
      $relations += [ordered]@{
        name = ''; preTaskCode = 0; preTaskVersion = 0
        postTaskCode = [long]$codeByName[$task.name]; postTaskVersion = 1
        conditionType = 'NONE'; conditionParams = @{}
      }
    }
  }
  foreach ($edge in $Edges) {
    $relations += [ordered]@{
      name = ''
      preTaskCode = [long]$codeByName[$edge.from]; preTaskVersion = 1
      postTaskCode = [long]$codeByName[$edge.to]; postTaskVersion = 1
      conditionType = 'NONE'; conditionParams = @{}
    }
  }

  $payload = @{
    name = $Name
    description = $Description
    globalParams = $GlobalParams
    locations = ($locations | ConvertTo-Json -Compress -Depth 6)
    timeout = 0
    taskRelationJson = ($relations | ConvertTo-Json -Compress -Depth 8)
    taskDefinitionJson = ($definitions | ConvertTo-Json -Compress -Depth 12)
    executionType = 'SERIAL_WAIT'
  }

  $page = Invoke-DsApi GET "/projects/$ProjectCode/workflow-definition" @{
    pageNo = 1; pageSize = 100; searchVal = $Name
  }
  $existing = @($page.totalList | Where-Object { $_.name -eq $Name }) | Select-Object -First 1
  if ($existing) {
    $workflowCode = [string]$existing.code
    $schedulePage = Invoke-DsApi GET "/projects/$ProjectCode/schedules" @{
      workflowDefinitionCode = $workflowCode; pageNo = 1; pageSize = 100
    }
    foreach ($schedule in @($schedulePage.totalList)) {
      if ($schedule.releaseState -eq 'ONLINE') {
        Invoke-DsApi POST "/projects/$ProjectCode/schedules/$($schedule.id)/offline" @{} | Out-Null
      }
    }
    Invoke-DsApi POST "/projects/$ProjectCode/workflow-definition/$workflowCode/release" @{
      releaseState = 'OFFLINE'; name = $Name
    } | Out-Null
    $payload.releaseState = 'OFFLINE'
    Invoke-DsApi PUT "/projects/$ProjectCode/workflow-definition/$workflowCode" $payload | Out-Null
    Write-Host "Updated workflow: $Name ($workflowCode)"
  } else {
    $created = Invoke-DsApi POST "/projects/$ProjectCode/workflow-definition" $payload
    $workflowCode = [string]$created.code
    Write-Host "Created workflow: $Name ($workflowCode)"
  }
  Invoke-DsApi POST "/projects/$ProjectCode/workflow-definition/$workflowCode/release" @{
    releaseState = 'ONLINE'; name = $Name
  } | Out-Null
  return $workflowCode
}

function Enable-Schedule {
  param([string]$ProjectCode,[string]$WorkflowCode,[string]$Cron)
  $page = Invoke-DsApi GET "/projects/$ProjectCode/schedules" @{
    workflowDefinitionCode = $WorkflowCode; pageNo = 1; pageSize = 20
  }
  $schedule = @($page.totalList) | Select-Object -First 1
  if (-not $schedule) {
    $scheduleJson = [ordered]@{
      startTime = (Get-Date).ToString('yyyy-MM-dd 00:00:00')
      endTime = '2099-12-31 23:59:59'
      crontab = $Cron
      timezoneId = 'Asia/Shanghai'
    } | ConvertTo-Json -Compress
    $schedule = Invoke-DsApi POST "/projects/$ProjectCode/schedules" @{
      workflowDefinitionCode = $WorkflowCode
      schedule = $scheduleJson
      warningType = 'FAILURE'
      warningGroupId = 0
      failureStrategy = 'END'
      workerGroup = 'default'
      tenantCode = 'default'
      workflowInstancePriority = 'MEDIUM'
    }
  }
  Invoke-DsApi POST "/projects/$ProjectCode/schedules/$($schedule.id)/online" @{} | Out-Null
  Write-Host "Schedule online: workflow=$WorkflowCode cron=$Cron"
}

function Start-Workflow {
  param([string]$ProjectCode,[string]$WorkflowCode,[hashtable]$Parameters = @{})
  $startParams = if ($Parameters.Count -gt 0) {
    $Parameters | ConvertTo-Json -Compress
  } else { '{}' }
  Invoke-DsApi POST "/projects/$ProjectCode/executors/start-workflow-instance" @{
    workflowDefinitionCode = $WorkflowCode
    scheduleTime = ''
    failureStrategy = 'END'
    warningType = 'NONE'
    workflowInstancePriority = 'MEDIUM'
    workerGroup = 'default'
    tenantCode = 'default'
    execType = 'START_PROCESS'
    runMode = 'RUN_MODE_SERIAL'
    startParams = $startParams
    dryRun = 0
  } | Out-Null
  Write-Host "Workflow execution requested: $WorkflowCode params=$startParams"
}

$runner = 'cd /workspace/project && bash scripts/linux/'
$realtimeTasks = @(
  (New-TaskSpec '检查基础服务' 'Validate MySQL, Fluss, Flink and StarRocks containers' ($runner + 'ds-realtime-task.sh check_core') 80 340 300),
  (New-TaskSpec '初始化 Fluss 表' 'Create current ODS, DIM, DWD, DWS, DM and ADS tables' ($runner + 'ds-realtime-task.sh prepare_tables') 320 340 1800),
  (New-TaskSpec '启动 Paimon Tiering' 'Start the existing Fluss lake tiering service idempotently' ($runner + 'ds-realtime-task.sh start_tiering') 580 80 600),
  (New-TaskSpec 'MySQL CDC 同步 DIM 与账单订单贴源数据' 'Run the existing CDC StatementSet; skip if already running' ($runner + 'ds-realtime-task.sh start_mysql_cdc') 580 300 600),
  (New-TaskSpec 'SDK 埋点日志采集' 'Ensure the existing SDK event generator is running' ($runner + 'ds-realtime-task.sh start_sdk_collection') 580 540 300),
  (New-TaskSpec '生成广告事件明细' 'Submit DwdLogDataStreamJob idempotently' ($runner + 'ds-realtime-task.sh start_dwd_event') 850 540 600),
  (New-TaskSpec '生成广告账单明细' 'Submit DwdAdBillJob with shared creative lookup' ($runner + 'ds-realtime-task.sh start_dwd_bill') 850 180 600),
  (New-TaskSpec 'Last Click 订单归因' 'Submit DwdOrderAttributionJob without changing uid and product attribution rules' ($runner + 'ds-realtime-task.sh start_attribution') 1110 440 600),
  (New-TaskSpec '生成 10 s 实时 DWS' 'Submit DwsAdCreativeJob idempotently' ($runner + 'ds-realtime-task.sh start_realtime_dws') 1380 340 600),
  (New-TaskSpec '验证 Paimon 与 StarRocks 实时结果' 'Verify five streaming jobs, tiered tables and StarRocks serving rows' ($runner + 'ds-realtime-task.sh verify_outputs') 1680 340 900)
)
$realtimeEdges = @(
  @{from='检查基础服务';to='初始化 Fluss 表'},
  @{from='初始化 Fluss 表';to='启动 Paimon Tiering'},
  @{from='初始化 Fluss 表';to='MySQL CDC 同步 DIM 与账单订单贴源数据'},
  @{from='初始化 Fluss 表';to='SDK 埋点日志采集'},
  @{from='SDK 埋点日志采集';to='生成广告事件明细'},
  @{from='MySQL CDC 同步 DIM 与账单订单贴源数据';to='生成广告账单明细'},
  @{from='MySQL CDC 同步 DIM 与账单订单贴源数据';to='Last Click 订单归因'},
  @{from='生成广告事件明细';to='Last Click 订单归因'},
  @{from='生成广告账单明细';to='生成 10 s 实时 DWS'},
  @{from='生成广告事件明细';to='生成 10 s 实时 DWS'},
  @{from='Last Click 订单归因';to='生成 10 s 实时 DWS'},
  @{from='启动 Paimon Tiering';to='验证 Paimon 与 StarRocks 实时结果'},
  @{from='生成 10 s 实时 DWS';to='验证 Paimon 与 StarRocks 实时结果'}
)

$offlineTasks = @(
  (New-TaskSpec '检查 Paimon DWD 输入' 'Validate current Paimon DWD tables and biz_date' ($runner + "ds-offline-task.sh check_inputs '`${biz_date}'") 100 300 300),
  (New-TaskSpec '生成日级 DWS 主题汇总' 'Execute flink/sql/04_daily_dws.sql with partition overwrite' ($runner + "ds-offline-task.sh run_dws '`${biz_date}'") 520 300 7200),
  (New-TaskSpec 'offline_dm' 'Execute unchanged DM statements from flink/sql/10_daily_offline.sql' ($runner + "ds-offline-task.sh run_dm '`${biz_date}'") 960 300 7200),
  (New-TaskSpec 'offline_ads' 'Execute unchanged ADS statements from flink/sql/10_daily_offline.sql' ($runner + "ds-offline-task.sh run_ads '`${biz_date}'") 1200 300 7200),
  (New-TaskSpec '验证 Paimon 与 StarRocks 离线结果' 'Validate DWS, DM, offline metric, attribution and retention rows' ($runner + "ds-offline-task.sh verify_outputs '`${biz_date}'") 1420 300 900)
)
$offlineEdges = @(
  @{from='检查 Paimon DWD 输入';to='生成日级 DWS 主题汇总'},
  @{from='生成日级 DWS 主题汇总';to='offline_dm'},
  @{from='offline_dm';to='offline_ads'},
  @{from='offline_ads';to='验证 Paimon 与 StarRocks 离线结果'}
)
$offlineGlobalParams = ConvertTo-Json -Compress -InputObject @(
  [ordered]@{ prop='biz_date'; direct='IN'; type='VARCHAR'; value='$[yyyy-MM-dd-1]' }
)

Write-Host 'Logging in to DolphinScheduler...'
Invoke-DsApi POST '/login' @{ userName = $UserName; userPassword = $Password } | Out-Null
$projects = Invoke-DsApi GET '/projects' @{ pageNo = 1; pageSize = 100; searchVal = $projectName }
$project = @($projects.totalList | Where-Object { $_.name -eq $projectName }) | Select-Object -First 1
if (-not $project) {
  $project = Invoke-DsApi POST '/projects' @{
    projectName = $projectName
    description = 'USTC advertising streaming lakehouse thesis demo'
  }
}
$projectCode = [string]$project.code

$realtimeCode = Set-WorkflowDefinition $projectCode $realtimeWorkflowName `
  'Persistent collection, Fluss DIM/DWD, Last Click, 10 s DWS and serving verification' `
  $realtimeTasks $realtimeEdges
$offlineCode = Set-WorkflowDefinition $projectCode $offlineWorkflowName `
  'Parameterized Paimon DWD to DWS, DM and ADS batch workflow' `
  $offlineTasks $offlineEdges $offlineGlobalParams
Enable-Schedule $projectCode $offlineCode '0 0 2 * * ?'

if ($TriggerRealtime) { Start-Workflow $projectCode $realtimeCode }
if ($TriggerOffline) { Start-Workflow $projectCode $offlineCode @{ biz_date = $BizDate } }

Write-Host "Registered: $realtimeWorkflowName code=$realtimeCode schedule=manual"
Write-Host "Registered: $offlineWorkflowName code=$offlineCode schedule=02:00 Asia/Shanghai"
Write-Host "Open $BaseUrl/ui/"
