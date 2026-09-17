param(
  [switch]$SkipStreamingSubmit,
  [int]$WarmupSeconds=20,
  [switch]$WithBi,
  [switch]$WithScheduler,
  [switch]$RebuildRuntimeSchema
)
$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root
docker info | Out-Null
& (Join-Path $PSScriptRoot "download-flink-jars.ps1")
docker compose build flink-jobmanager event-generator
if ($LASTEXITCODE -ne 0) { throw "Docker image build failed" }
docker compose up -d mysql zookeeper fluss-coordinator fluss-tablet starrocks starrocks-be flink-jobmanager flink-taskmanager
if ($LASTEXITCODE -ne 0) { throw "Core service startup failed" }
& (Join-Path $PSScriptRoot "apply-mysql-migrations.ps1")
if ($RebuildRuntimeSchema) {
  & (Join-Path $PSScriptRoot "stop-flink-jobs.ps1")
  $migrationSucceeded = $false
  foreach ($attempt in 1..3) {
    $migrationOutput = docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh `
      -f "/opt/flink/usrlib/sql/12_migrate_classification_metrics.sql" 2>&1
    $migrationOutput
    if ($LASTEXITCODE -eq 0 -and ($migrationOutput | Out-String) -notmatch "\[ERROR\]") {
      $migrationSucceeded = $true
      break
    }
    $migrationText = $migrationOutput | Out-String
    if ($attempt -eq 1 -and $migrationText -match "Paimon catalog" `
        -and $migrationText -match "schema is not compatible") {
      docker compose stop fluss-tablet fluss-coordinator
      if ($LASTEXITCODE -ne 0) { throw "Failed to pause Fluss before stale Paimon cleanup" }

      $paimonDb = (Resolve-Path (Join-Path $root "runtime-data\warehouse\paimon\ad_dw.db")).Path
      $staleDwdTables = @(
        "dwd_ad_event_di", "dwd_ad_bill_di", "dwd_order_acc",
        "dwd_ad_event_dirty_di", "dwd_ad_order_di", "dwd_ad_order_event_di"
      )
      foreach ($tableName in $staleDwdTables) {
        $candidate = Join-Path $paimonDb $tableName
        if (Test-Path -LiteralPath $candidate) {
          $resolved = (Resolve-Path -LiteralPath $candidate).Path
          if (-not $resolved.StartsWith($paimonDb + [IO.Path]::DirectorySeparatorChar)) {
            throw "Refusing to remove path outside the Paimon ad_dw database: $resolved"
          }
          Remove-Item -LiteralPath $resolved -Recurse -Force
          Write-Host "Removed stale Paimon table directory: $tableName"
        }
      }
      docker compose up -d fluss-coordinator fluss-tablet
      if ($LASTEXITCODE -ne 0) { throw "Failed to restart Fluss after stale Paimon cleanup" }
      Start-Sleep -Seconds 8
    }
    if ($attempt -lt 3) {
      Write-Warning "Runtime schema rebuild attempt $attempt hit an asynchronous Fluss/Paimon cleanup race; retrying."
      Start-Sleep -Seconds 5
    }
  }
  if (-not $migrationSucceeded) {
    throw "Fluss/Paimon runtime schema rebuild failed after 3 attempts"
  }
}
& (Join-Path $PSScriptRoot "init-flink-ddl.ps1")
& (Join-Path $PSScriptRoot "init-starrocks.ps1")
if (-not $SkipStreamingSubmit) {
  & (Join-Path $PSScriptRoot "submit-streaming-jobs.ps1")
}
docker compose up -d event-generator
if ($LASTEXITCODE -ne 0) { throw "Event generator startup failed" }
if ($WarmupSeconds -gt 0) { Start-Sleep -Seconds $WarmupSeconds }
if ($WithBi) {
  docker compose --profile bi up -d superset
  if ($LASTEXITCODE -ne 0) { throw "Superset startup failed" }
}
if ($WithScheduler) {
  docker compose --profile scheduler up -d --build dolphinscheduler
  if ($LASTEXITCODE -ne 0) { throw "DolphinScheduler startup failed" }
}
Write-Host "Ready: Flink 1.20.3 http://127.0.0.1:18082, Fluss localhost:19123, StarRocks http://127.0.0.1:18030"
