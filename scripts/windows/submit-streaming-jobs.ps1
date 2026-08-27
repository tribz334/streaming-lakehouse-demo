$ErrorActionPreference = "Continue"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root
$running = docker compose exec -T flink-jobmanager flink list -r 2>&1 | Out-String
if ($running -notmatch "Fluss Lake Tiering Service") {
  docker compose exec -T flink-jobmanager flink run -d /opt/flink/opt/fluss-flink-tiering.jar `
    --fluss.bootstrap.servers fluss-coordinator:9123 --datalake.format paimon `
    --datalake.paimon.metastore filesystem --datalake.paimon.warehouse file:///warehouse/paimon
  if ($LASTEXITCODE -ne 0) { throw "Fluss Paimon tiering submission failed" }
}

$jobs = @(
  @{ Name="mysql-cdc-to-fluss-ods-and-dim"; File="02_database_cdc_to_fluss.sql" },
  @{ Name="fluss-bill-ods-to-dwd"; File="02b_ods_changelog_to_dwd.sql" },
  @{ Name="fluss-order-ods-to-dwd-ad-order-di"; File="02d_order_ods_to_dwd.sql" },
  @{ Name="fluss-attributed-order-to-event-di"; File="02e_attributed_order_to_event.sql" },
  @{ Name="fluss-dwd-to-dws-topic-di"; File="03_realtime_order_dws.sql" }
)
foreach ($job in $jobs) {
  if ($running -match [regex]::Escape($job.Name)) { Write-Host "$($job.Name) is already running."; continue }
  $command = "nohup /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/$($job.File) > /tmp/$($job.Name).log 2>&1 &"
  docker compose exec -d flink-jobmanager /bin/bash -lc $command
  if ($LASTEXITCODE -ne 0) { throw "Submission failed: $($job.Name)" }
}

$jar = "/opt/flink/usrlib/ad-realtime-datastream-jobs.jar"
$javaJobs = @(
  @{ Name="fluss-ods-log-datastream-to-dwd"; Class="cn.edu.ustc.lakehouse.realtime.job.DwdLogDataStreamJob" },
  @{ Name="fluss-last-click-datastream-6h"; Class="cn.edu.ustc.lakehouse.realtime.job.DwdOrderAttributionJob" },
  @{ Name="fluss-realtime-metric-datastream-10s"; Class="cn.edu.ustc.lakehouse.realtime.job.RealtimeAdMetricJob" }
)
foreach ($job in $javaJobs) {
  if ($running -match [regex]::Escape($job.Name)) { Write-Host "$($job.Name) is already running."; continue }
  docker compose exec -T flink-jobmanager flink run -d -c $job.Class $jar `
    --fluss-bootstrap fluss-coordinator:9123 --fluss-database ad_dw `
    --startup-mode earliest --out-of-orderness-seconds 10 `
    --attribution-allowed-lateness-seconds 10 --realtime-metric-window-seconds 10
  if ($LASTEXITCODE -ne 0) { throw "Submission failed: $($job.Name)" }
}
Write-Host "Flink 1.20.3 tiering, CDC, DataStream DWD/LastClick, SQL DWS, and 10-second realtime ADS jobs submitted."
