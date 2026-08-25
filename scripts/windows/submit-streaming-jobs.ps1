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
  @{ Name="fluss-ods-to-dwd-ad-event-and-bill-di"; File="02b_ods_changelog_to_dwd.sql" },
  @{ Name="fluss-order-direct-attribution-6h"; File="02c_direct_order_attribution.sql" },
  @{ Name="fluss-order-cdc-to-dwd-ad-order-di"; File="02d_attributed_order_to_event.sql" },
  @{ Name="fluss-dwd-to-three-dws-topic-di"; File="03_realtime_order_dws.sql" },
  @{ Name="fluss-dws-creative-di-to-ads-realtime-30s"; File="04_realtime_ads.sql" }
)
foreach ($job in $jobs) {
  if ($running -match [regex]::Escape($job.Name)) { Write-Host "$($job.Name) is already running."; continue }
  $command = "nohup /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/$($job.File) > /tmp/$($job.Name).log 2>&1 &"
  docker compose exec -d flink-jobmanager /bin/bash -lc $command
  if ($LASTEXITCODE -ne 0) { throw "Submission failed: $($job.Name)" }
}
Write-Host "Flink 1.20.3 tiering, CDC, ODS-to-DWD, and 30-second order GMV jobs submitted."
