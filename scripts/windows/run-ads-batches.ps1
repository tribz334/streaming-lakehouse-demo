$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$jobs = @(
  "03_dws_metrics.sql",
  "07_thesis_offline_layers.sql",
  "04_ads_retention_batch.sql",
  "05_ads_attribution_batch.sql",
  "06_ads_fraud_batch.sql",
  "08_data_quality_batch.sql"
)

foreach ($job in $jobs) {
  Write-Host "Running $job ..."
  $runFile = "/tmp/run_$job"
  $output = docker compose --profile core exec -T flink-jobmanager /bin/bash -lc `
    "cat /opt/flink/usrlib/sql/00_catalogs_and_tables.sql /opt/flink/usrlib/sql/$job > $runFile && /opt/flink/bin/sql-client.sh -f $runFile" 2>&1
  if ($LASTEXITCODE -ne 0 -or $output -match "\[ERROR\]") {
    $output
    throw "ADS batch failed: $job. See SQL Client output above."
  }
  Write-Host "Finished $job."
}

Write-Host "Thesis DWD/DWM/DWS/DM layers and ADS batches finished."
