$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$jobs = @(
  "08_offline_dws.sql",
  "09_offline_dm.sql",
  "10_ads_retention.sql",
  "11_ads_attribution.sql",
  "12_ads_fraud.sql",
  "13_ads_creative_offline.sql"
)

foreach ($job in $jobs) {
  Write-Host "Running $job ..."
  $runFile = "/tmp/run_$job"
  $output = docker compose exec -T flink-jobmanager /bin/bash -lc `
    "cat /opt/flink/usrlib/sql/00_catalogs_and_tables.sql /opt/flink/usrlib/sql/01_model_tables.sql /opt/flink/usrlib/sql/$job > $runFile && /opt/flink/bin/sql-client.sh -f $runFile" 2>&1
  if ($LASTEXITCODE -ne 0 -or $output -match "\[ERROR\]") {
    $output
    throw "ADS batch failed: $job. See SQL Client output above."
  }
  Write-Host "Finished $job."
}

Write-Host "Streamlined DWS/DM/ADS batches finished."
