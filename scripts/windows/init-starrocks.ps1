$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$metricTableDdl = @(docker compose --profile olap exec -T starrocks bash -lc `
  "mysql -N -h127.0.0.1 -P9030 -uroot -e 'SHOW CREATE TABLE ad_ads.realtime_ad_metrics_10s'" 2>$null)
$metricTableResetSql = ""
if ($metricTableDdl.Count -gt 0 -and ($metricTableDdl -join "`n") -notmatch "PRIMARY KEY") {
  $metricTableResetSql = "DROP TABLE IF EXISTS ad_ads.realtime_ad_metrics_10s;"
  Write-Host "Migrating the legacy real-time metric table to the Primary Key model."
}

$cleanupSql = @"
CREATE DATABASE IF NOT EXISTS ad_ads;
DROP VIEW IF EXISTS ad_ads.v_realtime_ad_metrics;
$metricTableResetSql
DROP VIEW IF EXISTS ad_ads.v_advertiser_retention;
DROP VIEW IF EXISTS ad_ads.v_attribution_summary;
DROP VIEW IF EXISTS ad_ads.v_fraud_signal_summary;
"@
$cleanupFile = Join-Path ([System.IO.Path]::GetTempPath()) ("cleanup-starrocks-{0}.sql" -f ([guid]::NewGuid()))
try {
  Set-Content -Path $cleanupFile -Value $cleanupSql -Encoding UTF8
  docker cp $cleanupFile ustc_lakehouse-starrocks-1:/tmp/cleanup_starrocks.sql | Out-Null
  docker compose --profile olap exec -T starrocks bash -lc "mysql -h127.0.0.1 -P9030 -uroot < /tmp/cleanup_starrocks.sql"
  if ($LASTEXITCODE -ne 0) { throw "StarRocks view cleanup failed." }

  $catalogs = docker compose --profile olap exec -T starrocks bash -lc "mysql -N -h127.0.0.1 -P9030 -uroot -e 'SHOW CATALOGS'"
  if ($catalogs -match "(?m)^paimon_catalog\s") {
    docker compose --profile olap exec -T starrocks bash -lc "mysql -h127.0.0.1 -P9030 -uroot -e 'DROP CATALOG paimon_catalog'"
    if ($LASTEXITCODE -ne 0) { throw "Existing StarRocks Paimon catalog cleanup failed." }
  }
} finally {
  Remove-Item -Path $cleanupFile -ErrorAction SilentlyContinue
}

$tempFile = Join-Path ([System.IO.Path]::GetTempPath()) ("init-starrocks-{0}.sql" -f ([guid]::NewGuid()))
try {
  Get-Content -Raw .\starrocks\init_starrocks.sql | Set-Content -Path $tempFile -Encoding UTF8
  docker cp $tempFile ustc_lakehouse-starrocks-1:/tmp/init_starrocks.sql
  $output = docker compose --profile olap exec -T starrocks bash -lc "mysql -h127.0.0.1 -P9030 -uroot < /tmp/init_starrocks.sql" 2>&1
  $output
  if ($LASTEXITCODE -ne 0 -or $output -match "ERROR") {
    throw "StarRocks initialization failed. See mysql output above."
  }
} finally {
  Remove-Item -Path $tempFile -ErrorAction SilentlyContinue
}

Write-Host "StarRocks real-time Primary Key table is ready for the Java Flink JDBC sink."
Write-Host "Run sync-starrocks-olap.ps1 for offline ADS snapshots."
