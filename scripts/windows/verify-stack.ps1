$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

Write-Host "Compose services:"
docker compose --profile core --profile olap --profile bi --profile ops --profile governance --profile metastore ps

Write-Host ""
Write-Host "Flink jobs endpoint:"
try {
  Invoke-RestMethod -Uri "http://127.0.0.1:8082/jobs" | ConvertTo-Json -Depth 5
} catch {
  Write-Warning "Flink REST endpoint is not ready: $($_.Exception.Message)"
}

Write-Host ""
Write-Host "StarRocks catalogs:"
try {
  $sql = @"
SHOW CATALOGS;
USE ad_ads;
SELECT 'metrics' AS view_name, COUNT(*) AS rows_count FROM v_realtime_ad_metrics
UNION ALL SELECT 'retention', COUNT(*) FROM v_advertiser_retention
UNION ALL SELECT 'attribution', COUNT(*) FROM v_attribution_summary
UNION ALL SELECT 'fraud', COUNT(*) FROM v_fraud_signal_summary
UNION ALL SELECT 'quality_rules', COUNT(*) FROM v_data_quality_result
UNION ALL SELECT 'quality_summary', COUNT(*) FROM v_data_quality_summary;
"@
  $tempFile = Join-Path ([System.IO.Path]::GetTempPath()) ("verify-starrocks-{0}.sql" -f ([guid]::NewGuid()))
  try {
    Set-Content -Path $tempFile -Value $sql -Encoding UTF8
    docker cp $tempFile ustc_lakehouse-starrocks-1:/tmp/verify_starrocks.sql | Out-Null
    docker compose --profile olap exec -T starrocks bash -lc "mysql -h127.0.0.1 -P9030 -uroot < /tmp/verify_starrocks.sql"
  } finally {
    Remove-Item -Path $tempFile -ErrorAction SilentlyContinue
  }
} catch {
  Write-Warning "StarRocks query failed: $($_.Exception.Message)"
}

Write-Host ""
Write-Host "HTTP endpoints:"
foreach ($item in @(
  @{ Name = "Flink"; Uri = "http://127.0.0.1:8082/jobs" },
  @{ Name = "Prometheus"; Uri = "http://127.0.0.1:19090/-/ready" },
  @{ Name = "Apicurio"; Uri = "http://127.0.0.1:8081/apis/registry/v3/system/info" },
  @{ Name = "Superset"; Uri = "http://127.0.0.1:8088/health" }
)) {
  try {
    Invoke-RestMethod -Uri $item.Uri | Out-Null
    Write-Host "$($item.Name): OK"
  } catch {
    Write-Warning "$($item.Name) endpoint failed: $($_.Exception.Message)"
  }
}

Write-Host ""
Write-Host "Schema Registry artifacts:"
try {
  Invoke-RestMethod -Uri "http://127.0.0.1:8081/apis/registry/v3/search/artifacts" | ConvertTo-Json -Depth 8
} catch {
  Write-Warning "Apicurio artifact search failed: $($_.Exception.Message)"
}

Write-Host ""
Write-Host "Hive Metastore port:"
Test-NetConnection 127.0.0.1 -Port 9083 | Select-Object ComputerName, RemotePort, TcpTestSucceeded | Format-List
