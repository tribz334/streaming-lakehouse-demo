param([switch]$WithBi)
$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$expectedJobs = @(
  "Fluss Lake Tiering Service",
  "mysql-cdc-to-fluss-ods-and-dim",
  "fluss-ods-log-datastream-to-dwd",
  "fluss-dwd-ad-bill-enrichment",
  "fluss-order-fact-datastream",
  "fluss-realtime-metric-datastream-10s"
)
$jobs = (Invoke-RestMethod -Uri "http://127.0.0.1:18082/jobs" -TimeoutSec 10).jobs
$runningNames = foreach ($job in $jobs | Where-Object status -eq "RUNNING") {
  (Invoke-RestMethod -Uri "http://127.0.0.1:18082/jobs/$($job.id)" -TimeoutSec 10).name
}
$missing = @($expectedJobs | Where-Object {
  $expectedName = $_
  -not ($runningNames | Where-Object { $_ -eq $expectedName -or $_.StartsWith($expectedName + " - ") })
})
if ($missing.Count -gt 0) { throw "Missing RUNNING Flink jobs: $($missing -join ', ')" }

$mysqlCounts = docker compose exec -T mysql mysql -uroot -proot --batch --skip-column-names `
  -e "SELECT (SELECT COUNT(*) FROM ad_ods.user_info),(SELECT COUNT(*) FROM ad_ods.bill_info),(SELECT COUNT(*) FROM ad_ods.order_info)"
if ($LASTEXITCODE -ne 0) { throw "MySQL verification failed" }
$mysqlParts = (($mysqlCounts | Select-Object -Last 1) -split "`t")
if ($mysqlParts.Count -lt 3 -or [long]$mysqlParts[0] -eq 0 `
    -or [long]$mysqlParts[1] -eq 0 -or [long]$mysqlParts[2] -eq 0) {
  throw "MySQL demo data is incomplete: $mysqlCounts"
}

$metricSql = @"
SELECT COUNT(*) AS trend_days,
       ROUND(SUM(cost),2) AS cost_yuan,
       ROUND(SUM(closed_cost),2) AS closed_cost_yuan,
       ROUND(SUM(pay_order_gmv),2) AS gmv_yuan,
       ROUND(SUM(pay_order_gmv)/NULLIF(SUM(closed_cost),0),2) AS roas
FROM ad_ads.v_offline_metric;
SELECT window_start,cost,closed_cost,pay_order_gmv,realtime_roas
FROM ad_ads.v_realtime_metric_latest;
"@
$metricResult = docker compose exec -T starrocks mysql --protocol=TCP `
  --host=127.0.0.1 --port=9030 --user=root --table -e $metricSql
if ($LASTEXITCODE -ne 0) { throw "StarRocks metric verification failed" }
$metricResult

if ($WithBi) {
  $health = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:18088/health" -TimeoutSec 10
  if ($health.StatusCode -ne 200) { throw "Superset health check failed" }
}

Write-Host "Verified $($expectedJobs.Count) streaming jobs, MySQL facts, StarRocks metrics$(if ($WithBi) { ', and Superset' })."
