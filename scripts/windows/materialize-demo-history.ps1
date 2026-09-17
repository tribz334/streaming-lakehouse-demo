param(
  [ValidateRange(1, 35)][int]$Days = 14,
  [ValidateRange(1, 60)][int]$WaitTimeoutMinutes = 12
)
$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$deadline = (Get-Date).AddMinutes($WaitTimeoutMinutes)
do {
  $generatorLogs = docker compose logs --tail 200 event-generator 2>&1 | Out-String
  if ($generatorLogs -match "historical backfill complete") { break }
  if ((Get-Date) -ge $deadline) {
    throw "Timed out waiting for the deterministic historical generator"
  }
  Write-Host "Waiting for the 35-day generator backfill to finish..."
  Start-Sleep -Seconds 5
} while ($true)

do {
  $status = docker compose exec -T starrocks mysql --protocol=TCP `
    --host=127.0.0.1 --port=9030 --user=root --batch --skip-column-names `
    -e "SELECT COUNT(DISTINCT dt),COALESCE(SUM(pay_order_count),0),COALESCE(SUM(cost),0) FROM ad_ads.v_dws_ad_advertiser_di" 2>$null
  if ($LASTEXITCODE -eq 0 -and $status) {
    $parts = (($status | Select-Object -Last 1) -split "`t")
    if ($parts.Count -ge 3 -and [int]$parts[0] -ge $Days `
        -and [long]$parts[1] -gt 0 -and [long]$parts[2] -gt 0) {
      break
    }
  }
  if ((Get-Date) -ge $deadline) {
    throw "Timed out waiting for Cost and attributed GMV to reach the DWS layer"
  }
  Write-Host "Waiting for streaming DWS and Paimon tiering..."
  Start-Sleep -Seconds 5
} while ($true)

$endDate = (Get-Date).Date.AddDays(-1)
$startDate = $endDate.AddDays(-($Days - 1))
$baseDate = $startDate.AddDays(-1).ToString("yyyy-MM-dd")
& (Join-Path $PSScriptRoot "initialize-dm.ps1") -BizDate $baseDate

for ($offset = 0; $offset -lt $Days; $offset++) {
  $bizDate = $startDate.AddDays($offset).ToString("yyyy-MM-dd")
  Write-Host "Materializing offline partition $bizDate ($($offset + 1)/$Days)..."
  & (Join-Path $PSScriptRoot "run-daily-batch.ps1") -BizDate $bizDate
}

docker compose exec -T starrocks mysql --protocol=TCP --host=127.0.0.1 `
  --port=9030 --user=root --table -e `
  "SELECT MIN(dt) AS first_day,MAX(dt) AS last_day,COUNT(*) AS trend_days,ROUND(SUM(cost),2) AS cost_yuan,ROUND(SUM(pay_order_gmv),2) AS gmv_yuan,ROUND(SUM(pay_order_gmv)/NULLIF(SUM(closed_cost),0),2) AS roas FROM ad_ads.v_offline_metric WHERE dt BETWEEN '$($startDate.ToString("yyyy-MM-dd"))' AND '$($endDate.ToString("yyyy-MM-dd"))'"
if ($LASTEXITCODE -ne 0) { throw "Failed to verify materialized offline metrics" }

Write-Host "Deterministic offline demo history is ready for Superset."
