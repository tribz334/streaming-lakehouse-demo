param(
  [ValidateSet("All", "Retention")]
  [string]$Dataset = "All"
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

function Invoke-FlinkQuery {
  param([Parameter(Mandatory = $true)][string]$Query)

  $ddl = Get-Content -Raw -Path (Join-Path $root "flink/sql/00_catalogs_and_tables.sql")
  $sql = @"
$ddl
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

$Query
"@

  $queryId = [guid]::NewGuid().ToString("N")
  $tempFile = Join-Path ([System.IO.Path]::GetTempPath()) ("flink-export-{0}.sql" -f $queryId)
  $containerFile = "/tmp/starrocks_export_$queryId.sql"
  try {
    Set-Content -Path $tempFile -Value $sql -Encoding UTF8
    docker cp $tempFile "ustc_lakehouse-flink-jobmanager-1:$containerFile" | Out-Null
    $output = @(docker compose --profile core exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f $containerFile 2>&1)
    if ($LASTEXITCODE -ne 0 -or $output -match "\[ERROR\]") {
      $output
      throw "Flink export query failed."
    }
    return $output
  } finally {
    docker compose --profile core exec -T flink-jobmanager rm -f $containerFile 2>$null
    Remove-Item -Path $tempFile -ErrorAction SilentlyContinue
  }
}

function ConvertFrom-Tableau {
  param(
    [AllowEmptyCollection()][AllowEmptyString()][string[]]$Output,
    [Parameter(Mandatory = $true)][string[]]$Columns
  )

  $rows = [System.Collections.Generic.List[object]]::new()
  foreach ($line in $Output) {
    $trimmed = $line.Trim()
    if (-not ($trimmed.StartsWith("|") -and $trimmed.EndsWith("|"))) { continue }
    if ($trimmed -match "^\+-") { continue }

    $cells = $trimmed.Trim("|").Split("|") | ForEach-Object { $_.Trim() }
    if ($cells.Count -ne $Columns.Count) { continue }
    if ($cells[0] -eq $Columns[0]) { continue }

    $row = [ordered]@{}
    for ($i = 0; $i -lt $Columns.Count; $i++) {
      $row[$Columns[$i]] = $cells[$i]
    }
    $rows.Add([pscustomobject]$row)
  }
  return $rows.ToArray()
}

function SqlString {
  param([AllowNull()][string]$Value)
  if ($null -eq $Value -or $Value -eq "<NULL>") { return "NULL" }
  return "'" + ($Value -replace "'", "''") + "'"
}

function SqlNumber {
  param([AllowNull()][string]$Value)
  if ($null -eq $Value -or $Value -eq "<NULL>" -or $Value -eq "") { return "0" }
  return $Value
}

function SqlNullableNumber {
  param([AllowNull()][string]$Value)
  if ($null -eq $Value -or $Value -eq "<NULL>" -or $Value -eq "") { return "NULL" }
  return $Value
}

if ($Dataset -eq "Retention") {
  $retentionColumns = @("cohort_date", "cohort_size", "retained_1d", "retained_7d", "retained_15d", "retained_30d", "rate_1d", "rate_7d", "rate_15d", "rate_30d", "updated_at")
  $retentionOutput = Invoke-FlinkQuery @"
SELECT
  cohort_date,
  CAST(cohort_size AS STRING) AS cohort_size,
  CAST(retained_1d AS STRING) AS retained_1d,
  CAST(retained_7d AS STRING) AS retained_7d,
  CAST(retained_15d AS STRING) AS retained_15d,
  CAST(retained_30d AS STRING) AS retained_30d,
  CAST(rate_1d AS STRING) AS rate_1d,
  CAST(rate_7d AS STRING) AS rate_7d,
  CAST(rate_15d AS STRING) AS rate_15d,
  CAST(rate_30d AS STRING) AS rate_30d,
  CAST(updated_at AS STRING) AS updated_at
FROM paimon.ad_dw.ads_advertiser_retention_di
ORDER BY cohort_date;
"@
  $retentionRows = ConvertFrom-Tableau -Output $retentionOutput -Columns $retentionColumns
  if ($retentionRows.Count -eq 0) {
    throw "No retention rows exported from Paimon. Run run-ads-batches.ps1 first."
  }

  $retentionSql = @"
CREATE DATABASE IF NOT EXISTS ad_ads;
USE ad_ads;

DROP VIEW IF EXISTS v_advertiser_retention;
DROP TABLE IF EXISTS advertiser_retention_snapshot;
CREATE TABLE advertiser_retention_snapshot (
  cohort_date VARCHAR(32) NOT NULL,
  cohort_size BIGINT,
  retained_1d BIGINT,
  retained_7d BIGINT,
  retained_15d BIGINT,
  retained_30d BIGINT,
  rate_1d DECIMAL(18,6),
  rate_7d DECIMAL(18,6),
  rate_15d DECIMAL(18,6),
  rate_30d DECIMAL(18,6),
  updated_at DATETIME
)
DUPLICATE KEY(cohort_date)
DISTRIBUTED BY HASH(cohort_date) BUCKETS 1
PROPERTIES ("replication_num" = "1");
"@

  $values = $retentionRows | ForEach-Object {
    "(" + (@(
      SqlString $_.cohort_date
      SqlNumber $_.cohort_size
      SqlNumber $_.retained_1d
      SqlNumber $_.retained_7d
      SqlNumber $_.retained_15d
      SqlNumber $_.retained_30d
      SqlNumber $_.rate_1d
      SqlNumber $_.rate_7d
      SqlNumber $_.rate_15d
      SqlNumber $_.rate_30d
      SqlString $_.updated_at
    ) -join ", ") + ")"
  }
  $retentionSql += "`nINSERT INTO advertiser_retention_snapshot VALUES`n" + ($values -join ",`n") + ";`n"
  $retentionSql += @"

CREATE VIEW v_advertiser_retention AS
SELECT
  CAST(cohort_date AS DATE) AS cohort_date,
  cohort_size,
  retained_1d,
  retained_7d,
  retained_15d,
  retained_30d,
  rate_1d,
  rate_7d,
  rate_15d,
  rate_30d,
  updated_at
FROM advertiser_retention_snapshot;
"@

  $tempRetentionSql = Join-Path ([System.IO.Path]::GetTempPath()) ("sync-starrocks-retention-{0}.sql" -f ([guid]::NewGuid()))
  try {
    Set-Content -Path $tempRetentionSql -Value $retentionSql -Encoding UTF8
    docker cp $tempRetentionSql ustc_lakehouse-starrocks-1:/tmp/sync_starrocks_retention.sql | Out-Null
    $output = docker compose --profile olap exec -T starrocks bash -lc "mysql -h127.0.0.1 -P9030 -uroot < /tmp/sync_starrocks_retention.sql" 2>&1
    $output
    if ($LASTEXITCODE -ne 0 -or $output -match "ERROR") {
      throw "StarRocks retention sync failed. See mysql output above."
    }
  } finally {
    Remove-Item -Path $tempRetentionSql -ErrorAction SilentlyContinue
  }

  Write-Host "Synced retention=$($retentionRows.Count) into StarRocks."
  exit 0
}

$retentionColumns = @("cohort_date", "cohort_size", "retained_1d", "retained_7d", "retained_15d", "retained_30d", "rate_1d", "rate_7d", "rate_15d", "rate_30d", "updated_at")
$retentionOutput = Invoke-FlinkQuery @"
SELECT
  cohort_date,
  CAST(cohort_size AS STRING) AS cohort_size,
  CAST(retained_1d AS STRING) AS retained_1d,
  CAST(retained_7d AS STRING) AS retained_7d,
  CAST(retained_15d AS STRING) AS retained_15d,
  CAST(retained_30d AS STRING) AS retained_30d,
  CAST(rate_1d AS STRING) AS rate_1d,
  CAST(rate_7d AS STRING) AS rate_7d,
  CAST(rate_15d AS STRING) AS rate_15d,
  CAST(rate_30d AS STRING) AS rate_30d,
  CAST(updated_at AS STRING) AS updated_at
FROM paimon.ad_dw.ads_advertiser_retention_di
ORDER BY cohort_date;
"@
$retentionRows = ConvertFrom-Tableau -Output $retentionOutput -Columns $retentionColumns

$attributionColumns = @("event_date", "advertiser_id", "advertiser_name", "campaign_id", "campaign_name", "attribution_model", "conversions", "orders", "attributed_gmv", "attributed_spend", "updated_at")
$attributionOutput = Invoke-FlinkQuery @"
SELECT
  event_date,
  advertiser_id,
  advertiser_name,
  campaign_id,
  campaign_name,
  attribution_model,
  CAST(conversions AS STRING) AS conversions,
  CAST(orders AS STRING) AS orders,
  CAST(attributed_gmv AS STRING) AS attributed_gmv,
  CAST(attributed_spend AS STRING) AS attributed_spend,
  CAST(updated_at AS STRING) AS updated_at
FROM paimon.ad_dw.ads_attribution_summary_di
ORDER BY event_date, advertiser_id, campaign_id;
"@
$attributionRows = ConvertFrom-Tableau -Output $attributionOutput -Columns $attributionColumns
if ($attributionRows.Count -eq 0) {
  throw "Attribution summary export is empty; refusing to replace the existing StarRocks snapshot."
}

$attributionDetailColumns = @("event_date", "order_event_id", "order_id", "order_ts", "user_id", "order_advertiser_id", "order_advertiser_name", "order_campaign_id", "order_campaign_name", "order_gmv", "click_event_id", "click_ts", "creative_id", "campaign_id", "campaign_name", "advertiser_id", "advertiser_name", "touch_spend", "attribution_model", "attribution_type", "attribution_period", "attribution_sort", "lag_minutes", "is_attributed", "updated_at")
$attributionDetailOutput = Invoke-FlinkQuery @"
SELECT
  event_date, order_event_id, order_id,
  CAST(order_ts AS STRING) AS order_ts,
  user_id, order_advertiser_id, order_advertiser_name,
  order_campaign_id, order_campaign_name,
  CAST(order_gmv AS STRING) AS order_gmv,
  click_event_id, CAST(click_ts AS STRING) AS click_ts,
  creative_id, campaign_id, campaign_name, advertiser_id, advertiser_name,
  CAST(touch_spend AS STRING) AS touch_spend,
  attribution_model, attribution_type, attribution_period,
  CAST(attribution_sort AS STRING) AS attribution_sort,
  CAST(lag_minutes AS STRING) AS lag_minutes,
  CASE WHEN is_attributed THEN '1' ELSE '0' END AS is_attributed,
  CAST(updated_at AS STRING) AS updated_at
FROM paimon.ad_dw.ads_order_attribution_detail_di
ORDER BY event_date, order_event_id;
"@
$attributionDetailRows = ConvertFrom-Tableau -Output $attributionDetailOutput -Columns $attributionDetailColumns
if ($attributionDetailRows.Count -eq 0) {
  throw "Attribution detail export is empty; refusing to replace the existing StarRocks snapshot."
}

$offlineCreativeColumns = @("stat_date", "creative_id", "creative_name", "creative_format", "campaign_id", "campaign_name", "campaign_objective", "campaign_budget", "campaign_status", "advertiser_id", "advertiser_name", "industry", "advertiser_tier", "unit_id", "unit_name", "bid_type", "bid_amount", "impressions", "clicks", "conversions", "orders", "cost", "gmv", "ctr", "cvr", "cpc", "cpa", "roas", "updated_at")
$offlineCreativeOutput = Invoke-FlinkQuery @"
SELECT
  stat_date, creative_id, creative_name, creative_format,
  campaign_id, campaign_name, campaign_objective,
  CAST(campaign_budget AS STRING) AS campaign_budget,
  campaign_status, advertiser_id, advertiser_name, industry, advertiser_tier,
  unit_id, unit_name, bid_type, CAST(bid_amount AS STRING) AS bid_amount,
  CAST(impressions AS STRING) AS impressions,
  CAST(clicks AS STRING) AS clicks,
  CAST(conversions AS STRING) AS conversions,
  CAST(orders AS STRING) AS orders,
  CAST(cost AS STRING) AS cost,
  CAST(gmv AS STRING) AS gmv,
  CAST(ctr AS STRING) AS ctr,
  CAST(cvr AS STRING) AS cvr,
  CAST(cpc AS STRING) AS cpc,
  CAST(cpa AS STRING) AS cpa,
  CAST(roi AS STRING) AS roas,
  CAST(updated_at AS STRING) AS updated_at
FROM paimon.ad_dw.ads_creative_offline_di
ORDER BY stat_date, creative_id;
"@
$offlineCreativeRows = ConvertFrom-Tableau -Output $offlineCreativeOutput -Columns $offlineCreativeColumns

$fraudColumns = @("event_date", "advertiser_id", "advertiser_name", "suspicious_users", "suspicious_windows", "suspicious_clicks", "suspicious_spend", "avg_risk_score", "updated_at")
$fraudOutput = Invoke-FlinkQuery @"
SELECT
  event_date,
  advertiser_id,
  MAX(advertiser_name) AS advertiser_name,
  CAST(SUM(unique_users) AS STRING) AS suspicious_users,
  CAST(COUNT(*) AS STRING) AS suspicious_windows,
  CAST(SUM(click_count) AS STRING) AS suspicious_clicks,
  CAST(SUM(suspicious_spend) AS STRING) AS suspicious_spend,
  CAST(AVG(CAST(risk_score AS DOUBLE)) AS STRING) AS avg_risk_score,
  CAST(MAX(updated_at) AS STRING) AS updated_at
FROM paimon.ad_dw.ads_fraud_signal_di
GROUP BY event_date, advertiser_id
ORDER BY event_date, advertiser_id;
"@
$fraudRows = ConvertFrom-Tableau -Output $fraudOutput -Columns $fraudColumns
if ($fraudRows.Count -eq 0) {
  throw "Fraud signal export is empty; refusing to replace the existing StarRocks snapshot."
}

$starrocksSql = @"
CREATE DATABASE IF NOT EXISTS ad_ads;
USE ad_ads;

CREATE TABLE IF NOT EXISTS realtime_ad_metrics_snapshot (
  window_start DATETIME NOT NULL,
  advertiser_id VARCHAR(64) NOT NULL,
  campaign_id VARCHAR(64) NOT NULL,
  unit_id VARCHAR(64) NOT NULL,
  creative_id VARCHAR(64) NOT NULL,
  window_end DATETIME NOT NULL,
  advertiser_name VARCHAR(255),
  spend DECIMAL(18,4),
  gmv DECIMAL(18,2),
  impressions BIGINT,
  clicks BIGINT,
  conversions BIGINT,
  orders BIGINT,
  ctr DECIMAL(18,6),
  cvr DECIMAL(18,6),
  roi DECIMAL(18,6),
  updated_at DATETIME
)
PRIMARY KEY(window_start, advertiser_id, campaign_id, unit_id, creative_id)
DISTRIBUTED BY HASH(advertiser_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

DROP TABLE IF EXISTS creative_offline_snapshot;
CREATE TABLE creative_offline_snapshot (
  stat_date VARCHAR(32) NOT NULL,
  creative_id VARCHAR(64) NOT NULL,
  creative_name VARCHAR(255),
  creative_format VARCHAR(64),
  campaign_id VARCHAR(64),
  campaign_name VARCHAR(255),
  campaign_objective VARCHAR(64),
  campaign_budget DECIMAL(18,2),
  campaign_status VARCHAR(32),
  advertiser_id VARCHAR(64),
  advertiser_name VARCHAR(255),
  industry VARCHAR(128),
  advertiser_tier VARCHAR(64),
  unit_id VARCHAR(64),
  unit_name VARCHAR(255),
  bid_type VARCHAR(64),
  bid_amount DECIMAL(18,4),
  impressions BIGINT,
  clicks BIGINT,
  conversions BIGINT,
  orders BIGINT,
  cost DECIMAL(18,2),
  gmv DECIMAL(18,2),
  ctr DECIMAL(18,6),
  cvr DECIMAL(18,6),
  cpc DECIMAL(18,4),
  cpa DECIMAL(18,4),
  roas DECIMAL(18,6),
  updated_at DATETIME
)
DUPLICATE KEY(stat_date, creative_id)
DISTRIBUTED BY HASH(creative_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

DROP TABLE IF EXISTS order_attribution_detail_snapshot;
CREATE TABLE order_attribution_detail_snapshot (
  event_date VARCHAR(32) NOT NULL,
  order_event_id VARCHAR(64) NOT NULL,
  order_id VARCHAR(64),
  order_ts DATETIME,
  user_id VARCHAR(64),
  order_advertiser_id VARCHAR(64),
  order_advertiser_name VARCHAR(255),
  order_campaign_id VARCHAR(64),
  order_campaign_name VARCHAR(255),
  order_gmv DECIMAL(18,2),
  click_event_id VARCHAR(64),
  click_ts DATETIME,
  creative_id VARCHAR(64),
  campaign_id VARCHAR(64),
  campaign_name VARCHAR(255),
  advertiser_id VARCHAR(64),
  advertiser_name VARCHAR(255),
  touch_spend DECIMAL(18,4),
  attribution_model VARCHAR(64),
  attribution_type VARCHAR(32),
  attribution_period VARCHAR(64),
  attribution_sort INT,
  lag_minutes BIGINT,
  is_attributed BOOLEAN,
  updated_at DATETIME
)
DUPLICATE KEY(event_date, order_event_id)
DISTRIBUTED BY HASH(order_event_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

DROP TABLE IF EXISTS advertiser_retention_snapshot;
CREATE TABLE advertiser_retention_snapshot (
  cohort_date VARCHAR(32) NOT NULL,
  cohort_size BIGINT,
  retained_1d BIGINT,
  retained_7d BIGINT,
  retained_15d BIGINT,
  retained_30d BIGINT,
  rate_1d DECIMAL(18,6),
  rate_7d DECIMAL(18,6),
  rate_15d DECIMAL(18,6),
  rate_30d DECIMAL(18,6),
  updated_at DATETIME
)
DUPLICATE KEY(cohort_date)
DISTRIBUTED BY HASH(cohort_date) BUCKETS 1
PROPERTIES ("replication_num" = "1");

DROP TABLE IF EXISTS attribution_summary_snapshot;
CREATE TABLE attribution_summary_snapshot (
  event_date VARCHAR(32) NOT NULL,
  advertiser_id VARCHAR(64) NOT NULL,
  campaign_id VARCHAR(64) NOT NULL,
  attribution_model VARCHAR(64) NOT NULL,
  advertiser_name VARCHAR(255),
  campaign_name VARCHAR(255),
  conversions BIGINT,
  orders BIGINT,
  attributed_gmv DECIMAL(18,2),
  attributed_spend DECIMAL(18,4),
  updated_at DATETIME
)
DUPLICATE KEY(event_date, advertiser_id, campaign_id, attribution_model)
DISTRIBUTED BY HASH(advertiser_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

DROP TABLE IF EXISTS fraud_signal_snapshot;
CREATE TABLE fraud_signal_snapshot (
  event_date VARCHAR(32) NOT NULL,
  advertiser_id VARCHAR(64) NOT NULL,
  advertiser_name VARCHAR(255),
  suspicious_users BIGINT,
  suspicious_windows BIGINT,
  suspicious_clicks BIGINT,
  suspicious_spend DECIMAL(18,4),
  avg_risk_score DECIMAL(18,6),
  updated_at DATETIME
)
DUPLICATE KEY(event_date, advertiser_id)
DISTRIBUTED BY HASH(advertiser_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

"@

if ($retentionRows.Count -gt 0) {
  $values = $retentionRows | ForEach-Object {
    "(" + (@(
      SqlString $_.cohort_date
      SqlNumber $_.cohort_size
      SqlNumber $_.retained_1d
      SqlNumber $_.retained_7d
      SqlNumber $_.retained_15d
      SqlNumber $_.retained_30d
      SqlNumber $_.rate_1d
      SqlNumber $_.rate_7d
      SqlNumber $_.rate_15d
      SqlNumber $_.rate_30d
      SqlString $_.updated_at
    ) -join ", ") + ")"
  }
  $starrocksSql += "`nINSERT INTO advertiser_retention_snapshot VALUES`n" + ($values -join ",`n") + ";`n"
}

if ($attributionRows.Count -gt 0) {
  $values = $attributionRows | ForEach-Object {
    "(" + (@(
      SqlString $_.event_date
      SqlString $_.advertiser_id
      SqlString $_.campaign_id
      SqlString $_.attribution_model
      SqlString $_.advertiser_name
      SqlString $_.campaign_name
      SqlNumber $_.conversions
      SqlNumber $_.orders
      SqlNumber $_.attributed_gmv
      SqlNumber $_.attributed_spend
      SqlString $_.updated_at
    ) -join ", ") + ")"
  }
  $starrocksSql += "`nINSERT INTO attribution_summary_snapshot VALUES`n" + ($values -join ",`n") + ";`n"
}

if ($attributionDetailRows.Count -gt 0) {
  $values = $attributionDetailRows | ForEach-Object {
    "(" + (@(
      SqlString $_.event_date
      SqlString $_.order_event_id
      SqlString $_.order_id
      SqlString $_.order_ts
      SqlString $_.user_id
      SqlString $_.order_advertiser_id
      SqlString $_.order_advertiser_name
      SqlString $_.order_campaign_id
      SqlString $_.order_campaign_name
      SqlNumber $_.order_gmv
      SqlString $_.click_event_id
      SqlString $_.click_ts
      SqlString $_.creative_id
      SqlString $_.campaign_id
      SqlString $_.campaign_name
      SqlString $_.advertiser_id
      SqlString $_.advertiser_name
      SqlNullableNumber $_.touch_spend
      SqlString $_.attribution_model
      SqlString $_.attribution_type
      SqlString $_.attribution_period
      SqlNumber $_.attribution_sort
      SqlNullableNumber $_.lag_minutes
      SqlNumber $_.is_attributed
      SqlString $_.updated_at
    ) -join ", ") + ")"
  }
  $starrocksSql += "`nINSERT INTO order_attribution_detail_snapshot VALUES`n" + ($values -join ",`n") + ";`n"
}

if ($offlineCreativeRows.Count -gt 0) {
  $values = $offlineCreativeRows | ForEach-Object {
    "(" + (@(
      SqlString $_.stat_date
      SqlString $_.creative_id
      SqlString $_.creative_name
      SqlString $_.creative_format
      SqlString $_.campaign_id
      SqlString $_.campaign_name
      SqlString $_.campaign_objective
      SqlNullableNumber $_.campaign_budget
      SqlString $_.campaign_status
      SqlString $_.advertiser_id
      SqlString $_.advertiser_name
      SqlString $_.industry
      SqlString $_.advertiser_tier
      SqlString $_.unit_id
      SqlString $_.unit_name
      SqlString $_.bid_type
      SqlNullableNumber $_.bid_amount
      SqlNumber $_.impressions
      SqlNumber $_.clicks
      SqlNumber $_.conversions
      SqlNumber $_.orders
      SqlNumber $_.cost
      SqlNumber $_.gmv
      SqlNullableNumber $_.ctr
      SqlNullableNumber $_.cvr
      SqlNullableNumber $_.cpc
      SqlNullableNumber $_.cpa
      SqlNullableNumber $_.roas
      SqlString $_.updated_at
    ) -join ", ") + ")"
  }
  $starrocksSql += "`nINSERT INTO creative_offline_snapshot VALUES`n" + ($values -join ",`n") + ";`n"
}

if ($fraudRows.Count -gt 0) {
  $values = $fraudRows | ForEach-Object {
    "(" + (@(
      SqlString $_.event_date
      SqlString $_.advertiser_id
      SqlString $_.advertiser_name
      SqlNumber $_.suspicious_users
      SqlNumber $_.suspicious_windows
      SqlNumber $_.suspicious_clicks
      SqlNumber $_.suspicious_spend
      SqlNumber $_.avg_risk_score
      SqlString $_.updated_at
    ) -join ", ") + ")"
  }
  $starrocksSql += "`nINSERT INTO fraud_signal_snapshot VALUES`n" + ($values -join ",`n") + ";`n"
}

$starrocksSql += @"

CREATE OR REPLACE VIEW v_realtime_ad_metrics AS
SELECT
  window_start, advertiser_id, campaign_id, unit_id, creative_id,
  window_end, advertiser_name, spend, gmv, impressions, clicks,
  conversions, orders, ctr, cvr, roi AS roas, updated_at
FROM realtime_ad_metrics_snapshot;

CREATE OR REPLACE VIEW v_advertiser_retention AS
SELECT
  CAST(cohort_date AS DATE) AS cohort_date,
  cohort_size,
  retained_1d,
  retained_7d,
  retained_15d,
  retained_30d,
  rate_1d,
  rate_7d,
  rate_15d,
  rate_30d,
  updated_at
FROM advertiser_retention_snapshot;

CREATE OR REPLACE VIEW v_attribution_summary AS
SELECT
  CAST(event_date AS DATE) AS event_date,
  advertiser_id,
  advertiser_name,
  campaign_id,
  campaign_name,
  attribution_model AS attribution_period,
  conversions,
  orders,
  attributed_gmv,
  attributed_spend,
  updated_at
FROM attribution_summary_snapshot;

CREATE OR REPLACE VIEW v_order_attribution_detail AS
SELECT * FROM order_attribution_detail_snapshot;

CREATE OR REPLACE VIEW v_creative_offline_metrics AS
SELECT
  CAST(stat_date AS DATE) AS stat_date,
  creative_id,
  creative_name,
  creative_format,
  campaign_id,
  campaign_name,
  campaign_objective,
  campaign_budget,
  campaign_status,
  advertiser_id,
  advertiser_name,
  industry,
  advertiser_tier,
  unit_id,
  unit_name,
  bid_type,
  bid_amount,
  impressions,
  clicks,
  conversions,
  orders,
  cost,
  gmv,
  ctr,
  cvr,
  cpc,
  cpa,
  roas,
  stat_date = (SELECT MAX(stat_date) FROM creative_offline_snapshot) AS is_latest_partition,
  updated_at
FROM creative_offline_snapshot;

CREATE OR REPLACE VIEW v_fraud_signal_summary AS
SELECT
  CAST(event_date AS DATE) AS event_date,
  advertiser_id,
  advertiser_name,
  suspicious_users,
  suspicious_windows,
  suspicious_clicks,
  suspicious_spend,
  avg_risk_score,
  updated_at
FROM fraud_signal_snapshot;

"@

$tempStarrocksSql = Join-Path ([System.IO.Path]::GetTempPath()) ("sync-starrocks-{0}.sql" -f ([guid]::NewGuid()))
try {
  Set-Content -Path $tempStarrocksSql -Value $starrocksSql -Encoding UTF8
  docker cp $tempStarrocksSql ustc_lakehouse-starrocks-1:/tmp/sync_starrocks_olap.sql | Out-Null
  $output = docker compose --profile olap exec -T starrocks bash -lc "mysql -h127.0.0.1 -P9030 -uroot < /tmp/sync_starrocks_olap.sql" 2>&1
  $output
  if ($LASTEXITCODE -ne 0 -or $output -match "ERROR") {
    throw "StarRocks OLAP sync failed. See mysql output above."
  }
} finally {
  Remove-Item -Path $tempStarrocksSql -ErrorAction SilentlyContinue
}

if ($offlineCreativeRows.Count -eq 0) {
  $supersetContainer = docker compose ps -q superset
  if (-not [string]::IsNullOrWhiteSpace($supersetContainer)) {
    Write-Host "Paimon offline creative ADS is empty; building completed-day fallback from the synchronized realtime facts."
    docker compose exec -T superset python /app/pythonpath/backfill_creative_offline_snapshot.py
    if ($LASTEXITCODE -ne 0) {
      throw "Creative offline fallback backfill failed."
    }
  } else {
    Write-Warning "Superset container is not running; skipped creative offline fallback backfill."
  }
}

Write-Host "Synced offline ADS snapshots: retention=$($retentionRows.Count), attribution=$($attributionRows.Count), attribution_details=$($attributionDetailRows.Count), offline_creatives=$($offlineCreativeRows.Count), fraud=$($fraudRows.Count). Real-time metrics remain owned by Routine Load."
