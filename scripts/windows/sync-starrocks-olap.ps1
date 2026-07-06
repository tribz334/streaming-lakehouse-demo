$ErrorActionPreference = "Stop"
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

  $tempFile = Join-Path ([System.IO.Path]::GetTempPath()) ("flink-export-{0}.sql" -f ([guid]::NewGuid()))
  try {
    Set-Content -Path $tempFile -Value $sql -Encoding UTF8
    docker cp $tempFile ustc_lakehouse-flink-jobmanager-1:/tmp/starrocks_export.sql | Out-Null
    $output = @(docker compose --profile core exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f /tmp/starrocks_export.sql 2>&1)
    if ($LASTEXITCODE -ne 0 -or $output -match "\[ERROR\]") {
      $output
      throw "Flink export query failed."
    }
    return $output
  } finally {
    Remove-Item -Path $tempFile -ErrorAction SilentlyContinue
  }
}

function ConvertFrom-Tableau {
  param(
    [AllowEmptyCollection()][AllowEmptyString()][string[]]$Output,
    [Parameter(Mandatory = $true)][string[]]$Columns
  )

  $rows = @()
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
    $rows += [pscustomobject]$row
  }
  return $rows
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

$metricColumns = @("window_start", "advertiser_id", "campaign_id", "unit_id", "creative_id", "window_end", "advertiser_name", "spend", "gmv", "impressions", "clicks", "conversions", "orders", "ctr", "cvr", "roi", "updated_at")
$metricOutput = Invoke-FlinkQuery @"
SELECT
  CAST(window_start AS STRING) AS window_start,
  advertiser_id,
  campaign_id,
  unit_id,
  creative_id,
  CAST(window_end AS STRING) AS window_end,
  advertiser_name,
  CAST(spend AS STRING) AS spend,
  CAST(gmv AS STRING) AS gmv,
  CAST(impressions AS STRING) AS impressions,
  CAST(clicks AS STRING) AS clicks,
  CAST(conversions AS STRING) AS conversions,
  CAST(orders AS STRING) AS orders,
  CAST(ctr AS STRING) AS ctr,
  CAST(cvr AS STRING) AS cvr,
  CAST(roi AS STRING) AS roi,
  CAST(updated_at AS STRING) AS updated_at
FROM paimon.ad_dw.dws_ad_metric_10s
ORDER BY window_start, advertiser_id, campaign_id, unit_id, creative_id;
"@
$metricRows = ConvertFrom-Tableau -Output $metricOutput -Columns $metricColumns

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

$qualityResultColumns = @("check_date", "rule_code", "rule_name", "data_layer", "target_table", "actual_value", "expected_value", "check_status", "severity", "details", "checked_at")
$qualityResultOutput = Invoke-FlinkQuery @"
SELECT check_date, rule_code, rule_name, data_layer, target_table,
  CAST(actual_value AS STRING) AS actual_value,
  expected_value, check_status, severity, details,
  CAST(checked_at AS STRING) AS checked_at
FROM paimon.ad_dw.ads_data_quality_result_di
ORDER BY rule_code;
"@
$qualityResultRows = ConvertFrom-Tableau -Output $qualityResultOutput -Columns $qualityResultColumns

$qualitySummaryColumns = @("check_date", "total_rules", "passed_rules", "failed_rules", "quality_score", "overall_status", "checked_at")
$qualitySummaryOutput = Invoke-FlinkQuery @"
SELECT check_date,
  CAST(total_rules AS STRING) AS total_rules,
  CAST(passed_rules AS STRING) AS passed_rules,
  CAST(failed_rules AS STRING) AS failed_rules,
  CAST(quality_score AS STRING) AS quality_score,
  overall_status,
  CAST(checked_at AS STRING) AS checked_at
FROM paimon.ad_dw.ads_data_quality_summary_di
ORDER BY check_date;
"@
$qualitySummaryRows = ConvertFrom-Tableau -Output $qualitySummaryOutput -Columns $qualitySummaryColumns

if ($metricRows.Count -eq 0) {
  throw "No metric rows exported from Paimon."
}

$starrocksSql = @"
CREATE DATABASE IF NOT EXISTS ad_ads;
USE ad_ads;

DROP TABLE IF EXISTS realtime_ad_metrics_snapshot;
CREATE TABLE realtime_ad_metrics_snapshot (
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
DUPLICATE KEY(window_start, advertiser_id, campaign_id, unit_id, creative_id)
DISTRIBUTED BY HASH(advertiser_id) BUCKETS 4
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

DROP TABLE IF EXISTS data_quality_result_snapshot;
CREATE TABLE data_quality_result_snapshot (
  check_date VARCHAR(32) NOT NULL,
  rule_code VARCHAR(32) NOT NULL,
  rule_name VARCHAR(255),
  data_layer VARCHAR(32),
  target_table VARCHAR(255),
  actual_value DECIMAL(24,6),
  expected_value VARCHAR(255),
  check_status VARCHAR(32),
  severity VARCHAR(32),
  details VARCHAR(1024),
  checked_at DATETIME
)
DUPLICATE KEY(check_date, rule_code)
DISTRIBUTED BY HASH(rule_code) BUCKETS 2
PROPERTIES ("replication_num" = "1");

DROP TABLE IF EXISTS data_quality_summary_snapshot;
CREATE TABLE data_quality_summary_snapshot (
  check_date VARCHAR(32) NOT NULL,
  total_rules BIGINT,
  passed_rules BIGINT,
  failed_rules BIGINT,
  quality_score DECIMAL(18,2),
  overall_status VARCHAR(32),
  checked_at DATETIME
)
DUPLICATE KEY(check_date)
DISTRIBUTED BY HASH(check_date) BUCKETS 1
PROPERTIES ("replication_num" = "1");
"@

if ($metricRows.Count -gt 0) {
  $metricValues = @($metricRows | ForEach-Object {
    "(" + (@(
      SqlString $_.window_start
      SqlString $_.advertiser_id
      SqlString $_.campaign_id
      SqlString $_.unit_id
      SqlString $_.creative_id
      SqlString $_.window_end
      SqlString $_.advertiser_name
      SqlNumber $_.spend
      SqlNumber $_.gmv
      SqlNumber $_.impressions
      SqlNumber $_.clicks
      SqlNumber $_.conversions
      SqlNumber $_.orders
      SqlNumber $_.ctr
      SqlNumber $_.cvr
      SqlNumber $_.roi
      SqlString $_.updated_at
    ) -join ", ") + ")"
  })
  $batchSize = 5000
  for ($offset = 0; $offset -lt $metricValues.Count; $offset += $batchSize) {
    $end = [Math]::Min($offset + $batchSize - 1, $metricValues.Count - 1)
    $batch = $metricValues[$offset..$end]
    $starrocksSql += "`nINSERT INTO realtime_ad_metrics_snapshot VALUES`n" + ($batch -join ",`n") + ";`n"
  }
}

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

if ($qualityResultRows.Count -gt 0) {
  $values = $qualityResultRows | ForEach-Object {
    "(" + (@(
      SqlString $_.check_date
      SqlString $_.rule_code
      SqlString $_.rule_name
      SqlString $_.data_layer
      SqlString $_.target_table
      SqlNumber $_.actual_value
      SqlString $_.expected_value
      SqlString $_.check_status
      SqlString $_.severity
      SqlString $_.details
      SqlString $_.checked_at
    ) -join ", ") + ")"
  }
  $starrocksSql += "`nINSERT INTO data_quality_result_snapshot VALUES`n" + ($values -join ",`n") + ";`n"
}

if ($qualitySummaryRows.Count -gt 0) {
  $values = $qualitySummaryRows | ForEach-Object {
    "(" + (@(
      SqlString $_.check_date
      SqlNumber $_.total_rules
      SqlNumber $_.passed_rules
      SqlNumber $_.failed_rules
      SqlNumber $_.quality_score
      SqlString $_.overall_status
      SqlString $_.checked_at
    ) -join ", ") + ")"
  }
  $starrocksSql += "`nINSERT INTO data_quality_summary_snapshot VALUES`n" + ($values -join ",`n") + ";`n"
}

$starrocksSql += @"

CREATE OR REPLACE VIEW v_realtime_ad_metrics AS
SELECT * FROM realtime_ad_metrics_snapshot;

CREATE OR REPLACE VIEW v_advertiser_retention AS
SELECT * FROM advertiser_retention_snapshot;

CREATE OR REPLACE VIEW v_attribution_summary AS
SELECT * FROM attribution_summary_snapshot;

CREATE OR REPLACE VIEW v_fraud_signal_summary AS
SELECT * FROM fraud_signal_snapshot;

CREATE OR REPLACE VIEW v_data_quality_result AS
SELECT * FROM data_quality_result_snapshot;

CREATE OR REPLACE VIEW v_data_quality_summary AS
SELECT * FROM data_quality_summary_snapshot;
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

Write-Host "Synced metrics=$($metricRows.Count), retention=$($retentionRows.Count), attribution=$($attributionRows.Count), fraud=$($fraudRows.Count), quality_rules=$($qualityResultRows.Count), quality_summaries=$($qualitySummaryRows.Count) into StarRocks."
