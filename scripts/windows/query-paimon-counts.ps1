$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$ddl = Get-Content -Raw -Path (Join-Path $root "flink/sql/00_catalogs_and_tables.sql")
$queries = @'
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT 'ods_ad_events_di' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.ods_ad_events_di;
SELECT 'dwd_ad_events_di' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dwd_ad_events_di;
SELECT 'dws_ad_metric_10s' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dws_ad_metric_10s;
SELECT 'dws_ad_metric_stream_10s' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dws_ad_metric_stream_10s;
SELECT 'dws_ad_stream_10s' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dws_ad_stream_10s;
SELECT 'dwm_ad_event_wide' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dwm_ad_event_wide;
SELECT 'dws_advertiser_df' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dws_advertiser_df;
SELECT 'dws_campaign_df' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dws_campaign_df;
SELECT 'dws_creative_df' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dws_creative_df;
SELECT 'dws_slot_df' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dws_slot_df;
SELECT 'dws_user_df' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dws_user_df;
SELECT 'dws_region_df' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dws_region_df;
SELECT 'dm_attribution_touchpoint_df' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dm_attribution_touchpoint_df;
SELECT 'dm_antifraud_feature_df' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dm_antifraud_feature_df;
SELECT 'ads_data_quality_result_di' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.ads_data_quality_result_di;
SELECT 'ads_data_quality_summary_di' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.ads_data_quality_summary_di;
SELECT 'ads_advertiser_retention_di' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.ads_advertiser_retention_di;
SELECT 'ads_attribution_summary_di' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.ads_attribution_summary_di;
SELECT 'ads_fraud_signal_di' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.ads_fraud_signal_di;
SELECT 'dim_advertiser_df' AS table_name, COUNT(*) AS cnt FROM paimon.ad_dw.dim_advertiser_df;
'@

$tempFile = Join-Path ([System.IO.Path]::GetTempPath()) ("paimon-counts-{0}.sql" -f ([guid]::NewGuid()))
try {
  Set-Content -Path $tempFile -Value ($ddl + "`n" + $queries) -Encoding UTF8
  docker cp $tempFile ustc_lakehouse-flink-jobmanager-1:/tmp/query_paimon_counts.sql
  docker compose --profile core exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f /tmp/query_paimon_counts.sql
} finally {
  Remove-Item -Path $tempFile -ErrorAction SilentlyContinue
}
