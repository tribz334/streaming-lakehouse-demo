$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$outputDir = Join-Path $root "datahub/metadata"
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

$datasets = @(
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,ods_log,PROD)"; name = "ods_log"; platform = "kafka"; layer = "source" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ods_ad_events_di,PROD)"; name = "ad_dw.ods_ad_events_di"; platform = "paimon"; layer = "ods" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_advertiser_df,PROD)"; name = "ad_dw.dim_advertiser_df"; platform = "paimon"; layer = "dim" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_events_di,PROD)"; name = "ad_dw.dwd_ad_events_di"; platform = "paimon"; layer = "dwd" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_ad_metric_stream_10s,PROD)"; name = "ad_dw.dws_ad_metric_stream_10s"; platform = "paimon"; layer = "dws"; domain = "realtime_metrics" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_attribution_candidate_df,PROD)"; name = "ad_dw.dws_attribution_candidate_df"; platform = "paimon"; layer = "dws"; domain = "attribution" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_user_click_window_df,PROD)"; name = "ad_dw.dws_user_click_window_df"; platform = "paimon"; layer = "dws"; domain = "anti_fraud" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_attribution_touchpoint_df,PROD)"; name = "ad_dw.dm_attribution_touchpoint_df"; platform = "paimon"; layer = "dm"; domain = "attribution" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_antifraud_feature_df,PROD)"; name = "ad_dw.dm_antifraud_feature_df"; platform = "paimon"; layer = "dm"; domain = "anti_fraud" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_advertiser_retention_di,PROD)"; name = "ad_dw.ads_advertiser_retention_di"; platform = "paimon"; layer = "ads" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_attribution_summary_di,PROD)"; name = "ad_dw.ads_attribution_summary_di"; platform = "paimon"; layer = "ads"; domain = "attribution" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_fraud_signal_di,PROD)"; name = "ad_dw.ads_fraud_signal_di"; platform = "paimon"; layer = "ads"; domain = "anti_fraud" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_realtime_ad_metrics,PROD)"; name = "ad_ads.v_realtime_ad_metrics"; platform = "starrocks"; layer = "bi" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_advertiser_retention,PROD)"; name = "ad_ads.v_advertiser_retention"; platform = "starrocks"; layer = "bi" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_attribution_summary,PROD)"; name = "ad_ads.v_attribution_summary"; platform = "starrocks"; layer = "bi"; domain = "attribution" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_fraud_signal_summary,PROD)"; name = "ad_ads.v_fraud_signal_summary"; platform = "starrocks"; layer = "bi"; domain = "anti_fraud" }
)

$lineage = @(
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:kafka,ods_log,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ods_ad_events_di,PROD)"; job = "flink_02_realtime_ods" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ods_ad_events_di,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_events_di,PROD)"; job = "flink_03_realtime_dwd" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_events_di,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_ad_metric_stream_10s,PROD)"; job = "flink_04_realtime_dws_metrics" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_events_di,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_advertiser_retention_di,PROD)"; job = "flink_10_ads_retention" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_events_di,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_attribution_candidate_df,PROD)"; job = "flink_08_offline_dws" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_attribution_candidate_df,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_attribution_touchpoint_df,PROD)"; job = "flink_09_offline_dm" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_attribution_touchpoint_df,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_attribution_summary_di,PROD)"; job = "flink_11_ads_attribution" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_events_di,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_user_click_window_df,PROD)"; job = "flink_08_offline_dws" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_user_click_window_df,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_antifraud_feature_df,PROD)"; job = "flink_09_offline_dm" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_antifraud_feature_df,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_fraud_signal_di,PROD)"; job = "flink_12_ads_fraud" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_ad_metric_stream_10s,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_realtime_ad_metrics,PROD)"; job = "flink_05_realtime_starrocks_relay" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_attribution_summary_di,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_attribution_summary,PROD)"; job = "sync_starrocks_olap" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_fraud_signal_di,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_fraud_signal_summary,PROD)"; job = "sync_starrocks_olap" }
)

$metadata = [ordered]@{
  generated_at = (Get-Date).ToString("s")
  project = "ustc-streaming-lakehouse-demo"
  note = "DataHub-style offline metadata export. It can be converted to MCP/ingestion events when a DataHub service is available."
  datasets = $datasets
  lineage = $lineage
  glossary_terms = @(
    @{ term = "Last Click 7d Attribution"; applies_to = "ad_dw.ads_attribution_summary_di" },
    @{ term = "Demo Calibrated Fraud Signal"; applies_to = "ad_dw.ads_fraud_signal_di" },
    @{ term = "Advertiser Retention"; applies_to = "ad_dw.ads_advertiser_retention_di" }
  )
  schema_registry = @(
    @{
      registry = "Apicurio Registry 3.2.5"
      group_id = "ad-demo"
      artifact_id = "ods_log-value"
      artifact_type = "JSON"
      version = "1.0.0"
      topic = "ods_log"
      schema_file = "schemas/ods_log.schema.json"
    }
  )
}

$metadataPath = Join-Path $outputDir "lakehouse_metadata.json"
$metadata | ConvertTo-Json -Depth 8 | Set-Content -Path $metadataPath -Encoding UTF8
Write-Host "Exported governance metadata to $metadataPath"
