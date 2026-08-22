$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$outputDir = Join-Path $root "datahub/metadata"
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

$datasets = @(
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,ods_log,PROD)"; name = "ods_log"; platform = "kafka"; layer = "source" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ods_log_inc,PROD)"; name = "ad_dw.ods_log_inc"; platform = "paimon"; layer = "ods" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,ad_ods.master_data,PROD)"; name = "ad_ods master tables"; platform = "mysql"; layer = "source" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,ad_ods.bill_detail,PROD)"; name = "ad_ods.bill_detail"; platform = "mysql"; layer = "source" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,ad_ods.order_detail,PROD)"; name = "ad_ods.order_detail"; platform = "mysql"; layer = "source" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_advertiser_zip,PROD)"; name = "ad_dw.dim_advertiser_zip"; platform = "paimon"; layer = "dim" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_campaign,PROD)"; name = "ad_dw.dim_campaign"; platform = "paimon"; layer = "dim" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_unit,PROD)"; name = "ad_dw.dim_unit"; platform = "paimon"; layer = "dim" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_creative,PROD)"; name = "ad_dw.dim_creative"; platform = "paimon"; layer = "dim" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_user_zip,PROD)"; name = "ad_dw.dim_user_zip"; platform = "paimon"; layer = "dim" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_shop_zip,PROD)"; name = "ad_dw.dim_shop_zip"; platform = "paimon"; layer = "dim"; domain = "commerce" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_product_zip,PROD)"; name = "ad_dw.dim_product_zip"; platform = "paimon"; layer = "dim"; domain = "commerce" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_action_log_inc,PROD)"; name = "ad_dw.dwd_ad_action_log_inc"; platform = "paimon"; layer = "dwd" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_bill_detail_inc,PROD)"; name = "ad_dw.dwd_ad_bill_detail_inc"; platform = "paimon"; layer = "dwd"; domain = "billing" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_order_detail_acc,PROD)"; name = "ad_dw.dwd_order_detail_acc"; platform = "paimon"; layer = "dwd"; domain = "order_lifecycle" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_creative,PROD)"; name = "ad_dw.dws_creative"; platform = "paimon"; layer = "dws"; domain = "creative_analysis" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_unit,PROD)"; name = "ad_dw.dws_unit"; platform = "paimon"; layer = "dws" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_campaign,PROD)"; name = "ad_dw.dws_campaign"; platform = "paimon"; layer = "dws" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_advertiser,PROD)"; name = "ad_dw.dws_advertiser"; platform = "paimon"; layer = "dws" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_antifraud_feature,PROD)"; name = "ad_dw.dm_antifraud_feature"; platform = "paimon"; layer = "dm"; domain = "anti_fraud" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_creative,PROD)"; name = "ad_dw.dm_creative"; platform = "paimon"; layer = "dm"; domain = "creative_analysis" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_unit,PROD)"; name = "ad_dw.dm_unit"; platform = "paimon"; layer = "dm" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_campaign,PROD)"; name = "ad_dw.dm_campaign"; platform = "paimon"; layer = "dm" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_advertiser,PROD)"; name = "ad_dw.dm_advertiser"; platform = "paimon"; layer = "dm" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_advertiser_retention_di,PROD)"; name = "ad_dw.ads_advertiser_retention_di"; platform = "paimon"; layer = "ads" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_order_attribution_detail_di,PROD)"; name = "ad_dw.ads_order_attribution_detail_di"; platform = "paimon"; layer = "ads"; domain = "attribution" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_attribution_summary_di,PROD)"; name = "ad_dw.ads_attribution_summary_di"; platform = "paimon"; layer = "ads"; domain = "attribution" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_fraud_signal_di,PROD)"; name = "ad_dw.ads_fraud_signal_di"; platform = "paimon"; layer = "ads"; domain = "anti_fraud" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_creative_offline_di,PROD)"; name = "ad_dw.ads_creative_offline_di"; platform = "paimon"; layer = "ads"; domain = "creative_analysis" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_realtime_ad_metrics,PROD)"; name = "ad_ads.v_realtime_ad_metrics"; platform = "starrocks"; layer = "bi" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_advertiser_retention,PROD)"; name = "ad_ads.v_advertiser_retention"; platform = "starrocks"; layer = "bi" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_order_attribution_detail,PROD)"; name = "ad_ads.v_order_attribution_detail"; platform = "starrocks"; layer = "bi"; domain = "attribution" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_attribution_summary,PROD)"; name = "ad_ads.v_attribution_summary"; platform = "starrocks"; layer = "bi"; domain = "attribution" },
  @{ urn = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_fraud_signal_summary,PROD)"; name = "ad_ads.v_fraud_signal_summary"; platform = "starrocks"; layer = "bi"; domain = "anti_fraud" }
)

$lineage = @(
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:mysql,ad_ods.master_data,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_advertiser_zip,PROD)"; job = "flink_05_mysql_cdc_direct_to_dim" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:mysql,ad_ods.master_data,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_shop_zip,PROD)"; job = "flink_05_mysql_cdc_direct_to_dim" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:mysql,ad_ods.bill_detail,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_bill_detail_inc,PROD)"; job = "flink_04_dwd_ad_facts_to_paimon" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:mysql,ad_ods.order_detail,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_order_detail_acc,PROD)"; job = "flink_02_dwd_order_lifecycle" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_advertiser_zip,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_creative_offline_di,PROD)"; job = "flink_13_ads_creative_offline" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_campaign,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_creative_offline_di,PROD)"; job = "flink_13_ads_creative_offline" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_unit,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_creative_offline_di,PROD)"; job = "flink_13_ads_creative_offline" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_creative,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_creative_offline_di,PROD)"; job = "flink_13_ads_creative_offline" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:kafka,ods_log,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ods_log_inc,PROD)"; job = "flink_04_ods_log_and_dwd_ad_facts" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ods_log_inc,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_action_log_inc,PROD)"; job = "flink_04_ods_log_and_dwd_ad_facts" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_action_log_inc,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_creative,PROD)"; job = "flink_08_offline_dws" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_bill_detail_inc,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_creative,PROD)"; job = "flink_08_offline_dws" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_order_detail_acc,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_creative,PROD)"; job = "flink_08_offline_dws" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_creative,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_creative,PROD)"; job = "flink_09_offline_dm" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dm_creative,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_creative_offline_di,PROD)"; job = "flink_13_ads_creative_offline" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dws_advertiser,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_advertiser_retention_di,PROD)"; job = "flink_10_ads_retention" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_order_detail_acc,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_order_attribution_detail_di,PROD)"; job = "flink_11_ads_order_attribution" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dwd_ad_action_log_inc,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_order_attribution_detail_di,PROD)"; job = "flink_11_ads_order_attribution" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.dim_unit,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_order_attribution_detail_di,PROD)"; job = "flink_11_ads_order_attribution" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_order_attribution_detail_di,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_attribution_summary_di,PROD)"; job = "flink_11_ads_order_attribution" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:kafka,ods_log,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_realtime_ad_metrics,PROD)"; job = "flink_java_realtime_ad_metric" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_advertiser_retention_di,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_advertiser_retention,PROD)"; job = "sync_starrocks_olap" },
  @{ upstream = "urn:li:dataset:(urn:li:dataPlatform:paimon,ad_dw.ads_order_attribution_detail_di,PROD)"; downstream = "urn:li:dataset:(urn:li:dataPlatform:starrocks,ad_ads.v_order_attribution_detail,PROD)"; job = "sync_starrocks_olap" },
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
    @{ term = "Last Click 30d Attribution"; applies_to = "ad_dw.ads_attribution_summary_di" },
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
