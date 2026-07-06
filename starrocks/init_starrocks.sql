CREATE DATABASE IF NOT EXISTS ad_ads;

DROP VIEW IF EXISTS ad_ads.v_realtime_ad_metrics;
DROP VIEW IF EXISTS ad_ads.v_advertiser_retention;
DROP VIEW IF EXISTS ad_ads.v_attribution_summary;
DROP VIEW IF EXISTS ad_ads.v_fraud_signal_summary;
DROP VIEW IF EXISTS ad_ads.v_data_quality_result;
DROP VIEW IF EXISTS ad_ads.v_data_quality_summary;

CREATE EXTERNAL CATALOG paimon_catalog
PROPERTIES (
  "type" = "paimon",
  "paimon.catalog.type" = "filesystem",
  "paimon.catalog.warehouse" = "file:///warehouse/paimon"
);

CREATE OR REPLACE VIEW ad_ads.v_realtime_ad_metrics AS
SELECT *
FROM paimon_catalog.ad_dw.dws_ad_metric_10s;

CREATE OR REPLACE VIEW ad_ads.v_advertiser_retention AS
SELECT *
FROM paimon_catalog.ad_dw.ads_advertiser_retention_di;

CREATE OR REPLACE VIEW ad_ads.v_attribution_summary AS
SELECT *
FROM paimon_catalog.ad_dw.ads_attribution_summary_di;

CREATE OR REPLACE VIEW ad_ads.v_fraud_signal_summary AS
SELECT *
FROM paimon_catalog.ad_dw.ads_fraud_signal_di;

CREATE OR REPLACE VIEW ad_ads.v_data_quality_result AS
SELECT *
FROM paimon_catalog.ad_dw.ads_data_quality_result_di;

CREATE OR REPLACE VIEW ad_ads.v_data_quality_summary AS
SELECT *
FROM paimon_catalog.ad_dw.ads_data_quality_summary_di;
