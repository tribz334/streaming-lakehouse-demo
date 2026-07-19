CREATE DATABASE IF NOT EXISTS ad_ads;

USE ad_ads;

DROP VIEW IF EXISTS v_realtime_ad_metrics;
DROP VIEW IF EXISTS v_advertiser_retention;
DROP VIEW IF EXISTS v_attribution_summary;
DROP VIEW IF EXISTS v_order_attribution_detail;
DROP VIEW IF EXISTS v_creative_offline_metrics;
DROP VIEW IF EXISTS v_fraud_signal_summary;
DROP VIEW IF EXISTS v_dwd_ad_events_detail;
DROP VIEW IF EXISTS v_dwm_ad_event_wide;

CREATE TABLE IF NOT EXISTS realtime_ad_metrics_10s (
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

CREATE OR REPLACE VIEW v_realtime_ad_metrics AS
SELECT
  window_start, advertiser_id, campaign_id, unit_id, creative_id,
  window_end, advertiser_name, spend, gmv, impressions, clicks,
  conversions, orders, ctr, cvr, roi AS roas, updated_at,
  previous_spend, previous_gmv,
  spend - previous_spend AS spend_change,
  gmv - previous_gmv AS gmv_change,
  (spend - previous_spend) / NULLIF(previous_spend, 0) AS spend_change_rate,
  (gmv - previous_gmv) / NULLIF(previous_gmv, 0) AS gmv_change_rate
FROM (
  SELECT
    window_start, advertiser_id, campaign_id, unit_id, creative_id,
    window_end, advertiser_name, spend, gmv, impressions, clicks,
    conversions, orders, ctr, cvr, roi, updated_at,
    LAG(spend) OVER (
      PARTITION BY advertiser_id, campaign_id, unit_id, creative_id
      ORDER BY window_start
    ) AS previous_spend,
    LAG(gmv) OVER (
      PARTITION BY advertiser_id, campaign_id, unit_id, creative_id
      ORDER BY window_start
    ) AS previous_gmv
  FROM realtime_ad_metrics_10s
) window_metrics;

-- The Java Flink job writes each finalized 10-second window directly to this
-- Primary Key table. The former Kafka relay and Routine Load are no longer in
-- the realtime hot path.

CREATE EXTERNAL CATALOG paimon_catalog
PROPERTIES (
  "type" = "paimon",
  "paimon.catalog.type" = "filesystem",
  "paimon.catalog.warehouse" = "file:///warehouse/paimon"
);

-- The external catalog can be queried directly when the StarRocks/Paimon
-- reader versions are compatible. sync-starrocks-olap.ps1 still creates
-- internal snapshot tables and BI views for faster dashboards and demos.
