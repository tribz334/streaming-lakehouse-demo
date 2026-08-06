CREATE DATABASE IF NOT EXISTS ad_ads;

USE ad_ads;

DROP VIEW IF EXISTS v_realtime_ad_metrics_latest_10s;
DROP VIEW IF EXISTS v_realtime_ad_metrics_today;
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

-- Scene-aware real-time attribution table. The legacy table above is retained
-- so existing screenshots and rollback paths remain intact.
CREATE TABLE IF NOT EXISTS realtime_ad_attribution_metrics_10s (
  window_start DATETIME NOT NULL,
  advertiser_id VARCHAR(64) NOT NULL,
  campaign_id VARCHAR(64) NOT NULL,
  unit_id VARCHAR(64) NOT NULL,
  creative_id VARCHAR(64) NOT NULL,
  media VARCHAR(32) NOT NULL,
  commerce_scene VARCHAR(32) NOT NULL,
  window_end DATETIME NOT NULL,
  spend DECIMAL(18,4),
  order_gmv DECIMAL(18,2),
  attributed_gmv DECIMAL(18,2),
  organic_gmv DECIMAL(18,2),
  impressions BIGINT,
  clicks BIGINT,
  paid_orders BIGINT,
  attributed_orders BIGINT,
  organic_orders BIGINT,
  ctr DECIMAL(18,6),
  cvr DECIMAL(18,6),
  roi DECIMAL(18,6),
  updated_at DATETIME
)
PRIMARY KEY(
  window_start, advertiser_id, campaign_id, unit_id, creative_id,
  media, commerce_scene
)
DISTRIBUTED BY HASH(advertiser_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE OR REPLACE VIEW v_realtime_ad_metrics AS
SELECT
  DATE_SUB(window_start, INTERVAL 8 HOUR) AS window_start,
  window_start AS window_start_local,
  advertiser_id, campaign_id, unit_id, creative_id,
  media, commerce_scene,
  CASE commerce_scene
    WHEN 'live' THEN '直播'
    WHEN 'short_video' THEN '短视频'
    WHEN 'shop' THEN '电商'
    WHEN 'external' THEN '外循环'
    ELSE commerce_scene
  END AS commerce_scene_name,
  CASE WHEN commerce_scene = 'external' THEN '外循环' ELSE '内循环' END AS loop_type,
  DATE_SUB(window_end, INTERVAL 8 HOUR) AS window_end,
  spend,
  order_gmv, attributed_gmv, organic_gmv,
  attributed_gmv AS gmv,
  impressions, clicks, paid_orders,
  paid_orders AS conversions,
  paid_orders AS orders,
  attributed_orders, organic_orders,
  ctr, cvr, roi AS roas,
  DATE_SUB(updated_at, INTERVAL 8 HOUR) AS updated_at,
  previous_spend, previous_attributed_gmv AS previous_gmv,
  spend - previous_spend AS spend_change,
  attributed_gmv - previous_attributed_gmv AS gmv_change,
  (spend - previous_spend) / NULLIF(previous_spend, 0) AS spend_change_rate,
  (attributed_gmv - previous_attributed_gmv) /
    NULLIF(previous_attributed_gmv, 0) AS gmv_change_rate
FROM (
  SELECT
    window_start, advertiser_id, campaign_id, unit_id, creative_id,
    media, commerce_scene,
    window_end, spend,
    order_gmv, attributed_gmv, organic_gmv,
    impressions, clicks, paid_orders, attributed_orders, organic_orders,
    ctr, cvr, roi, updated_at,
    LAG(spend) OVER (
      PARTITION BY advertiser_id, campaign_id, unit_id, creative_id,
        media, commerce_scene
      ORDER BY window_start
    ) AS previous_spend,
    LAG(attributed_gmv) OVER (
      PARTITION BY advertiser_id, campaign_id, unit_id, creative_id,
        media, commerce_scene
      ORDER BY window_start
    ) AS previous_attributed_gmv
  FROM realtime_ad_attribution_metrics_10s
) window_metrics;

-- Business-day view for cumulative KPI cards. window_start_local is the
-- original Asia/Shanghai event-window timestamp written by Flink.
CREATE OR REPLACE VIEW v_realtime_ad_metrics_today AS
SELECT *
FROM v_realtime_ad_metrics
WHERE DATE(window_start_local) = DATE(DATE_ADD(NOW(), INTERVAL 8 HOUR));

CREATE OR REPLACE VIEW v_realtime_ad_metrics_latest_10s AS
SELECT *
FROM v_realtime_ad_metrics
WHERE window_start = (SELECT MAX(window_start) FROM v_realtime_ad_metrics);

-- DolphinScheduler seals the previous business day into this compact archive
-- at 00:05 Asia/Shanghai. The live dashboard still reads the current-day view.
CREATE TABLE IF NOT EXISTS realtime_ad_metrics_daily (
  stat_date DATE NOT NULL,
  loop_type VARCHAR(16) NOT NULL,
  commerce_scene VARCHAR(32) NOT NULL,
  spend DECIMAL(18,4) NOT NULL,
  attributed_gmv DECIMAL(18,2) NOT NULL,
  impressions BIGINT NOT NULL,
  clicks BIGINT NOT NULL,
  paid_orders BIGINT NOT NULL,
  updated_at DATETIME NOT NULL
)
PRIMARY KEY(stat_date, loop_type, commerce_scene)
DISTRIBUTED BY HASH(stat_date, loop_type, commerce_scene) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- Finalized daily facts used by the offline core dashboard. Every row has
-- already passed the 00:05 archive-before-delete rollover workflow.
CREATE OR REPLACE VIEW v_offline_core_metrics AS
SELECT
  stat_date,
  CASE WHEN loop_type = '自循环' THEN '内循环' ELSE loop_type END AS loop_type,
  commerce_scene,
  CASE commerce_scene
    WHEN 'live' THEN '直播'
    WHEN 'short_video' THEN '短视频'
    WHEN 'shop' THEN '电商'
    WHEN 'external' THEN '外循环'
    ELSE commerce_scene
  END AS commerce_scene_name,
  spend,
  attributed_gmv,
  impressions,
  clicks,
  paid_orders,
  updated_at
FROM realtime_ad_metrics_daily;

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
