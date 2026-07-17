-- Streamlined thesis-side physical tables that are still consumed by the demo.
SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '1';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:///warehouse/paimon'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ad_dw;
USE ad_dw;

CREATE TABLE IF NOT EXISTS dws_creative_df (
  stat_date STRING, creative_id STRING, campaign_id STRING, creative_type STRING,
  imp_cnt_1d BIGINT, click_cnt_1d BIGINT, conv_cnt_1d BIGINT,
  cost_1d DECIMAL(18,2), gmv_1d DECIMAL(18,2), cost_7d DECIMAL(18,2),
  gmv_7d DECIMAL(18,2), cost_total DECIMAL(18,2), ctr_1d DECIMAL(8,6),
  cvr_1d DECIMAL(8,6), roi_1d DECIMAL(10,4), order_cnt_1d BIGINT,
  PRIMARY KEY (stat_date,creative_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

-- Order/click candidate pairs. DWS performs the reusable 30-day temporal join;
-- DM only selects and scores the final attribution touchpoint.
CREATE TABLE IF NOT EXISTS dws_attribution_candidate_df (
  stat_date STRING, candidate_id STRING, outcome_event_id STRING, order_id STRING,
  outcome_time TIMESTAMP(3), user_id STRING,
  order_advertiser_id STRING, order_advertiser_name STRING,
  order_campaign_id STRING, order_campaign_name STRING, order_gmv DECIMAL(18,2),
  touch_event_id STRING, touch_time TIMESTAMP(3), creative_id STRING,
  campaign_id STRING, campaign_name STRING, advertiser_id STRING,
  advertiser_name STRING, touch_spend DECIMAL(18,4),
  touchpoint_seq INT, lag_minutes BIGINT,
  PRIMARY KEY (stat_date,candidate_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

-- One row per click with rolling traffic features. The source has no real IP,
-- device or UA fields, so user_id/media are retained as explicit demo proxies.
CREATE TABLE IF NOT EXISTS dws_user_click_window_df (
  stat_date STRING, event_id STRING, event_time TIMESTAMP(3), user_id STRING,
  device_id STRING, device_ip STRING, creative_id STRING, slot_id STRING,
  advertiser_id STRING, advertiser_name STRING, media STRING, spend DECIMAL(18,4),
  click_cnt_1h INT, click_cnt_1d INT, impression_cnt_1h INT,
  impression_cnt_1m INT, ip_click_cnt_1h INT, ip_uv_1h INT,
  click_interval_ms BIGINT, ctr_deviation DECIMAL(10,6), is_night_burst BOOLEAN,
  PRIMARY KEY (stat_date,event_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dm_attribution_touchpoint_df (
  attribution_id STRING, stat_date STRING, conversion_id STRING, order_id STRING,
  user_id STRING, touchpoint_seq INT, touchpoint_type STRING,
  touchpoint_time TIMESTAMP(3), creative_id STRING, campaign_id STRING,
  advertiser_id STRING, is_last_click BOOLEAN, attribution_model STRING,
  attribution_weight DECIMAL(10,6), attributed_gmv DECIMAL(18,4),
  attributed_conv DECIMAL(10,6), lookback_days INT,
  PRIMARY KEY (stat_date,attribution_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dm_antifraud_feature_df (
  stat_date STRING, event_id STRING, user_id STRING, device_id STRING,
  device_ip STRING, creative_id STRING, slot_id STRING, click_cnt_1h INT,
  click_cnt_1d INT, ip_click_cnt_1h INT, ip_uv_1h INT,
  click_interval_ms BIGINT, ctr_deviation DECIMAL(10,6), ua_entropy DECIMAL(10,6),
  geo_ip_mismatch BOOLEAN, is_night_burst BOOLEAN, fraud_score DECIMAL(6,4),
  fraud_label STRING, rule_hits ARRAY<STRING>,
  PRIMARY KEY (stat_date,event_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');
