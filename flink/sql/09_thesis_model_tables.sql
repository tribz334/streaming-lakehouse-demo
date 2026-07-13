-- Canonical physical tables from thesis Appendix A.
-- This file is additive so existing demo snapshots remain readable.
SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '1';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:///warehouse/paimon'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ad_dw;
USE ad_dw;

CREATE TABLE IF NOT EXISTS dim_customer_df (
  customer_id STRING, customer_name STRING, industry_code STRING,
  industry_name STRING, sub_industry STRING, customer_level STRING,
  sales_owner STRING, region_code STRING, credit_limit DECIMAL(18,2),
  sign_date DATE, status STRING, effective_from TIMESTAMP(3),
  effective_to TIMESTAMP(3), is_current BOOLEAN, version_no BIGINT,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH ('bucket'='2','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dim_slot_df (
  slot_id STRING, media_id STRING, media_name STRING, app_bundle STRING,
  slot_name STRING, slot_type STRING, device_platform STRING,
  floor_price DECIMAL(10,4), status STRING, effective_from TIMESTAMP(3),
  effective_to TIMESTAMP(3), is_current BOOLEAN, version_no BIGINT,
  PRIMARY KEY (slot_id) NOT ENFORCED
) WITH ('bucket'='2','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dim_user_df (
  user_id STRING, device_id STRING, gender STRING, age_range STRING,
  city_code STRING, os_type STRING, first_active_date DATE,
  user_tags ARRAY<STRING>, consumer_level STRING, effective_from TIMESTAMP(3),
  effective_to TIMESTAMP(3), is_current BOOLEAN, version_no BIGINT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dim_shop_df (
  shop_id STRING, shop_name STRING, advertiser_id STRING, shop_type STRING,
  main_category STRING, shop_level STRING, open_date DATE, status STRING,
  effective_from TIMESTAMP(3), effective_to TIMESTAMP(3),
  is_current BOOLEAN, version_no BIGINT,
  PRIMARY KEY (shop_id) NOT ENFORCED
) WITH ('bucket'='2','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dim_product_df (
  product_id STRING, shop_id STRING, product_name STRING, category_l1 STRING,
  category_l2 STRING, brand STRING, price DECIMAL(10,2), sku_count INT,
  online_date DATE, status STRING, effective_from TIMESTAMP(3),
  effective_to TIMESTAMP(3), is_current BOOLEAN, version_no BIGINT,
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH ('bucket'='2','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dwd_ad_bid_di (
  bid_id STRING, event_time TIMESTAMP(3), event_date STRING,
  advertiser_id STRING, campaign_id STRING, unit_id STRING,
  creative_id STRING, slot_id STRING, user_id STRING,
  bid_price DECIMAL(10,4), win_price DECIMAL(10,4), is_win BOOLEAN,
  device_ip STRING, PRIMARY KEY (event_date,bid_id) NOT ENFORCED
) PARTITIONED BY (event_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dwd_ad_impression_di (
  impression_id STRING, bid_id STRING, event_time TIMESTAMP(3), event_date STRING,
  advertiser_id STRING, campaign_id STRING, unit_id STRING,
  creative_id STRING, slot_id STRING, user_id STRING,
  view_duration_ms INT, is_viewable BOOLEAN,
  PRIMARY KEY (event_date,impression_id) NOT ENFORCED
) PARTITIONED BY (event_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dwd_ad_click_di (
  click_id STRING, impression_id STRING, event_time TIMESTAMP(3), event_date STRING,
  advertiser_id STRING, campaign_id STRING, unit_id STRING,
  creative_id STRING, slot_id STRING, user_id STRING,
  device_ip STRING, user_agent STRING, is_valid BOOLEAN,
  PRIMARY KEY (event_date,click_id) NOT ENFORCED
) PARTITIONED BY (event_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dwd_ad_conversion_di (
  conversion_id STRING, click_id STRING, event_time TIMESTAMP(3), event_date STRING,
  advertiser_id STRING, campaign_id STRING, unit_id STRING,
  creative_id STRING, slot_id STRING, user_id STRING, conversion_type STRING,
  conversion_value DECIMAL(18,2), order_id STRING, attribution_window INT,
  PRIMARY KEY (event_date,conversion_id) NOT ENFORCED
) PARTITIONED BY (event_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dwd_ad_cost_di (
  cost_id STRING, event_time TIMESTAMP(3), event_date STRING,
  advertiser_id STRING, campaign_id STRING, unit_id STRING,
  creative_id STRING, slot_id STRING, billing_type STRING, ref_event_id STRING,
  raw_cost DECIMAL(18,4), adjusted_cost DECIMAL(18,4), currency STRING,
  PRIMARY KEY (event_date,cost_id) NOT ENFORCED
) PARTITIONED BY (event_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dwm_ad_event_wide (
  event_id STRING, event_type STRING, event_time TIMESTAMP(3), event_date STRING,
  advertiser_id STRING, advertiser_name STRING, industry_code STRING,
  campaign_id STRING, campaign_name STRING, marketing_goal STRING,
  creative_id STRING, creative_type STRING, unit_id STRING, slot_id STRING,
  media_id STRING, slot_type STRING, user_id STRING, city_code STRING,
  device_platform STRING, impression_flag TINYINT, click_flag TINYINT,
  conversion_flag TINYINT, billing_type STRING, actual_cost DECIMAL(18,4),
  conversion_value DECIMAL(18,2), order_id STRING, gmv DECIMAL(18,2),
  attributed_date STRING,
  PRIMARY KEY (event_date,event_type,event_id) NOT ENFORCED
) PARTITIONED BY (event_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dws_advertiser_df (
  stat_date STRING, advertiser_id STRING, industry_code STRING,
  imp_cnt_1d BIGINT, click_cnt_1d BIGINT, conv_cnt_1d BIGINT,
  cost_1d DECIMAL(18,2), gmv_1d DECIMAL(18,2), imp_cnt_7d BIGINT,
  click_cnt_7d BIGINT, cost_7d DECIMAL(18,2), gmv_7d DECIMAL(18,2),
  cost_30d DECIMAL(18,2), gmv_30d DECIMAL(18,2), cost_total DECIMAL(18,2),
  gmv_total DECIMAL(18,2), ecpm_1d DECIMAL(10,4), ctr_1d DECIMAL(8,6),
  cvr_1d DECIMAL(8,6), roi_1d DECIMAL(10,4), first_pay_date STRING,
  last_pay_date STRING, PRIMARY KEY (stat_date,advertiser_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dws_campaign_df (
  stat_date STRING, campaign_id STRING, advertiser_id STRING,
  imp_cnt_1d BIGINT, click_cnt_1d BIGINT, conv_cnt_1d BIGINT,
  cost_1d DECIMAL(18,2), cost_7d DECIMAL(18,2), cost_30d DECIMAL(18,2),
  cost_total DECIMAL(18,2), ctr_1d DECIMAL(8,6), cvr_1d DECIMAL(8,6),
  cpc_1d DECIMAL(10,4), cpa_1d DECIMAL(10,4), budget_use_rate_1d DECIMAL(8,6),
  PRIMARY KEY (stat_date,campaign_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dws_creative_df (
  stat_date STRING, creative_id STRING, campaign_id STRING, creative_type STRING,
  imp_cnt_1d BIGINT, click_cnt_1d BIGINT, conv_cnt_1d BIGINT,
  cost_1d DECIMAL(18,2), gmv_1d DECIMAL(18,2), cost_7d DECIMAL(18,2),
  gmv_7d DECIMAL(18,2), cost_total DECIMAL(18,2), ctr_1d DECIMAL(8,6),
  cvr_1d DECIMAL(8,6), roi_1d DECIMAL(10,4), order_cnt_1d BIGINT,
  PRIMARY KEY (stat_date,creative_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dws_slot_df (
  stat_date STRING, slot_id STRING, media_id STRING, slot_type STRING,
  device_platform STRING, bid_cnt_1d BIGINT, win_cnt_1d BIGINT,
  imp_cnt_1d BIGINT, click_cnt_1d BIGINT, cost_1d DECIMAL(18,2),
  win_rate_1d DECIMAL(8,6), ecpm_1d DECIMAL(10,4), fill_rate_1d DECIMAL(8,6),
  PRIMARY KEY (stat_date,slot_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dws_user_df (
  stat_date STRING, user_id STRING, city_code STRING, imp_cnt_1d BIGINT,
  click_cnt_1d BIGINT, conv_cnt_1d BIGINT, order_cnt_1d BIGINT,
  gmv_1d DECIMAL(18,2), active_days_30d INT, last_active_date STRING,
  gmv_total DECIMAL(18,2), PRIMARY KEY (stat_date,user_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dws_region_df (
  stat_date STRING, region_code STRING, region_name STRING,
  imp_cnt_1d BIGINT, click_cnt_1d BIGINT, conv_cnt_1d BIGINT,
  cost_1d DECIMAL(18,2), gmv_1d DECIMAL(18,2), uv_1d BIGINT,
  ctr_1d DECIMAL(8,6), cvr_1d DECIMAL(8,6),
  PRIMARY KEY (stat_date,region_code) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dws_ad_stream_10s (
  window_start TIMESTAMP(3), window_end TIMESTAMP(3), advertiser_id STRING,
  imp_cnt BIGINT, click_cnt BIGINT, conv_cnt BIGINT, cost DECIMAL(18,4),
  gmv DECIMAL(18,2), ctr DECIMAL(8,6), cvr DECIMAL(8,6),
  PRIMARY KEY (window_start,window_end,advertiser_id) NOT ENFORCED
) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

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
