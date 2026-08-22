SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '1';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://hive-metastore:9083',
  'warehouse' = 'file:///warehouse/paimon'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ad_dw;
USE ad_dw;

-- MySQL master data is synchronized directly by Flink CDC. These are current
-- primary-key tables; Paimon daily tags provide historical snapshots.
CREATE TABLE IF NOT EXISTS dim_advertiser_zip (
  advertiser_id BIGINT,
  advertiser_name STRING,
  qualification_type INT,
  status INT,
  industry_l1_id BIGINT,
  industry_l1_name STRING,
  industry_l2_id BIGINT,
  industry_l2_name STRING,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (advertiser_id) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input',
  'tag.automatic-creation' = 'process-time',
  'tag.creation-period' = 'daily',
  'tag.num-retained-max' = '35',
  'tag.default-time-retained' = '30 d'
);

CREATE TABLE IF NOT EXISTS dim_campaign (
  campaign_id BIGINT,
  campaign_name STRING,
  advertiser_id BIGINT,
  advertiser_name STRING,
  status INT,
  market_goal INT,
  ad_type INT,
  trading_mode INT,
  budget BIGINT,
  daily_budget BIGINT,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (campaign_id) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input','tag.automatic-creation'='process-time',
  'tag.creation-period'='daily','tag.num-retained-max'='35',
  'tag.default-time-retained'='30 d'
);

CREATE TABLE IF NOT EXISTS dim_creative (
  creative_id BIGINT,
  creative_name STRING,
  unit_id BIGINT,
  unit_name STRING,
  status INT,
  creative_mode INT,
  material_mode INT,
  creative_title STRING,
  creative_category STRING,
  creative_tags ARRAY<STRING>,
  creative_text STRING,
  creative_image_urls STRING,
  creative_video_id BIGINT,
  monitoring_url STRING,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (creative_id) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input','tag.automatic-creation'='process-time',
  'tag.creation-period'='daily','tag.num-retained-max'='35',
  'tag.default-time-retained'='30 d'
);

CREATE TABLE IF NOT EXISTS dim_unit (
  unit_id BIGINT,
  unit_name STRING,
  campaign_id BIGINT,
  campaign_name STRING,
  status INT,
  is_closed INT,
  delivery_way INT,
  search_keyword ARRAY<STRING>,
  product_id BIGINT,
  landing_page_url STRING,
  audience STRING,
  start_date STRING,
  end_date STRING,
  daily_budget BIGINT,
  bid_type STRING,
  bid BIGINT,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (unit_id) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input','tag.automatic-creation'='process-time',
  'tag.creation-period'='daily','tag.num-retained-max'='35',
  'tag.default-time-retained'='30 d'
);

CREATE TABLE IF NOT EXISTS dim_shop_zip (
  shop_id BIGINT,
  shop_name STRING,
  shop_type INT,
  status INT,
  main_category_id BIGINT,
  main_category_name STRING,
  shop_qualification_type INT,
  credit_code STRING,
  contact_person STRING,
  contact_phone STRING,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (shop_id) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input','tag.automatic-creation'='process-time',
  'tag.creation-period'='daily','tag.num-retained-max'='35',
  'tag.default-time-retained'='30 d'
);

CREATE TABLE IF NOT EXISTS dim_product_zip (
  product_id BIGINT,
  product_name STRING,
  shop_id BIGINT,
  shop_name STRING,
  price BIGINT,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input','tag.automatic-creation'='process-time',
  'tag.creation-period'='daily','tag.num-retained-max'='35',
  'tag.default-time-retained'='30 d'
);

CREATE TABLE IF NOT EXISTS dim_user_zip (
  uid BIGINT,
  user_name STRING,
  gender INT,
  phone_hash STRING,
  email STRING,
  user_level INT,
  birthday STRING,
  status INT,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (uid) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input','tag.automatic-creation'='process-time',
  'tag.creation-period'='daily','tag.num-retained-max'='35',
  'tag.default-time-retained'='30 d'
);

-- Accumulating order fact. Open orders live in the valid sentinel partition;
-- Key Dynamic Bucket moves a closed order into its terminal-date partition.
CREATE TABLE IF NOT EXISTS dwd_order_detail_acc (
  order_id BIGINT,
  uid BIGINT,
  product_id BIGINT,
  shop_id BIGINT,
  creative_id BIGINT,
  slot_id BIGINT,
  unit_price BIGINT,
  order_num INT,
  total_amount BIGINT,
  payment_method INT,
  receiver_name STRING,
  receiver_phone STRING,
  shipping_address STRING,
  tracking_number STRING,
  order_status INT,
  create_time STRING,
  cancel_time STRING,
  payment_time STRING,
  confirm_time STRING,
  refund_time STRING,
  refund_finish_time STRING,
  finish_time STRING,
  update_time STRING,
  dt STRING,
  `hour` STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) PARTITIONED BY (dt, `hour`) WITH (
  'bucket' = '-1',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'lookup','tag.automatic-creation'='process-time',
  'tag.creation-period'='daily','tag.num-retained-max'='35',
  'tag.default-time-retained'='30 d'
);

-- The only persistent ODS table: one row is one raw SDK tracking report.
CREATE TABLE IF NOT EXISTS ods_log_inc (
  msg_id BIGINT,
  bus_id INT,
  app_id INT,
  log_id INT,
  common ROW<
    uid BIGINT,
    area STRING,
    ip STRING,
    device_id BIGINT,
    platform INT,
    app_version STRING,
    browser_version STRING,
    sdk_version STRING
  >,
  actions ARRAY<ROW<
    event_id BIGINT,
    action STRING,
    creative_id BIGINT,
    product_id BIGINT,
    slot_id BIGINT,
    media STRING,
    commerce_scene STRING,
    traffic_type STRING,
    play_during BIGINT,
    ts BIGINT
  >>,
  ts BIGINT,
  dt STRING,
  PRIMARY KEY (dt, msg_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input'
);

-- Incremental DWD facts partitioned by business date and hour.
CREATE TABLE IF NOT EXISTS dwd_ad_action_log_inc (
  uid BIGINT,
  device_id STRING,
  platform INT,
  app_vc STRING,
  browser_vc STRING,
  sdk_vc STRING,
  creative_id BIGINT,
  slot_id BIGINT,
  action_type STRING,
  play_during BIGINT,
  ts BIGINT,
  dt STRING,
  `hour` STRING
) PARTITIONED BY (dt, `hour`) WITH (
  'bucket' = '4',
  'bucket-key' = 'creative_id','tag.automatic-creation'='process-time',
  'tag.creation-period'='daily','tag.num-retained-max'='35',
  'tag.default-time-retained'='30 d'
);

CREATE TABLE IF NOT EXISTS dwd_ad_bill_detail_inc (
  creative_id BIGINT,
  unit_id BIGINT,
  campaign_id BIGINT,
  slot_id BIGINT,
  uid BIGINT,
  cost BIGINT,
  billing_type TINYINT,
  ts BIGINT,
  dt STRING,
  `hour` STRING
) PARTITIONED BY (dt, `hour`) WITH (
  'bucket' = '4',
  'bucket-key' = 'creative_id','tag.automatic-creation'='process-time',
  'tag.creation-period'='daily','tag.num-retained-max'='35',
  'tag.default-time-retained'='30 d'
);

CREATE TABLE IF NOT EXISTS ads_advertiser_retention_di (
  cohort_date STRING,
  cohort_size BIGINT,
  retained_1d BIGINT,
  retained_7d BIGINT,
  retained_15d BIGINT,
  retained_30d BIGINT,
  rate_1d DECIMAL(18,6),
  rate_7d DECIMAL(18,6),
  rate_15d DECIMAL(18,6),
  rate_30d DECIMAL(18,6),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (cohort_date) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'full-compaction'
);

CREATE TABLE IF NOT EXISTS ads_attribution_summary_di (
  event_date STRING,
  advertiser_id BIGINT,
  advertiser_name STRING,
  campaign_id BIGINT,
  campaign_name STRING,
  attribution_model STRING,
  conversions BIGINT,
  orders BIGINT,
  attributed_gmv DECIMAL(18,2),
  attributed_spend DECIMAL(18,4),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (event_date, advertiser_id, campaign_id, attribution_model) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'full-compaction'
);

-- One row per order.  This is the drill-down dataset behind the attribution
-- overview: the attributed creative/campaign fields remain NULL for organic
-- orders, while the original order-side dimensions are retained separately.
CREATE TABLE IF NOT EXISTS ads_order_attribution_detail_di (
  event_date STRING,
  order_event_id BIGINT,
  order_id BIGINT,
  order_ts TIMESTAMP(3),
  user_id BIGINT,
  order_advertiser_id BIGINT,
  order_advertiser_name STRING,
  order_campaign_id BIGINT,
  order_campaign_name STRING,
  order_gmv DECIMAL(18,2),
  click_event_id BIGINT,
  click_ts TIMESTAMP(3),
  creative_id BIGINT,
  campaign_id BIGINT,
  campaign_name STRING,
  advertiser_id BIGINT,
  advertiser_name STRING,
  touch_spend DECIMAL(18,4),
  attribution_model STRING,
  attribution_type STRING,
  attribution_period STRING,
  attribution_sort INT,
  lag_minutes BIGINT,
  is_attributed BOOLEAN,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (event_date, order_event_id) NOT ENFORCED
) PARTITIONED BY (event_date) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'full-compaction'
);

-- Offline creative-grain serving dataset.  It intentionally keeps additive
-- facts and dimensional attributes together so BI users can safely aggregate
-- and drill across advertiser -> campaign -> creative without rejoining DWS.
CREATE TABLE IF NOT EXISTS ads_creative_offline_di (
  stat_date STRING,
  creative_id BIGINT,
  creative_name STRING,
  campaign_id BIGINT,
  campaign_name STRING,
  campaign_objective STRING,
  campaign_budget DECIMAL(18,2),
  campaign_status STRING,
  advertiser_id BIGINT,
  advertiser_name STRING,
  industry STRING,
  advertiser_tier STRING,
  unit_id BIGINT,
  unit_name STRING,
  bid_type STRING,
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
  roi DECIMAL(18,6),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (stat_date, creative_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'full-compaction'
);

CREATE TABLE IF NOT EXISTS ads_fraud_signal_di (
  event_date STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  advertiser_id BIGINT,
  advertiser_name STRING,
  media STRING,
  rule_code STRING,
  rule_desc STRING,
  click_count BIGINT,
  impression_count BIGINT,
  unique_users BIGINT,
  suspicious_spend DECIMAL(18,4),
  risk_score DECIMAL(18,6),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (event_date, window_start, advertiser_id, media, rule_code) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'full-compaction'
);
