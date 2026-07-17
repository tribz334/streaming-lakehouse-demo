SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '1';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:///warehouse/paimon'
);

USE CATALOG default_catalog;
USE default_database;

CREATE TABLE IF NOT EXISTS ods_log_kafka (
  event_id STRING,
  ts STRING,
  advertiser_id STRING,
  campaign_id STRING,
  unit_id STRING,
  creative_id STRING,
  media STRING,
  region STRING,
  user_id STRING,
  event_type STRING,
  bid_price DECIMAL(18,4),
  spend DECIMAL(18,4),
  gmv DECIMAL(18,2),
  order_id STRING,
  schema_version INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_log',
  'properties.bootstrap.servers' = 'kafka-node-1:9092',
  'properties.group.id' = 'flink-ods-log',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Relay the finalized Paimon 10-second windows to StarRocks Routine Load.
-- Kafka decouples the Flink 2.0 job from the StarRocks connector release cycle.
CREATE TABLE IF NOT EXISTS starrocks_realtime_metric_kafka (
  window_start TIMESTAMP(3),
  advertiser_id STRING,
  campaign_id STRING,
  unit_id STRING,
  creative_id STRING,
  window_end TIMESTAMP(3),
  advertiser_name STRING,
  spend DECIMAL(18,4),
  gmv DECIMAL(18,2),
  impressions BIGINT,
  clicks BIGINT,
  conversions BIGINT,
  orders BIGINT,
  ctr DECIMAL(18,6),
  cvr DECIMAL(18,6),
  roi DECIMAL(18,6),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (window_start, advertiser_id, campaign_id, unit_id, creative_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dws_ad_metric_stream_10s_sr',
  'properties.bootstrap.servers' = 'kafka-node-1:9092',
  'key.format' = 'json',
  'key.json.timestamp-format.standard' = 'SQL',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'SQL',
  'value.fields-include' = 'ALL'
);

CREATE TABLE IF NOT EXISTS mysql_advertiser (
  advertiser_id STRING,
  advertiser_name STRING,
  industry STRING,
  tier STRING,
  home_region STRING,
  signup_date DATE,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (advertiser_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/ad_ods',
  'username' = 'root',
  'password' = 'root',
  'table-name' = 'advertiser',
  'driver' = 'com.mysql.cj.jdbc.Driver'
);

CREATE TABLE IF NOT EXISTS mysql_campaign (
  campaign_id STRING,
  advertiser_id STRING,
  campaign_name STRING,
  objective STRING,
  budget DECIMAL(18,2),
  status STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (campaign_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/ad_ods',
  'username' = 'root',
  'password' = 'root',
  'table-name' = 'campaign',
  'driver' = 'com.mysql.cj.jdbc.Driver'
);

CREATE TABLE IF NOT EXISTS mysql_creative (
  creative_id STRING,
  campaign_id STRING,
  unit_id STRING,
  creative_name STRING,
  format STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (creative_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/ad_ods',
  'username' = 'root',
  'password' = 'root',
  'table-name' = 'creative',
  'driver' = 'com.mysql.cj.jdbc.Driver'
);

CREATE TABLE IF NOT EXISTS mysql_unit (
  unit_id STRING,
  campaign_id STRING,
  unit_name STRING,
  bid_type STRING,
  bid_amount DECIMAL(18,4),
  status STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (unit_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/ad_ods',
  'username' = 'root',
  'password' = 'root',
  'table-name' = 'unit',
  'driver' = 'com.mysql.cj.jdbc.Driver'
);

CREATE TABLE IF NOT EXISTS mysql_order (
  order_id STRING,
  advertiser_id STRING,
  creative_id STRING,
  user_id STRING,
  gmv DECIMAL(18,2),
  order_status STRING,
  create_time TIMESTAMP(3),
  payment_time TIMESTAMP(3),
  refund_time TIMESTAMP(3),
  finish_time TIMESTAMP(3),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/ad_ods',
  'username' = 'root',
  'password' = 'root',
  'table-name' = 'ad_order',
  'driver' = 'com.mysql.cj.jdbc.Driver'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ad_dw;
USE ad_dw;

CREATE TABLE IF NOT EXISTS dim_advertiser_df (
  advertiser_id STRING,
  advertiser_name STRING,
  industry STRING,
  tier STRING,
  home_region STRING,
  signup_date DATE,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (advertiser_id) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input'
);

CREATE TABLE IF NOT EXISTS dim_campaign_df (
  campaign_id STRING,
  advertiser_id STRING,
  campaign_name STRING,
  objective STRING,
  budget DECIMAL(18,2),
  status STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (campaign_id) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input'
);

CREATE TABLE IF NOT EXISTS dim_creative_df (
  creative_id STRING,
  campaign_id STRING,
  unit_id STRING,
  creative_name STRING,
  format STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (creative_id) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input'
);

CREATE TABLE IF NOT EXISTS dim_unit_df (
  unit_id STRING,
  campaign_id STRING,
  unit_name STRING,
  bid_type STRING,
  bid_amount DECIMAL(18,4),
  status STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (unit_id) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input'
);

CREATE TABLE IF NOT EXISTS ods_ad_events_di (
  event_date STRING,
  event_id STRING,
  event_ts TIMESTAMP(3),
  advertiser_id STRING,
  campaign_id STRING,
  unit_id STRING,
  creative_id STRING,
  media STRING,
  region STRING,
  user_id STRING,
  event_type STRING,
  bid_price DECIMAL(18,4),
  spend DECIMAL(18,4),
  gmv DECIMAL(18,2),
  order_id STRING,
  source_topic STRING,
  schema_version INT,
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND,
  PRIMARY KEY (event_date, event_id) NOT ENFORCED
) PARTITIONED BY (event_date) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input'
);

CREATE TABLE IF NOT EXISTS dwd_ad_events_di (
  event_date STRING,
  event_id STRING,
  event_ts TIMESTAMP(3),
  advertiser_id STRING,
  advertiser_name STRING,
  industry STRING,
  tier STRING,
  campaign_id STRING,
  campaign_name STRING,
  unit_id STRING,
  creative_id STRING,
  creative_name STRING,
  media STRING,
  region STRING,
  user_id STRING,
  event_type STRING,
  spend DECIMAL(18,4),
  gmv DECIMAL(18,2),
  order_id STRING,
  loaded_at TIMESTAMP(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND,
  PRIMARY KEY (event_date, event_id) NOT ENFORCED
) PARTITIONED BY (event_date) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'lookup'
);

CREATE TABLE IF NOT EXISTS dws_ad_metric_stream_10s (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  advertiser_id STRING,
  advertiser_name STRING,
  campaign_id STRING,
  unit_id STRING,
  creative_id STRING,
  spend DECIMAL(18,4),
  gmv DECIMAL(18,2),
  impressions BIGINT,
  clicks BIGINT,
  conversions BIGINT,
  orders BIGINT,
  ctr DECIMAL(18,6),
  cvr DECIMAL(18,6),
  roi DECIMAL(18,6),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (window_start, advertiser_id, campaign_id, unit_id, creative_id) NOT ENFORCED
) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'lookup'
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
  advertiser_id STRING,
  advertiser_name STRING,
  campaign_id STRING,
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
  order_event_id STRING,
  order_id STRING,
  order_ts TIMESTAMP(3),
  user_id STRING,
  order_advertiser_id STRING,
  order_advertiser_name STRING,
  order_campaign_id STRING,
  order_campaign_name STRING,
  order_gmv DECIMAL(18,2),
  click_event_id STRING,
  click_ts TIMESTAMP(3),
  creative_id STRING,
  campaign_id STRING,
  campaign_name STRING,
  advertiser_id STRING,
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
  creative_id STRING,
  creative_name STRING,
  creative_format STRING,
  campaign_id STRING,
  campaign_name STRING,
  campaign_objective STRING,
  campaign_budget DECIMAL(18,2),
  campaign_status STRING,
  advertiser_id STRING,
  advertiser_name STRING,
  industry STRING,
  advertiser_tier STRING,
  unit_id STRING,
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
  advertiser_id STRING,
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
