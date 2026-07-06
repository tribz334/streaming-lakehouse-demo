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
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-ods-log',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
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

CREATE TABLE IF NOT EXISTS dwd_order_lifecycle_df (
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
  'bucket' = '4',
  'merge-engine' = 'partial-update',
  'changelog-producer' = 'lookup'
);

CREATE TABLE IF NOT EXISTS dws_ad_metric_10s (
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

CREATE TABLE IF NOT EXISTS ads_data_quality_result_di (
  check_date STRING,
  rule_code STRING,
  rule_name STRING,
  data_layer STRING,
  target_table STRING,
  actual_value DECIMAL(24,6),
  expected_value STRING,
  check_status STRING,
  severity STRING,
  details STRING,
  checked_at TIMESTAMP(3),
  PRIMARY KEY (check_date, rule_code) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'lookup'
);

CREATE TABLE IF NOT EXISTS ads_data_quality_summary_di (
  check_date STRING,
  total_rules BIGINT,
  passed_rules BIGINT,
  failed_rules BIGINT,
  quality_score DECIMAL(18,2),
  overall_status STRING,
  checked_at TIMESTAMP(3),
  PRIMARY KEY (check_date) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'lookup'
);

CREATE TABLE IF NOT EXISTS lakehouse_feature_experiment (
  experiment_id STRING,
  metric_value BIGINT,
  created_at TIMESTAMP(3),
  PRIMARY KEY (experiment_id) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'lookup',
  'snapshot.time-retained' = '7 d',
  'snapshot.num-retained.min' = '20',
  'snapshot.num-retained.max' = '100'
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
