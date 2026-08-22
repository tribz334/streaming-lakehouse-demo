-- DWS/DM physical model tables used by the offline creative subject pipeline.
SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '1';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://hive-metastore:9083',
  'warehouse' = 'file:///warehouse/paimon'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ad_dw;
USE ad_dw;

CREATE TABLE IF NOT EXISTS dws_creative (
  creative_id BIGINT,
  delivery_cnt BIGINT,
  impression_cnt BIGINT,
  click_cnt BIGINT,
  conversion_cnt BIGINT,
  cost BIGINT,
  order_pay_cnt BIGINT,
  order_pay_gmv BIGINT,
  order_refund_cnt BIGINT,
  order_refund_gmv BIGINT,
  order_valid_cnt BIGINT,
  order_valid_gmv BIGINT,
  dt STRING,
  PRIMARY KEY (dt, creative_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'lookup'
);

CREATE TABLE IF NOT EXISTS dws_unit (
  unit_id BIGINT,
  delivery_cnt BIGINT, impression_cnt BIGINT, click_cnt BIGINT, conversion_cnt BIGINT,
  cost BIGINT, order_pay_cnt BIGINT, order_pay_gmv BIGINT,
  order_refund_cnt BIGINT, order_refund_gmv BIGINT,
  order_valid_cnt BIGINT, order_valid_gmv BIGINT,
  dt STRING,
  PRIMARY KEY (dt, unit_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dws_campaign (
  campaign_id BIGINT,
  delivery_cnt BIGINT, impression_cnt BIGINT, click_cnt BIGINT, conversion_cnt BIGINT,
  cost BIGINT, order_pay_cnt BIGINT, order_pay_gmv BIGINT,
  order_refund_cnt BIGINT, order_refund_gmv BIGINT,
  order_valid_cnt BIGINT, order_valid_gmv BIGINT,
  dt STRING,
  PRIMARY KEY (dt, campaign_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dws_advertiser (
  advertiser_id BIGINT,
  delivery_cnt BIGINT, impression_cnt BIGINT, click_cnt BIGINT, conversion_cnt BIGINT,
  cost BIGINT, order_pay_cnt BIGINT, order_pay_gmv BIGINT,
  order_refund_cnt BIGINT, order_refund_gmv BIGINT,
  order_valid_cnt BIGINT, order_valid_gmv BIGINT,
  dt STRING,
  PRIMARY KEY (dt, advertiser_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dm_creative (
  creative_id BIGINT,
  delivery_cnt_1d BIGINT, delivery_cnt_7d BIGINT, delivery_cnt_30d BIGINT, delivery_cnt_acc BIGINT,
  impression_cnt_1d BIGINT, impression_cnt_7d BIGINT, impression_cnt_30d BIGINT, impression_cnt_acc BIGINT,
  click_cnt_1d BIGINT, click_cnt_7d BIGINT, click_cnt_30d BIGINT, click_cnt_acc BIGINT,
  conversion_cnt_1d BIGINT, conversion_cnt_7d BIGINT, conversion_cnt_30d BIGINT, conversion_cnt_acc BIGINT,
  cost_first_date STRING, cost_last_date STRING,
  cost_1d BIGINT, cost_7d BIGINT, cost_30d BIGINT, cost_acc BIGINT,
  order_pay_first_date STRING, order_pay_last_date STRING,
  order_pay_cnt_1d BIGINT, order_pay_cnt_7d BIGINT, order_pay_cnt_30d BIGINT, order_pay_cnt_acc BIGINT,
  order_pay_gmv_1d BIGINT, order_pay_gmv_7d BIGINT, order_pay_gmv_30d BIGINT, order_pay_gmv_acc BIGINT,
  order_refund_first_date STRING, order_refund_last_date STRING,
  order_refund_cnt_1d BIGINT, order_refund_cnt_7d BIGINT, order_refund_cnt_30d BIGINT, order_refund_cnt_acc BIGINT,
  order_refund_gmv_1d BIGINT, order_refund_gmv_7d BIGINT, order_refund_gmv_30d BIGINT, order_refund_gmv_acc BIGINT,
  order_valid_first_date STRING, order_valid_last_date STRING,
  order_valid_cnt_1d BIGINT, order_valid_cnt_7d BIGINT, order_valid_cnt_30d BIGINT, order_valid_cnt_acc BIGINT,
  order_valid_gmv_1d BIGINT, order_valid_gmv_7d BIGINT, order_valid_gmv_30d BIGINT, order_valid_gmv_acc BIGINT,
  dt STRING,
  PRIMARY KEY (dt, creative_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'lookup'
);

CREATE TABLE IF NOT EXISTS dm_unit (
  unit_id BIGINT,
  delivery_cnt_1d BIGINT, delivery_cnt_7d BIGINT, delivery_cnt_30d BIGINT, delivery_cnt_acc BIGINT,
  impression_cnt_1d BIGINT, impression_cnt_7d BIGINT, impression_cnt_30d BIGINT, impression_cnt_acc BIGINT,
  click_cnt_1d BIGINT, click_cnt_7d BIGINT, click_cnt_30d BIGINT, click_cnt_acc BIGINT,
  conversion_cnt_1d BIGINT, conversion_cnt_7d BIGINT, conversion_cnt_30d BIGINT, conversion_cnt_acc BIGINT,
  cost_first_date STRING, cost_last_date STRING,
  cost_1d BIGINT, cost_7d BIGINT, cost_30d BIGINT, cost_acc BIGINT,
  order_pay_first_date STRING, order_pay_last_date STRING,
  order_pay_cnt_1d BIGINT, order_pay_cnt_7d BIGINT, order_pay_cnt_30d BIGINT, order_pay_cnt_acc BIGINT,
  order_pay_gmv_1d BIGINT, order_pay_gmv_7d BIGINT, order_pay_gmv_30d BIGINT, order_pay_gmv_acc BIGINT,
  order_refund_first_date STRING, order_refund_last_date STRING,
  order_refund_cnt_1d BIGINT, order_refund_cnt_7d BIGINT, order_refund_cnt_30d BIGINT, order_refund_cnt_acc BIGINT,
  order_refund_gmv_1d BIGINT, order_refund_gmv_7d BIGINT, order_refund_gmv_30d BIGINT, order_refund_gmv_acc BIGINT,
  order_valid_first_date STRING, order_valid_last_date STRING,
  order_valid_cnt_1d BIGINT, order_valid_cnt_7d BIGINT, order_valid_cnt_30d BIGINT, order_valid_cnt_acc BIGINT,
  order_valid_gmv_1d BIGINT, order_valid_gmv_7d BIGINT, order_valid_gmv_30d BIGINT, order_valid_gmv_acc BIGINT,
  dt STRING,
  PRIMARY KEY (dt, unit_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dm_campaign (
  campaign_id BIGINT,
  delivery_cnt_1d BIGINT, delivery_cnt_7d BIGINT, delivery_cnt_30d BIGINT, delivery_cnt_acc BIGINT,
  impression_cnt_1d BIGINT, impression_cnt_7d BIGINT, impression_cnt_30d BIGINT, impression_cnt_acc BIGINT,
  click_cnt_1d BIGINT, click_cnt_7d BIGINT, click_cnt_30d BIGINT, click_cnt_acc BIGINT,
  conversion_cnt_1d BIGINT, conversion_cnt_7d BIGINT, conversion_cnt_30d BIGINT, conversion_cnt_acc BIGINT,
  cost_first_date STRING, cost_last_date STRING,
  cost_1d BIGINT, cost_7d BIGINT, cost_30d BIGINT, cost_acc BIGINT,
  order_pay_first_date STRING, order_pay_last_date STRING,
  order_pay_cnt_1d BIGINT, order_pay_cnt_7d BIGINT, order_pay_cnt_30d BIGINT, order_pay_cnt_acc BIGINT,
  order_pay_gmv_1d BIGINT, order_pay_gmv_7d BIGINT, order_pay_gmv_30d BIGINT, order_pay_gmv_acc BIGINT,
  order_refund_first_date STRING, order_refund_last_date STRING,
  order_refund_cnt_1d BIGINT, order_refund_cnt_7d BIGINT, order_refund_cnt_30d BIGINT, order_refund_cnt_acc BIGINT,
  order_refund_gmv_1d BIGINT, order_refund_gmv_7d BIGINT, order_refund_gmv_30d BIGINT, order_refund_gmv_acc BIGINT,
  order_valid_first_date STRING, order_valid_last_date STRING,
  order_valid_cnt_1d BIGINT, order_valid_cnt_7d BIGINT, order_valid_cnt_30d BIGINT, order_valid_cnt_acc BIGINT,
  order_valid_gmv_1d BIGINT, order_valid_gmv_7d BIGINT, order_valid_gmv_30d BIGINT, order_valid_gmv_acc BIGINT,
  dt STRING,
  PRIMARY KEY (dt, campaign_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dm_advertiser (
  advertiser_id BIGINT,
  delivery_cnt_1d BIGINT, delivery_cnt_7d BIGINT, delivery_cnt_30d BIGINT, delivery_cnt_acc BIGINT,
  impression_cnt_1d BIGINT, impression_cnt_7d BIGINT, impression_cnt_30d BIGINT, impression_cnt_acc BIGINT,
  click_cnt_1d BIGINT, click_cnt_7d BIGINT, click_cnt_30d BIGINT, click_cnt_acc BIGINT,
  conversion_cnt_1d BIGINT, conversion_cnt_7d BIGINT, conversion_cnt_30d BIGINT, conversion_cnt_acc BIGINT,
  cost_first_date STRING, cost_last_date STRING,
  cost_1d BIGINT, cost_7d BIGINT, cost_30d BIGINT, cost_acc BIGINT,
  order_pay_first_date STRING, order_pay_last_date STRING,
  order_pay_cnt_1d BIGINT, order_pay_cnt_7d BIGINT, order_pay_cnt_30d BIGINT, order_pay_cnt_acc BIGINT,
  order_pay_gmv_1d BIGINT, order_pay_gmv_7d BIGINT, order_pay_gmv_30d BIGINT, order_pay_gmv_acc BIGINT,
  order_refund_first_date STRING, order_refund_last_date STRING,
  order_refund_cnt_1d BIGINT, order_refund_cnt_7d BIGINT, order_refund_cnt_30d BIGINT, order_refund_cnt_acc BIGINT,
  order_refund_gmv_1d BIGINT, order_refund_gmv_7d BIGINT, order_refund_gmv_30d BIGINT, order_refund_gmv_acc BIGINT,
  order_valid_first_date STRING, order_valid_last_date STRING,
  order_valid_cnt_1d BIGINT, order_valid_cnt_7d BIGINT, order_valid_cnt_30d BIGINT, order_valid_cnt_acc BIGINT,
  order_valid_gmv_1d BIGINT, order_valid_gmv_7d BIGINT, order_valid_gmv_30d BIGINT, order_valid_gmv_acc BIGINT,
  dt STRING,
  PRIMARY KEY (dt, advertiser_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');

CREATE TABLE IF NOT EXISTS dm_antifraud_feature (
  stat_date STRING, event_id BIGINT, user_id BIGINT, device_id BIGINT,
  device_ip STRING, creative_id BIGINT, slot_id BIGINT, click_cnt_1h INT,
  click_cnt_1d INT, ip_click_cnt_1h INT, ip_uv_1h INT,
  click_interval_ms BIGINT, ctr_deviation DECIMAL(10,6), ua_entropy DECIMAL(10,6),
  geo_ip_mismatch BOOLEAN, is_night_burst BOOLEAN, fraud_score DECIMAL(6,4),
  fraud_label STRING, rule_hits ARRAY<STRING>,
  PRIMARY KEY (stat_date, event_id) NOT ENFORCED
) PARTITIONED BY (stat_date) WITH ('bucket'='4','merge-engine'='deduplicate','changelog-producer'='lookup');
