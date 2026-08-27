-- Converge the running catalogs on the Unit-owned classification model.
-- Stop dependent streaming jobs before applying this migration.
CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');
USE CATALOG fluss;
USE ad_dw;
DROP TABLE IF EXISTS ads_realtime_metric_30s;
DROP TABLE IF EXISTS ads_realtime_metric_10s;
DROP TABLE IF EXISTS dws_ad_type_di;
DROP TABLE IF EXISTS dws_placement_di;
DROP TABLE IF EXISTS dws_campaign_di;
DROP TABLE IF EXISTS dws_advertiser_di;
DROP TABLE IF EXISTS dwd_ad_order_acc;
DROP TABLE IF EXISTS dwd_ad_order_event_di;
DROP TABLE IF EXISTS dwd_ad_bill_di;
DROP TABLE IF EXISTS dwd_ad_event_dirty_di;

CREATE TABLE IF NOT EXISTS dwd_ad_event_dirty_di (
  event_id BIGINT,creative_id BIGINT,error_reason STRING,common STRING,events STRING,
  ts BIGINT,dt STRING,PRIMARY KEY(dt,event_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');

CREATE TABLE IF NOT EXISTS dwd_ad_bill_di (
  bill_id BIGINT,creative_id BIGINT,unit_id BIGINT,campaign_id BIGINT,slot_id BIGINT,uid BIGINT,
  advertiser_id BIGINT,placement_type INT,ad_type INT,cost BIGINT,is_closed INT,
  ts BIGINT,event_time TIMESTAMP_LTZ(3),dt STRING,`hour` STRING,
  WATERMARK FOR event_time AS event_time-INTERVAL '5' SECOND,PRIMARY KEY(dt,bill_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');

CREATE TABLE IF NOT EXISTS dwd_ad_order_acc (
  order_id BIGINT,uid BIGINT,product_id BIGINT,shop_id BIGINT,creative_id BIGINT,slot_id BIGINT,
  product_price BIGINT,product_num INT,total_amount BIGINT,payment_method INT,receiver_name STRING,
  receiver_phone STRING,shipping_address STRING,tracking_number STRING,order_status INT,
  create_time STRING,cancel_time STRING,pay_time STRING,confirm_time STRING,refund_time STRING,
  updated_at STRING,advertiser_id BIGINT,
  campaign_id BIGINT,unit_id BIGINT,placement_type INT,ad_type INT,
  click_time TIMESTAMP_LTZ(3),is_direct_attribution BOOLEAN,
  event_time TIMESTAMP_LTZ(3),dt STRING,`hour` STRING,
  WATERMARK FOR event_time AS event_time-INTERVAL '10' SECOND,
  PRIMARY KEY(dt,order_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');

DROP TABLE IF EXISTS dwd_ad_order_di;
CREATE TABLE IF NOT EXISTS dwd_ad_order_di (
  event_id STRING,order_id BIGINT,uid BIGINT,product_id BIGINT,order_type STRING,
  pay_time STRING,pay_order_gmv BIGINT,
  shop_id BIGINT,product_price BIGINT,product_num INT,total_amount BIGINT,payment_method INT,
  receiver_name STRING,receiver_phone STRING,shipping_address STRING,tracking_number STRING,
  order_status INT,create_time STRING,cancel_time STRING,confirm_time STRING,refund_time STRING,
  updated_at STRING,dt STRING,
  PRIMARY KEY(dt,event_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');

CREATE TABLE IF NOT EXISTS dwd_ad_order_event_di (
  event_id STRING,order_id BIGINT,uid BIGINT,product_id BIGINT,order_type STRING,
  order_time TIMESTAMP_LTZ(3),order_gmv BIGINT,
  advertiser_id BIGINT,campaign_id BIGINT,unit_id BIGINT,creative_id BIGINT,
  placement_type INT,ad_type INT,dt STRING,
  WATERMARK FOR order_time AS order_time-INTERVAL '10' SECOND,
  PRIMARY KEY(dt,event_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');

CREATE TABLE IF NOT EXISTS dws_advertiser_di (
  dt STRING,advertiser_id BIGINT,advertiser_name STRING,
  delivery_count BIGINT,
  impression_count BIGINT,
  click_count BIGINT,
  conversion_count BIGINT,
  cost BIGINT,
  closed_cost BIGINT,
  pay_order_count BIGINT,
  refund_order_count BIGINT,
  pay_order_gmv BIGINT,
  refund_order_gmv BIGINT,
  short_video_pay_order_count BIGINT,short_video_refund_order_count BIGINT,
  short_video_pay_order_gmv BIGINT,short_video_refund_order_gmv BIGINT,
  live_pay_order_count BIGINT,live_refund_order_count BIGINT,
  live_pay_order_gmv BIGINT,live_refund_order_gmv BIGINT,
  image_text_pay_order_count BIGINT,image_text_refund_order_count BIGINT,
  image_text_pay_order_gmv BIGINT,image_text_refund_order_gmv BIGINT,
  other_ad_type_pay_order_count BIGINT,other_ad_type_refund_order_count BIGINT,
  other_ad_type_pay_order_gmv BIGINT,other_ad_type_refund_order_gmv BIGINT,
  search_pay_order_count BIGINT,search_refund_order_count BIGINT,
  search_pay_order_gmv BIGINT,search_refund_order_gmv BIGINT,
  splash_pay_order_count BIGINT,splash_refund_order_count BIGINT,
  splash_pay_order_gmv BIGINT,splash_refund_order_gmv BIGINT,
  feed_pay_order_count BIGINT,feed_refund_order_count BIGINT,
  feed_pay_order_gmv BIGINT,feed_refund_order_gmv BIGINT,
  rewarded_pay_order_count BIGINT,rewarded_refund_order_count BIGINT,
  rewarded_pay_order_gmv BIGINT,rewarded_refund_order_gmv BIGINT,
  banner_pay_order_count BIGINT,banner_refund_order_count BIGINT,
  banner_pay_order_gmv BIGINT,banner_refund_order_gmv BIGINT,
  other_placement_pay_order_count BIGINT,other_placement_refund_order_count BIGINT,
  other_placement_pay_order_gmv BIGINT,other_placement_refund_order_gmv BIGINT,
  PRIMARY KEY(dt,advertiser_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');

CREATE TABLE IF NOT EXISTS dws_campaign_di (
  dt STRING,campaign_id BIGINT,campaign_name STRING,
  delivery_count BIGINT,
  impression_count BIGINT,
  click_count BIGINT,
  conversion_count BIGINT,
  cost BIGINT,
  closed_cost BIGINT,
  pay_order_count BIGINT,
  refund_order_count BIGINT,
  pay_order_gmv BIGINT,
  refund_order_gmv BIGINT,
  short_video_pay_order_count BIGINT,short_video_refund_order_count BIGINT,
  short_video_pay_order_gmv BIGINT,short_video_refund_order_gmv BIGINT,
  live_pay_order_count BIGINT,live_refund_order_count BIGINT,
  live_pay_order_gmv BIGINT,live_refund_order_gmv BIGINT,
  image_text_pay_order_count BIGINT,image_text_refund_order_count BIGINT,
  image_text_pay_order_gmv BIGINT,image_text_refund_order_gmv BIGINT,
  other_ad_type_pay_order_count BIGINT,other_ad_type_refund_order_count BIGINT,
  other_ad_type_pay_order_gmv BIGINT,other_ad_type_refund_order_gmv BIGINT,
  search_pay_order_count BIGINT,search_refund_order_count BIGINT,
  search_pay_order_gmv BIGINT,search_refund_order_gmv BIGINT,
  splash_pay_order_count BIGINT,splash_refund_order_count BIGINT,
  splash_pay_order_gmv BIGINT,splash_refund_order_gmv BIGINT,
  feed_pay_order_count BIGINT,feed_refund_order_count BIGINT,
  feed_pay_order_gmv BIGINT,feed_refund_order_gmv BIGINT,
  rewarded_pay_order_count BIGINT,rewarded_refund_order_count BIGINT,
  rewarded_pay_order_gmv BIGINT,rewarded_refund_order_gmv BIGINT,
  banner_pay_order_count BIGINT,banner_refund_order_count BIGINT,
  banner_pay_order_gmv BIGINT,banner_refund_order_gmv BIGINT,
  other_placement_pay_order_count BIGINT,other_placement_refund_order_count BIGINT,
  other_placement_pay_order_gmv BIGINT,other_placement_refund_order_gmv BIGINT,
  PRIMARY KEY(dt,campaign_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');

CREATE TABLE IF NOT EXISTS ads_realtime_metric_10s (
  window_start TIMESTAMP_LTZ(3),window_end TIMESTAMP_LTZ(3),
  delivery_count BIGINT,
  impression_count BIGINT,
  click_count BIGINT,
  conversion_count BIGINT,
  cost BIGINT,
  closed_cost BIGINT,
  pay_order_count BIGINT,
  refund_order_count BIGINT,
  pay_order_gmv BIGINT,
  refund_order_gmv BIGINT,
  short_video_pay_order_gmv BIGINT,
  live_pay_order_gmv BIGINT,
  image_text_pay_order_gmv BIGINT,
  other_ad_type_pay_order_gmv BIGINT,
  search_pay_order_gmv BIGINT,
  splash_pay_order_gmv BIGINT,
  feed_pay_order_gmv BIGINT,
  rewarded_pay_order_gmv BIGINT,
  banner_pay_order_gmv BIGINT,
  other_placement_pay_order_gmv BIGINT,
  dt STRING,PRIMARY KEY(dt,window_start) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');

CREATE CATALOG paimon WITH ('type'='paimon','metastore'='filesystem','warehouse'='file:///warehouse/paimon');
USE CATALOG paimon;
USE ad_dw;
DROP TABLE IF EXISTS dm_ad_type_df;
DROP TABLE IF EXISTS dm_placement_df;
DROP TABLE IF EXISTS ads_order_attribution_di;
DROP TABLE IF EXISTS ads_offline_metric_di;

CREATE TABLE IF NOT EXISTS ads_offline_metric_di (
  dt STRING,
  metric_key STRING,
  delivery_count BIGINT,
  impression_count BIGINT,
  click_count BIGINT,
  conversion_count BIGINT,
  cost BIGINT,
  closed_cost BIGINT,
  pay_order_count BIGINT,
  refund_order_count BIGINT,
  pay_order_gmv BIGINT,
  refund_order_gmv BIGINT,
  short_video_pay_order_gmv BIGINT,
  live_pay_order_gmv BIGINT,
  image_text_pay_order_gmv BIGINT,
  search_pay_order_gmv BIGINT,
  splash_pay_order_gmv BIGINT,
  feed_pay_order_gmv BIGINT,
  rewarded_pay_order_gmv BIGINT,
  banner_pay_order_gmv BIGINT,
  other_placement_pay_order_gmv BIGINT,
  PRIMARY KEY(dt,metric_key) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket'='2','file.format'='parquet');

CREATE TABLE IF NOT EXISTS ads_order_attribution_di (
  dt STRING,order_id BIGINT,uid BIGINT,product_id BIGINT,pay_time TIMESTAMP_LTZ(3),
  pay_order_gmv BIGINT,last_click_time TIMESTAMP_LTZ(3),advertiser_id BIGINT,
  campaign_id BIGINT,unit_id BIGINT,creative_id BIGINT,placement_type INT,ad_type INT,
  attribute_period STRING,
  PRIMARY KEY(dt,order_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket'='2','file.format'='parquet');
