-- HOT PATH: only Fluss. Tiering Service owns every same-named Paimon lake copy.
CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');
USE CATALOG fluss;
CREATE DATABASE IF NOT EXISTS ad_dw;
USE ad_dw;

CREATE TABLE IF NOT EXISTS ods_log_di (
  msg_id BIGINT,bus_id INT,app_id INT,log_id INT,common STRING,events STRING,ts BIGINT,dt STRING
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.log.ttl'='3min','table.datalake.enabled'='true',
  'table.datalake.freshness'='30s','paimon.file.format'='parquet');

CREATE TABLE IF NOT EXISTS ods_mysql_bill_di (
  bill_id BIGINT,advertiser_id BIGINT,campaign_id BIGINT,unit_id BIGINT,creative_id BIGINT,
  uid BIGINT,slot_id BIGINT,cost BIGINT,bill_time STRING,
  updated_at STRING,dt STRING,PRIMARY KEY(dt,bill_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS ods_mysql_order_acc (
  order_id BIGINT,user_id BIGINT,product_id BIGINT,shop_id BIGINT,
  product_price BIGINT,product_num INT,total_amount BIGINT,payment_method INT,receiver_name STRING,
  receiver_phone STRING,shipping_address STRING,tracking_number STRING,order_status INT,
  create_time STRING,cancel_time STRING,pay_time STRING,confirm_time STRING,refund_time STRING,
  updated_at STRING,dt STRING,
  PRIMARY KEY(dt,order_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');

CREATE TABLE IF NOT EXISTS dim_advertiser_df (
  advertiser_id BIGINT,advertiser_name STRING,qualification_type INT,status INT,
  industry_l1_id BIGINT,industry_l1_name STRING,industry_l2_id BIGINT,industry_l2_name STRING,
  start_dt STRING,end_dt STRING,created_at STRING,updated_at STRING,
  PRIMARY KEY(advertiser_id) NOT ENFORCED
) WITH ('bucket.num'='4','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dim_campaign_df (
  campaign_id BIGINT,campaign_name STRING,advertiser_id BIGINT,advertiser_name STRING,status INT,
  market_goal INT,trading_mode INT,budget BIGINT,daily_budget BIGINT,created_at STRING,
  updated_at STRING,PRIMARY KEY(campaign_id) NOT ENFORCED
) WITH ('bucket.num'='4','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dim_unit_df (
  unit_id BIGINT,unit_name STRING,campaign_id BIGINT,campaign_name STRING,status INT,is_closed INT,
  placement_type INT,ad_type INT,search_keyword ARRAY<STRING>,product_id BIGINT,landing_page_url STRING,
  audience STRING,start_date STRING,end_date STRING,daily_budget BIGINT,bid_type STRING,bid BIGINT,
  created_at STRING,updated_at STRING,PRIMARY KEY(unit_id) NOT ENFORCED
) WITH ('bucket.num'='4','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dim_creative_df (
  creative_id BIGINT,creative_name STRING,unit_id BIGINT,unit_name STRING,status INT,
  creative_mode INT,material_mode INT,creative_title STRING,creative_category STRING,
  creative_tags ARRAY<STRING>,creative_text STRING,creative_image_urls STRING,
  creative_video_id BIGINT,monitoring_url STRING,created_at STRING,updated_at STRING,
  PRIMARY KEY(creative_id) NOT ENFORCED
) WITH ('bucket.num'='4','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dim_user_df (
  uid BIGINT,user_name STRING,gender INT,phone_hash STRING,email STRING,user_level INT,
  birthday STRING,status INT,start_dt STRING,end_dt STRING,created_at STRING,updated_at STRING,
  PRIMARY KEY(uid) NOT ENFORCED
) WITH ('bucket.num'='4','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dim_product_df (
  product_id BIGINT,product_name STRING,shop_id BIGINT,shop_name STRING,price BIGINT,
  start_dt STRING,end_dt STRING,created_at STRING,updated_at STRING,
  PRIMARY KEY(product_id) NOT ENFORCED
) WITH ('bucket.num'='4','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dim_shop_df (
  shop_id BIGINT,shop_name STRING,shop_type INT,status INT,main_category_id BIGINT,
  main_category_name STRING,shop_qualification_type INT,credit_code STRING,contact_person STRING,
  contact_phone STRING,start_dt STRING,end_dt STRING,created_at STRING,updated_at STRING,
  PRIMARY KEY(shop_id) NOT ENFORCED
) WITH ('bucket.num'='4','table.datalake.enabled'='true','table.datalake.freshness'='30s');

CREATE TABLE IF NOT EXISTS dwd_ad_event_di (
  event_id BIGINT,uid BIGINT,device_id STRING,platform INT,app_vc STRING,browser_vc STRING,
  sdk_vc STRING,advertiser_id BIGINT,campaign_id BIGINT,unit_id BIGINT,creative_id BIGINT,
  product_id BIGINT,slot_id BIGINT,event_type STRING,placement_type INT,ad_type INT,ts BIGINT,
  event_time TIMESTAMP_LTZ(3),dt STRING,`hour` STRING,
  WATERMARK FOR event_time AS event_time-INTERVAL '5' SECOND,
  PRIMARY KEY(dt,event_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.log.ttl'='6h','table.datalake.enabled'='true','table.datalake.freshness'='30s');
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
CREATE TABLE IF NOT EXISTS dws_unit_di (
  dt STRING,unit_id BIGINT,unit_name STRING,placement_type INT,ad_type INT,
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
  PRIMARY KEY(dt,unit_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dws_creative_di (
  dt STRING,creative_id BIGINT,creative_name STRING,
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
  PRIMARY KEY(dt,creative_id) NOT ENFORCED
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
CREATE DATABASE IF NOT EXISTS ad_dw;
USE ad_dw;

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

CREATE TABLE IF NOT EXISTS ads_advertiser_retention_di (
  dt STRING,advertiser_count BIGINT,retention_rate_1d DOUBLE,retention_rate_7d DOUBLE,
  retention_rate_15d DOUBLE,retention_rate_30d DOUBLE
) PARTITIONED BY(dt) WITH ('bucket'='-1','file.format'='parquet');

-- Daily full topic snapshots. DIM is the driving table; DWS only contributes metrics.
CREATE TABLE IF NOT EXISTS dm_advertiser_df (
  dt STRING,advertiser_id BIGINT,advertiser_name STRING,
  delivery_count_1d BIGINT,
  delivery_count_7d BIGINT,
  delivery_count_30d BIGINT,
  delivery_count_lifetime BIGINT,
  impression_count_1d BIGINT,
  impression_count_7d BIGINT,
  impression_count_30d BIGINT,
  impression_count_lifetime BIGINT,
  click_count_1d BIGINT,
  click_count_7d BIGINT,
  click_count_30d BIGINT,
  click_count_lifetime BIGINT,
  conversion_count_1d BIGINT,
  conversion_count_7d BIGINT,
  conversion_count_30d BIGINT,
  conversion_count_lifetime BIGINT,
  cost_1d BIGINT,
  cost_7d BIGINT,
  cost_30d BIGINT,
  cost_lifetime BIGINT,
  closed_cost_1d BIGINT,
  closed_cost_7d BIGINT,
  closed_cost_30d BIGINT,
  closed_cost_lifetime BIGINT,
  pay_order_count_1d BIGINT,
  pay_order_count_7d BIGINT,
  pay_order_count_30d BIGINT,
  pay_order_count_lifetime BIGINT,
  refund_order_count_1d BIGINT,
  refund_order_count_7d BIGINT,
  refund_order_count_30d BIGINT,
  refund_order_count_lifetime BIGINT,
  pay_order_gmv_1d BIGINT,
  pay_order_gmv_7d BIGINT,
  pay_order_gmv_30d BIGINT,
  pay_order_gmv_lifetime BIGINT,
  refund_order_gmv_1d BIGINT,
  refund_order_gmv_7d BIGINT,
  refund_order_gmv_30d BIGINT,
  refund_order_gmv_lifetime BIGINT,
  PRIMARY KEY(dt,advertiser_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket'='2','file.format'='parquet');

CREATE TABLE IF NOT EXISTS dm_campaign_df (
  dt STRING,campaign_id BIGINT,campaign_name STRING,
  delivery_count_1d BIGINT,
  delivery_count_7d BIGINT,
  delivery_count_30d BIGINT,
  delivery_count_lifetime BIGINT,
  impression_count_1d BIGINT,
  impression_count_7d BIGINT,
  impression_count_30d BIGINT,
  impression_count_lifetime BIGINT,
  click_count_1d BIGINT,
  click_count_7d BIGINT,
  click_count_30d BIGINT,
  click_count_lifetime BIGINT,
  conversion_count_1d BIGINT,
  conversion_count_7d BIGINT,
  conversion_count_30d BIGINT,
  conversion_count_lifetime BIGINT,
  cost_1d BIGINT,
  cost_7d BIGINT,
  cost_30d BIGINT,
  cost_lifetime BIGINT,
  closed_cost_1d BIGINT,
  closed_cost_7d BIGINT,
  closed_cost_30d BIGINT,
  closed_cost_lifetime BIGINT,
  pay_order_count_1d BIGINT,
  pay_order_count_7d BIGINT,
  pay_order_count_30d BIGINT,
  pay_order_count_lifetime BIGINT,
  refund_order_count_1d BIGINT,
  refund_order_count_7d BIGINT,
  refund_order_count_30d BIGINT,
  refund_order_count_lifetime BIGINT,
  pay_order_gmv_1d BIGINT,
  pay_order_gmv_7d BIGINT,
  pay_order_gmv_30d BIGINT,
  pay_order_gmv_lifetime BIGINT,
  refund_order_gmv_1d BIGINT,
  refund_order_gmv_7d BIGINT,
  refund_order_gmv_30d BIGINT,
  refund_order_gmv_lifetime BIGINT,
  PRIMARY KEY(dt,campaign_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket'='2','file.format'='parquet');
CREATE TABLE IF NOT EXISTS dm_unit_df (
  dt STRING,unit_id BIGINT,unit_name STRING,placement_type INT,ad_type INT,
  delivery_count_1d BIGINT,
  delivery_count_7d BIGINT,
  delivery_count_30d BIGINT,
  delivery_count_lifetime BIGINT,
  impression_count_1d BIGINT,
  impression_count_7d BIGINT,
  impression_count_30d BIGINT,
  impression_count_lifetime BIGINT,
  click_count_1d BIGINT,
  click_count_7d BIGINT,
  click_count_30d BIGINT,
  click_count_lifetime BIGINT,
  conversion_count_1d BIGINT,
  conversion_count_7d BIGINT,
  conversion_count_30d BIGINT,
  conversion_count_lifetime BIGINT,
  cost_1d BIGINT,
  cost_7d BIGINT,
  cost_30d BIGINT,
  cost_lifetime BIGINT,
  closed_cost_1d BIGINT,
  closed_cost_7d BIGINT,
  closed_cost_30d BIGINT,
  closed_cost_lifetime BIGINT,
  pay_order_count_1d BIGINT,
  pay_order_count_7d BIGINT,
  pay_order_count_30d BIGINT,
  pay_order_count_lifetime BIGINT,
  refund_order_count_1d BIGINT,
  refund_order_count_7d BIGINT,
  refund_order_count_30d BIGINT,
  refund_order_count_lifetime BIGINT,
  pay_order_gmv_1d BIGINT,
  pay_order_gmv_7d BIGINT,
  pay_order_gmv_30d BIGINT,
  pay_order_gmv_lifetime BIGINT,
  refund_order_gmv_1d BIGINT,
  refund_order_gmv_7d BIGINT,
  refund_order_gmv_30d BIGINT,
  refund_order_gmv_lifetime BIGINT,
  PRIMARY KEY(dt,unit_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket'='2','file.format'='parquet');
CREATE TABLE IF NOT EXISTS dm_creative_df (
  dt STRING,creative_id BIGINT,creative_name STRING,
  delivery_count_1d BIGINT,
  delivery_count_7d BIGINT,
  delivery_count_30d BIGINT,
  delivery_count_lifetime BIGINT,
  impression_count_1d BIGINT,
  impression_count_7d BIGINT,
  impression_count_30d BIGINT,
  impression_count_lifetime BIGINT,
  click_count_1d BIGINT,
  click_count_7d BIGINT,
  click_count_30d BIGINT,
  click_count_lifetime BIGINT,
  conversion_count_1d BIGINT,
  conversion_count_7d BIGINT,
  conversion_count_30d BIGINT,
  conversion_count_lifetime BIGINT,
  cost_1d BIGINT,
  cost_7d BIGINT,
  cost_30d BIGINT,
  cost_lifetime BIGINT,
  closed_cost_1d BIGINT,
  closed_cost_7d BIGINT,
  closed_cost_30d BIGINT,
  closed_cost_lifetime BIGINT,
  pay_order_count_1d BIGINT,
  pay_order_count_7d BIGINT,
  pay_order_count_30d BIGINT,
  pay_order_count_lifetime BIGINT,
  refund_order_count_1d BIGINT,
  refund_order_count_7d BIGINT,
  refund_order_count_30d BIGINT,
  refund_order_count_lifetime BIGINT,
  pay_order_gmv_1d BIGINT,
  pay_order_gmv_7d BIGINT,
  pay_order_gmv_30d BIGINT,
  pay_order_gmv_lifetime BIGINT,
  refund_order_gmv_1d BIGINT,
  refund_order_gmv_7d BIGINT,
  refund_order_gmv_30d BIGINT,
  refund_order_gmv_lifetime BIGINT,
  PRIMARY KEY(dt,creative_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket'='2','file.format'='parquet');
