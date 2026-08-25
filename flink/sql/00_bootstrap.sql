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
  market_goal INT,ad_type INT,trading_mode INT,budget BIGINT,daily_budget BIGINT,created_at STRING,
  updated_at STRING,PRIMARY KEY(campaign_id) NOT ENFORCED
) WITH ('bucket.num'='4','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dim_unit_df (
  unit_id BIGINT,unit_name STRING,campaign_id BIGINT,campaign_name STRING,status INT,is_closed INT,
  delivery_type INT,search_keyword ARRAY<STRING>,product_id BIGINT,landing_page_url STRING,
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
  product_id BIGINT,slot_id BIGINT,event_type STRING,scene STRING,ts BIGINT,
  event_time TIMESTAMP_LTZ(3),dt STRING,`hour` STRING,
  WATERMARK FOR event_time AS event_time-INTERVAL '5' SECOND,
  PRIMARY KEY(dt,event_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.log.ttl'='6h','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dwd_ad_bill_di (
  bill_id BIGINT,creative_id BIGINT,unit_id BIGINT,campaign_id BIGINT,slot_id BIGINT,uid BIGINT,
  advertiser_id BIGINT,cost BIGINT,is_closed INT,ts BIGINT,event_time TIMESTAMP_LTZ(3),dt STRING,`hour` STRING,
  WATERMARK FOR event_time AS event_time-INTERVAL '5' SECOND,PRIMARY KEY(dt,bill_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dwd_ad_order_acc (
  order_id BIGINT,uid BIGINT,product_id BIGINT,shop_id BIGINT,creative_id BIGINT,slot_id BIGINT,
  product_price BIGINT,product_num INT,total_amount BIGINT,payment_method INT,receiver_name STRING,
  receiver_phone STRING,shipping_address STRING,tracking_number STRING,order_status INT,
  create_time STRING,cancel_time STRING,pay_time STRING,confirm_time STRING,refund_time STRING,
  updated_at STRING,advertiser_id BIGINT,
  campaign_id BIGINT,unit_id BIGINT,event_time TIMESTAMP_LTZ(3),dt STRING,`hour` STRING,
  WATERMARK FOR event_time AS event_time-INTERVAL '5' SECOND,
  PRIMARY KEY(dt,order_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dwd_ad_order_di (
  event_id STRING,order_id BIGINT,uid BIGINT,product_id BIGINT,order_type STRING,
  pay_time TIMESTAMP_LTZ(3),pay_order_gmv BIGINT,dt STRING,
  WATERMARK FOR pay_time AS pay_time-INTERVAL '5' SECOND,
  PRIMARY KEY(dt,event_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dws_advertiser_di (
  dt STRING,advertiser_id BIGINT,advertiser_name STRING,
  delivery_count BIGINT,impression_count BIGINT,click_count BIGINT,conversion_count BIGINT,
  cost BIGINT,closed_cost BIGINT,pay_order_count BIGINT,refund_order_count BIGINT,
  pay_order_gmv BIGINT,refund_order_gmv BIGINT,
  ecommerce_pay_order_count BIGINT,ecommerce_refund_order_count BIGINT,
  ecommerce_pay_order_gmv BIGINT,ecommerce_refund_order_gmv BIGINT,
  short_video_pay_order_count BIGINT,short_video_refund_order_count BIGINT,
  short_video_pay_order_gmv BIGINT,short_video_refund_order_gmv BIGINT,
  live_pay_order_count BIGINT,live_refund_order_count BIGINT,
  live_pay_order_gmv BIGINT,live_refund_order_gmv BIGINT,
  PRIMARY KEY(dt,advertiser_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dws_unit_di (
  dt STRING,unit_id BIGINT,unit_name STRING,
  delivery_count BIGINT,impression_count BIGINT,click_count BIGINT,conversion_count BIGINT,
  cost BIGINT,closed_cost BIGINT,pay_order_count BIGINT,refund_order_count BIGINT,
  pay_order_gmv BIGINT,refund_order_gmv BIGINT,
  ecommerce_pay_order_count BIGINT,ecommerce_refund_order_count BIGINT,
  ecommerce_pay_order_gmv BIGINT,ecommerce_refund_order_gmv BIGINT,
  short_video_pay_order_count BIGINT,short_video_refund_order_count BIGINT,
  short_video_pay_order_gmv BIGINT,short_video_refund_order_gmv BIGINT,
  live_pay_order_count BIGINT,live_refund_order_count BIGINT,
  live_pay_order_gmv BIGINT,live_refund_order_gmv BIGINT,
  PRIMARY KEY(dt,unit_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS dws_creative_di (
  dt STRING,creative_id BIGINT,creative_name STRING,
  delivery_count BIGINT,impression_count BIGINT,click_count BIGINT,conversion_count BIGINT,
  cost BIGINT,closed_cost BIGINT,pay_order_count BIGINT,refund_order_count BIGINT,
  pay_order_gmv BIGINT,refund_order_gmv BIGINT,
  ecommerce_pay_order_count BIGINT,ecommerce_refund_order_count BIGINT,
  ecommerce_pay_order_gmv BIGINT,ecommerce_refund_order_gmv BIGINT,
  short_video_pay_order_count BIGINT,short_video_refund_order_count BIGINT,
  short_video_pay_order_gmv BIGINT,short_video_refund_order_gmv BIGINT,
  live_pay_order_count BIGINT,live_refund_order_count BIGINT,
  live_pay_order_gmv BIGINT,live_refund_order_gmv BIGINT,
  PRIMARY KEY(dt,creative_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');
CREATE TABLE IF NOT EXISTS ads_realtime_metric_30s (
  window_start TIMESTAMP_LTZ(3),window_end TIMESTAMP_LTZ(3),advertiser_id BIGINT,
  campaign_id BIGINT,unit_id BIGINT,creative_id BIGINT,delivery_count BIGINT,
  impression_count BIGINT,click_count BIGINT,conversion_count BIGINT,cost BIGINT,
  closed_cost BIGINT,pay_order_count BIGINT,refund_order_count BIGINT,
  pay_order_gmv BIGINT,refund_order_gmv BIGINT,
  ecommerce_pay_order_count BIGINT,ecommerce_refund_order_count BIGINT,
  ecommerce_pay_order_gmv BIGINT,ecommerce_refund_order_gmv BIGINT,
  short_video_pay_order_count BIGINT,short_video_refund_order_count BIGINT,
  short_video_pay_order_gmv BIGINT,short_video_refund_order_gmv BIGINT,
  live_pay_order_count BIGINT,live_refund_order_count BIGINT,
  live_pay_order_gmv BIGINT,live_refund_order_gmv BIGINT,
  dt STRING,
  PRIMARY KEY(dt,window_start,creative_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket.num'='2','table.datalake.enabled'='true','table.datalake.freshness'='30s');

-- COLD PATH: native Paimon daily outputs; realtime jobs never read this catalog.
CREATE CATALOG paimon WITH ('type'='paimon','metastore'='filesystem','warehouse'='file:///warehouse/paimon');
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ad_dw;
USE ad_dw;
CREATE TABLE IF NOT EXISTS ads_offline_metric_di (
  dt STRING,advertiser_id BIGINT,campaign_id BIGINT,unit_id BIGINT,creative_id BIGINT,
  delivery_count BIGINT,impression_count BIGINT,click_count BIGINT,conversion_count BIGINT,
  cost BIGINT,closed_cost BIGINT,pay_order_count BIGINT,refund_order_count BIGINT,
  pay_order_gmv BIGINT,refund_order_gmv BIGINT,
  ecommerce_pay_order_count BIGINT,ecommerce_refund_order_count BIGINT,
  ecommerce_pay_order_gmv BIGINT,ecommerce_refund_order_gmv BIGINT,
  short_video_pay_order_count BIGINT,short_video_refund_order_count BIGINT,
  short_video_pay_order_gmv BIGINT,short_video_refund_order_gmv BIGINT,
  live_pay_order_count BIGINT,live_refund_order_count BIGINT,
  live_pay_order_gmv BIGINT,live_refund_order_gmv BIGINT,
  PRIMARY KEY(dt,creative_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket'='2','file.format'='parquet');
CREATE TABLE IF NOT EXISTS ads_order_attribution_di (
  dt STRING,order_id BIGINT,uid BIGINT,product_id BIGINT,pay_time TIMESTAMP_LTZ(3),
  pay_order_gmv BIGINT,last_click_time TIMESTAMP_LTZ(3),advertiser_id BIGINT,
  campaign_id BIGINT,unit_id BIGINT,creative_id BIGINT,attribute_period STRING,
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
delivery_count_acc BIGINT,
  impression_count_1d BIGINT,
impression_count_7d BIGINT,
impression_count_30d BIGINT,
impression_count_acc BIGINT,
  click_count_1d BIGINT,
click_count_7d BIGINT,
click_count_30d BIGINT,
click_count_acc BIGINT,
  conversion_count_1d BIGINT,
conversion_count_7d BIGINT,
conversion_count_30d BIGINT,
conversion_count_acc BIGINT,
  cost_1d BIGINT,
cost_7d BIGINT,
cost_30d BIGINT,
cost_acc BIGINT,
  closed_cost_1d BIGINT,
closed_cost_7d BIGINT,
closed_cost_30d BIGINT,
closed_cost_acc BIGINT,
  pay_order_count_1d BIGINT,
pay_order_count_7d BIGINT,
pay_order_count_30d BIGINT,
pay_order_count_acc BIGINT,
  refund_order_count_1d BIGINT,
refund_order_count_7d BIGINT,
refund_order_count_30d BIGINT,
refund_order_count_acc BIGINT,
  pay_order_gmv_1d BIGINT,
pay_order_gmv_7d BIGINT,
pay_order_gmv_30d BIGINT,
pay_order_gmv_acc BIGINT,
  refund_order_gmv_1d BIGINT,
refund_order_gmv_7d BIGINT,
refund_order_gmv_30d BIGINT,
refund_order_gmv_acc BIGINT,
  ecommerce_pay_order_count_1d BIGINT,ecommerce_pay_order_count_7d BIGINT,
  ecommerce_pay_order_count_30d BIGINT,ecommerce_pay_order_count_acc BIGINT,
  ecommerce_refund_order_count_1d BIGINT,ecommerce_refund_order_count_7d BIGINT,
  ecommerce_refund_order_count_30d BIGINT,ecommerce_refund_order_count_acc BIGINT,
  ecommerce_pay_order_gmv_1d BIGINT,ecommerce_pay_order_gmv_7d BIGINT,
  ecommerce_pay_order_gmv_30d BIGINT,ecommerce_pay_order_gmv_acc BIGINT,
  ecommerce_refund_order_gmv_1d BIGINT,ecommerce_refund_order_gmv_7d BIGINT,
  ecommerce_refund_order_gmv_30d BIGINT,ecommerce_refund_order_gmv_acc BIGINT,
  short_video_pay_order_count_1d BIGINT,short_video_pay_order_count_7d BIGINT,
  short_video_pay_order_count_30d BIGINT,short_video_pay_order_count_acc BIGINT,
  short_video_refund_order_count_1d BIGINT,short_video_refund_order_count_7d BIGINT,
  short_video_refund_order_count_30d BIGINT,short_video_refund_order_count_acc BIGINT,
  short_video_pay_order_gmv_1d BIGINT,short_video_pay_order_gmv_7d BIGINT,
  short_video_pay_order_gmv_30d BIGINT,short_video_pay_order_gmv_acc BIGINT,
  short_video_refund_order_gmv_1d BIGINT,short_video_refund_order_gmv_7d BIGINT,
  short_video_refund_order_gmv_30d BIGINT,short_video_refund_order_gmv_acc BIGINT,
  live_pay_order_count_1d BIGINT,live_pay_order_count_7d BIGINT,
  live_pay_order_count_30d BIGINT,live_pay_order_count_acc BIGINT,
  live_refund_order_count_1d BIGINT,live_refund_order_count_7d BIGINT,
  live_refund_order_count_30d BIGINT,live_refund_order_count_acc BIGINT,
  live_pay_order_gmv_1d BIGINT,live_pay_order_gmv_7d BIGINT,
  live_pay_order_gmv_30d BIGINT,live_pay_order_gmv_acc BIGINT,
  live_refund_order_gmv_1d BIGINT,live_refund_order_gmv_7d BIGINT,
  live_refund_order_gmv_30d BIGINT,live_refund_order_gmv_acc BIGINT,
  PRIMARY KEY(dt,advertiser_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket'='2','file.format'='parquet');
CREATE TABLE IF NOT EXISTS dm_unit_df (
  dt STRING,unit_id BIGINT,unit_name STRING,
  delivery_count_1d BIGINT,
delivery_count_7d BIGINT,
delivery_count_30d BIGINT,
delivery_count_acc BIGINT,
  impression_count_1d BIGINT,
impression_count_7d BIGINT,
impression_count_30d BIGINT,
impression_count_acc BIGINT,
  click_count_1d BIGINT,
click_count_7d BIGINT,
click_count_30d BIGINT,
click_count_acc BIGINT,
  conversion_count_1d BIGINT,
conversion_count_7d BIGINT,
conversion_count_30d BIGINT,
conversion_count_acc BIGINT,
  cost_1d BIGINT,
cost_7d BIGINT,
cost_30d BIGINT,
cost_acc BIGINT,
  closed_cost_1d BIGINT,
closed_cost_7d BIGINT,
closed_cost_30d BIGINT,
closed_cost_acc BIGINT,
  pay_order_count_1d BIGINT,
pay_order_count_7d BIGINT,
pay_order_count_30d BIGINT,
pay_order_count_acc BIGINT,
  refund_order_count_1d BIGINT,
refund_order_count_7d BIGINT,
refund_order_count_30d BIGINT,
refund_order_count_acc BIGINT,
  pay_order_gmv_1d BIGINT,
pay_order_gmv_7d BIGINT,
pay_order_gmv_30d BIGINT,
pay_order_gmv_acc BIGINT,
  refund_order_gmv_1d BIGINT,
refund_order_gmv_7d BIGINT,
refund_order_gmv_30d BIGINT,
refund_order_gmv_acc BIGINT,
  ecommerce_pay_order_count_1d BIGINT,ecommerce_pay_order_count_7d BIGINT,
  ecommerce_pay_order_count_30d BIGINT,ecommerce_pay_order_count_acc BIGINT,
  ecommerce_refund_order_count_1d BIGINT,ecommerce_refund_order_count_7d BIGINT,
  ecommerce_refund_order_count_30d BIGINT,ecommerce_refund_order_count_acc BIGINT,
  ecommerce_pay_order_gmv_1d BIGINT,ecommerce_pay_order_gmv_7d BIGINT,
  ecommerce_pay_order_gmv_30d BIGINT,ecommerce_pay_order_gmv_acc BIGINT,
  ecommerce_refund_order_gmv_1d BIGINT,ecommerce_refund_order_gmv_7d BIGINT,
  ecommerce_refund_order_gmv_30d BIGINT,ecommerce_refund_order_gmv_acc BIGINT,
  short_video_pay_order_count_1d BIGINT,short_video_pay_order_count_7d BIGINT,
  short_video_pay_order_count_30d BIGINT,short_video_pay_order_count_acc BIGINT,
  short_video_refund_order_count_1d BIGINT,short_video_refund_order_count_7d BIGINT,
  short_video_refund_order_count_30d BIGINT,short_video_refund_order_count_acc BIGINT,
  short_video_pay_order_gmv_1d BIGINT,short_video_pay_order_gmv_7d BIGINT,
  short_video_pay_order_gmv_30d BIGINT,short_video_pay_order_gmv_acc BIGINT,
  short_video_refund_order_gmv_1d BIGINT,short_video_refund_order_gmv_7d BIGINT,
  short_video_refund_order_gmv_30d BIGINT,short_video_refund_order_gmv_acc BIGINT,
  live_pay_order_count_1d BIGINT,live_pay_order_count_7d BIGINT,
  live_pay_order_count_30d BIGINT,live_pay_order_count_acc BIGINT,
  live_refund_order_count_1d BIGINT,live_refund_order_count_7d BIGINT,
  live_refund_order_count_30d BIGINT,live_refund_order_count_acc BIGINT,
  live_pay_order_gmv_1d BIGINT,live_pay_order_gmv_7d BIGINT,
  live_pay_order_gmv_30d BIGINT,live_pay_order_gmv_acc BIGINT,
  live_refund_order_gmv_1d BIGINT,live_refund_order_gmv_7d BIGINT,
  live_refund_order_gmv_30d BIGINT,live_refund_order_gmv_acc BIGINT,
  PRIMARY KEY(dt,unit_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket'='2','file.format'='parquet');
CREATE TABLE IF NOT EXISTS dm_creative_df (
  dt STRING,creative_id BIGINT,creative_name STRING,
  delivery_count_1d BIGINT,
delivery_count_7d BIGINT,
delivery_count_30d BIGINT,
delivery_count_acc BIGINT,
  impression_count_1d BIGINT,
impression_count_7d BIGINT,
impression_count_30d BIGINT,
impression_count_acc BIGINT,
  click_count_1d BIGINT,
click_count_7d BIGINT,
click_count_30d BIGINT,
click_count_acc BIGINT,
  conversion_count_1d BIGINT,
conversion_count_7d BIGINT,
conversion_count_30d BIGINT,
conversion_count_acc BIGINT,
  cost_1d BIGINT,
cost_7d BIGINT,
cost_30d BIGINT,
cost_acc BIGINT,
  closed_cost_1d BIGINT,
closed_cost_7d BIGINT,
closed_cost_30d BIGINT,
closed_cost_acc BIGINT,
  pay_order_count_1d BIGINT,
pay_order_count_7d BIGINT,
pay_order_count_30d BIGINT,
pay_order_count_acc BIGINT,
  refund_order_count_1d BIGINT,
refund_order_count_7d BIGINT,
refund_order_count_30d BIGINT,
refund_order_count_acc BIGINT,
  pay_order_gmv_1d BIGINT,
pay_order_gmv_7d BIGINT,
pay_order_gmv_30d BIGINT,
pay_order_gmv_acc BIGINT,
  refund_order_gmv_1d BIGINT,
refund_order_gmv_7d BIGINT,
refund_order_gmv_30d BIGINT,
refund_order_gmv_acc BIGINT,
  ecommerce_pay_order_count_1d BIGINT,ecommerce_pay_order_count_7d BIGINT,
  ecommerce_pay_order_count_30d BIGINT,ecommerce_pay_order_count_acc BIGINT,
  ecommerce_refund_order_count_1d BIGINT,ecommerce_refund_order_count_7d BIGINT,
  ecommerce_refund_order_count_30d BIGINT,ecommerce_refund_order_count_acc BIGINT,
  ecommerce_pay_order_gmv_1d BIGINT,ecommerce_pay_order_gmv_7d BIGINT,
  ecommerce_pay_order_gmv_30d BIGINT,ecommerce_pay_order_gmv_acc BIGINT,
  ecommerce_refund_order_gmv_1d BIGINT,ecommerce_refund_order_gmv_7d BIGINT,
  ecommerce_refund_order_gmv_30d BIGINT,ecommerce_refund_order_gmv_acc BIGINT,
  short_video_pay_order_count_1d BIGINT,short_video_pay_order_count_7d BIGINT,
  short_video_pay_order_count_30d BIGINT,short_video_pay_order_count_acc BIGINT,
  short_video_refund_order_count_1d BIGINT,short_video_refund_order_count_7d BIGINT,
  short_video_refund_order_count_30d BIGINT,short_video_refund_order_count_acc BIGINT,
  short_video_pay_order_gmv_1d BIGINT,short_video_pay_order_gmv_7d BIGINT,
  short_video_pay_order_gmv_30d BIGINT,short_video_pay_order_gmv_acc BIGINT,
  short_video_refund_order_gmv_1d BIGINT,short_video_refund_order_gmv_7d BIGINT,
  short_video_refund_order_gmv_30d BIGINT,short_video_refund_order_gmv_acc BIGINT,
  live_pay_order_count_1d BIGINT,live_pay_order_count_7d BIGINT,
  live_pay_order_count_30d BIGINT,live_pay_order_count_acc BIGINT,
  live_refund_order_count_1d BIGINT,live_refund_order_count_7d BIGINT,
  live_refund_order_count_30d BIGINT,live_refund_order_count_acc BIGINT,
  live_pay_order_gmv_1d BIGINT,live_pay_order_gmv_7d BIGINT,
  live_pay_order_gmv_30d BIGINT,live_pay_order_gmv_acc BIGINT,
  live_refund_order_gmv_1d BIGINT,live_refund_order_gmv_7d BIGINT,
  live_refund_order_gmv_30d BIGINT,live_refund_order_gmv_acc BIGINT,
  PRIMARY KEY(dt,creative_id) NOT ENFORCED
) PARTITIONED BY(dt) WITH ('bucket'='2','file.format'='parquet');
