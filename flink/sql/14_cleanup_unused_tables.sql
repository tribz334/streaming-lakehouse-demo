SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://hive-metastore:9083',
  'warehouse' = 'file:///warehouse/paimon'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ad_dw;
USE ad_dw;

-- Retired persistent ODS mirrors. ods_log_inc is the only ODS table.
DROP TABLE IF EXISTS ods_dim_advertiser_current;
DROP TABLE IF EXISTS ods_dim_campaign_current;
DROP TABLE IF EXISTS ods_dim_unit_current;
DROP TABLE IF EXISTS ods_dim_creative_current;
DROP TABLE IF EXISTS ods_dim_user_current;
DROP TABLE IF EXISTS ods_dim_shop_current;
DROP TABLE IF EXISTS ods_dim_product_current;
DROP TABLE IF EXISTS ods_order;
DROP TABLE IF EXISTS ods_ad_bill;
DROP TABLE IF EXISTS ods_ad_events_di;
DROP TABLE IF EXISTS ods_order_attribution;
DROP TABLE IF EXISTS dwd_order_attribution;

DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_slot;
DROP TABLE IF EXISTS dim_advertiser;
DROP TABLE IF EXISTS dim_user;
DROP TABLE IF EXISTS dim_shop;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dwd_ad_bid_di;
DROP TABLE IF EXISTS dwd_ad_impression_di;
DROP TABLE IF EXISTS dwd_ad_click_di;
DROP TABLE IF EXISTS dwd_ad_conversion_di;
DROP TABLE IF EXISTS dwd_ad_cost_di;
DROP TABLE IF EXISTS dwm_ad_event_wide;
DROP TABLE IF EXISTS dws_slot;
DROP TABLE IF EXISTS dws_user;
DROP TABLE IF EXISTS dws_region;
DROP TABLE IF EXISTS dws_ad_stream_10s;
