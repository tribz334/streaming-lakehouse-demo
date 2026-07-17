SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:///warehouse/paimon'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ad_dw;
USE ad_dw;

DROP TABLE IF EXISTS dim_customer_df;
DROP TABLE IF EXISTS dim_slot_df;
DROP TABLE IF EXISTS dim_user_df;
DROP TABLE IF EXISTS dim_shop_df;
DROP TABLE IF EXISTS dim_product_df;
DROP TABLE IF EXISTS dwd_order_lifecycle_df;
DROP TABLE IF EXISTS dwd_ad_bid_di;
DROP TABLE IF EXISTS dwd_ad_impression_di;
DROP TABLE IF EXISTS dwd_ad_click_di;
DROP TABLE IF EXISTS dwd_ad_conversion_di;
DROP TABLE IF EXISTS dwd_ad_cost_di;
DROP TABLE IF EXISTS dwm_ad_event_wide;
DROP TABLE IF EXISTS dws_advertiser_df;
DROP TABLE IF EXISTS dws_campaign_df;
DROP TABLE IF EXISTS dws_slot_df;
DROP TABLE IF EXISTS dws_user_df;
DROP TABLE IF EXISTS dws_region_df;
DROP TABLE IF EXISTS dws_ad_stream_10s;
