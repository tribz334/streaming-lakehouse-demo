-- One-time migration to direct-CDC current DIM tables with Paimon daily tags.
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://hive-metastore:9083',
  'warehouse' = 'file:///warehouse/paimon'
);

USE CATALOG paimon;
USE ad_dw;

DROP TABLE IF EXISTS dim_advertiser_zip;
DROP TABLE IF EXISTS dim_user_zip;
DROP TABLE IF EXISTS dim_shop_zip;
DROP TABLE IF EXISTS dim_product_zip;

-- These three names existed as unpartitioned current-state tables. Recreate
-- them through 00_catalogs_and_tables.sql with the new dt partition contract.
DROP TABLE IF EXISTS dim_campaign;
DROP TABLE IF EXISTS dim_unit;
DROP TABLE IF EXISTS dim_creative;

DROP TABLE IF EXISTS dim_advertiser;
DROP TABLE IF EXISTS dim_user;
DROP TABLE IF EXISTS dim_shop;
DROP TABLE IF EXISTS dim_product;

-- Obsolete database ODS mirrors/ledgers. ods_log_inc is intentionally kept:
-- it is the SDK tracking-log ODS table, not a MySQL CDC staging table.
DROP TABLE IF EXISTS ods_shop_info;
DROP TABLE IF EXISTS ods_advertiser_info;
DROP TABLE IF EXISTS ods_campaign_info;
DROP TABLE IF EXISTS ods_unit_info;
DROP TABLE IF EXISTS ods_creative_info;
DROP TABLE IF EXISTS ods_user_info;
DROP TABLE IF EXISTS ods_product_info;
DROP TABLE IF EXISTS ods_advertiser_inc;
DROP TABLE IF EXISTS ods_campaign_inc;
DROP TABLE IF EXISTS ods_unit_inc;
DROP TABLE IF EXISTS ods_creative_inc;
DROP TABLE IF EXISTS ods_user_inc;
DROP TABLE IF EXISTS ods_product_inc;
DROP TABLE IF EXISTS ods_bill_detail_inc;
DROP TABLE IF EXISTS ods_order_detail_inc;
