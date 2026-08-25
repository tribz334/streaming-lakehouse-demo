CREATE DATABASE IF NOT EXISTS ad_ads;
CREATE EXTERNAL CATALOG IF NOT EXISTS paimon_catalog
PROPERTIES (
  "type"="paimon",
  "paimon.catalog.type"="filesystem",
  "paimon.catalog.warehouse"="file:///warehouse/paimon"
);

DROP VIEW IF EXISTS ad_ads.v_ads_realtime_metric_30s;
DROP VIEW IF EXISTS ad_ads.v_ads_offline_metric_1d;
DROP VIEW IF EXISTS ad_ads.v_ads_offline_metric_di;
DROP VIEW IF EXISTS ad_ads.v_ads_order_attribution_di;
DROP VIEW IF EXISTS ad_ads.v_realtime_metric;
DROP VIEW IF EXISTS ad_ads.v_offline_metric;
DROP VIEW IF EXISTS ad_ads.v_order_attribution;
DROP VIEW IF EXISTS ad_ads.v_dws_creative_df;
DROP VIEW IF EXISTS ad_ads.v_dws_advertiser_di;
DROP VIEW IF EXISTS ad_ads.v_dws_unit_di;
DROP VIEW IF EXISTS ad_ads.v_dws_creative_di;
DROP VIEW IF EXISTS ad_ads.v_ads_advertiser_retention_di;
DROP VIEW IF EXISTS ad_ads.v_dm_advertiser_df;
DROP VIEW IF EXISTS ad_ads.v_dm_unit_df;
DROP VIEW IF EXISTS ad_ads.v_dm_creative_df;
DROP VIEW IF EXISTS ad_ads.v_ads_platform_order_gmv;
DROP TABLE IF EXISTS ad_ads.ads_platform_order_gmv;

CREATE VIEW ad_ads.v_dws_advertiser_di AS
SELECT dt,advertiser_id,advertiser_name,
  delivery_count,impression_count,click_count,conversion_count,
  cost,closed_cost,pay_order_count,refund_order_count,pay_order_gmv,refund_order_gmv,
  ecommerce_pay_order_count,ecommerce_refund_order_count,ecommerce_pay_order_gmv,ecommerce_refund_order_gmv,
  short_video_pay_order_count,short_video_refund_order_count,short_video_pay_order_gmv,short_video_refund_order_gmv,
  live_pay_order_count,live_refund_order_count,live_pay_order_gmv,live_refund_order_gmv
FROM paimon_catalog.ad_dw.dws_advertiser_di;

CREATE VIEW ad_ads.v_dws_unit_di AS
SELECT dt,unit_id,unit_name,
  delivery_count,impression_count,click_count,conversion_count,
  cost,closed_cost,pay_order_count,refund_order_count,pay_order_gmv,refund_order_gmv,
  ecommerce_pay_order_count,ecommerce_refund_order_count,ecommerce_pay_order_gmv,ecommerce_refund_order_gmv,
  short_video_pay_order_count,short_video_refund_order_count,short_video_pay_order_gmv,short_video_refund_order_gmv,
  live_pay_order_count,live_refund_order_count,live_pay_order_gmv,live_refund_order_gmv
FROM paimon_catalog.ad_dw.dws_unit_di;

CREATE VIEW ad_ads.v_dws_creative_di AS
SELECT dt,creative_id,creative_name,
  delivery_count,impression_count,click_count,conversion_count,
  cost,closed_cost,pay_order_count,refund_order_count,pay_order_gmv,refund_order_gmv,
  ecommerce_pay_order_count,ecommerce_refund_order_count,ecommerce_pay_order_gmv,ecommerce_refund_order_gmv,
  short_video_pay_order_count,short_video_refund_order_count,short_video_pay_order_gmv,short_video_refund_order_gmv,
  live_pay_order_count,live_refund_order_count,live_pay_order_gmv,live_refund_order_gmv
FROM paimon_catalog.ad_dw.dws_creative_di;

CREATE VIEW ad_ads.v_ads_realtime_metric_30s AS
SELECT * FROM paimon_catalog.ad_dw.ads_realtime_metric_30s;

CREATE VIEW ad_ads.v_ads_offline_metric_di AS
SELECT * FROM paimon_catalog.ad_dw.ads_offline_metric_di;

CREATE VIEW ad_ads.v_ads_order_attribution_di AS
SELECT * FROM paimon_catalog.ad_dw.ads_order_attribution_di;

-- Stable BI contracts. Superset only reads these StarRocks views and never
-- connects to Fluss or Paimon directly. The v_ads_* aliases above are kept for
-- compatibility with previously created datasets/charts.
CREATE VIEW ad_ads.v_realtime_metric AS
SELECT * FROM paimon_catalog.ad_dw.ads_realtime_metric_30s;

CREATE VIEW ad_ads.v_offline_metric AS
SELECT * FROM paimon_catalog.ad_dw.ads_offline_metric_di;

CREATE VIEW ad_ads.v_order_attribution AS
SELECT * FROM paimon_catalog.ad_dw.ads_order_attribution_di;

CREATE VIEW ad_ads.v_ads_advertiser_retention_di AS
SELECT dt,advertiser_count,retention_rate_1d,retention_rate_7d,
  retention_rate_15d,retention_rate_30d
FROM paimon_catalog.ad_dw.ads_advertiser_retention_di;

CREATE VIEW ad_ads.v_dm_advertiser_df AS
SELECT * FROM paimon_catalog.ad_dw.dm_advertiser_df;

CREATE VIEW ad_ads.v_dm_unit_df AS
SELECT * FROM paimon_catalog.ad_dw.dm_unit_df;

CREATE VIEW ad_ads.v_dm_creative_df AS
SELECT * FROM paimon_catalog.ad_dw.dm_creative_df;
