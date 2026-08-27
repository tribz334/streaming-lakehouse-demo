CREATE DATABASE IF NOT EXISTS ad_ads;
DROP VIEW IF EXISTS ad_ads.v_ads_realtime_metric_30s;
DROP VIEW IF EXISTS ad_ads.v_ads_realtime_metric_10s;
DROP VIEW IF EXISTS ad_ads.v_ads_offline_metric_1d;
DROP VIEW IF EXISTS ad_ads.v_ads_offline_metric_di;
DROP VIEW IF EXISTS ad_ads.v_ads_order_attribution_di;
DROP VIEW IF EXISTS ad_ads.v_realtime_metric;
DROP VIEW IF EXISTS ad_ads.v_offline_metric;
DROP VIEW IF EXISTS ad_ads.v_order_attribution;
DROP VIEW IF EXISTS ad_ads.v_dws_advertiser_di;
DROP VIEW IF EXISTS ad_ads.v_dws_campaign_di;
DROP VIEW IF EXISTS ad_ads.v_dws_unit_di;
DROP VIEW IF EXISTS ad_ads.v_dws_creative_di;
DROP VIEW IF EXISTS ad_ads.v_dws_placement_di;
DROP VIEW IF EXISTS ad_ads.v_dws_ad_type_di;
DROP VIEW IF EXISTS ad_ads.v_ads_advertiser_retention_di;
DROP VIEW IF EXISTS ad_ads.v_dm_advertiser_df;
DROP VIEW IF EXISTS ad_ads.v_dm_campaign_df;
DROP VIEW IF EXISTS ad_ads.v_dm_unit_df;
DROP VIEW IF EXISTS ad_ads.v_dm_creative_df;
DROP VIEW IF EXISTS ad_ads.v_dm_ad_type_df;
DROP VIEW IF EXISTS ad_ads.v_dm_placement_df;

-- Re-register the filesystem catalog so removed/renamed Paimon tables do not
-- remain visible through StarRocks' external metadata cache.
DROP CATALOG IF EXISTS paimon_catalog;
CREATE EXTERNAL CATALOG paimon_catalog PROPERTIES (
  "type"="paimon","paimon.catalog.type"="filesystem","paimon.catalog.warehouse"="file:///warehouse/paimon"
);

CREATE VIEW ad_ads.v_dws_advertiser_di AS SELECT * FROM paimon_catalog.ad_dw.dws_advertiser_di;
CREATE VIEW ad_ads.v_dws_campaign_di AS SELECT * FROM paimon_catalog.ad_dw.dws_campaign_di;
CREATE VIEW ad_ads.v_dws_unit_di AS SELECT * FROM paimon_catalog.ad_dw.dws_unit_di;
CREATE VIEW ad_ads.v_dws_creative_di AS SELECT * FROM paimon_catalog.ad_dw.dws_creative_di;
CREATE VIEW ad_ads.v_ads_realtime_metric_10s AS SELECT * FROM paimon_catalog.ad_dw.ads_realtime_metric_10s;
CREATE VIEW ad_ads.v_ads_offline_metric_di AS SELECT * FROM paimon_catalog.ad_dw.ads_offline_metric_di;
CREATE VIEW ad_ads.v_ads_order_attribution_di AS SELECT * FROM paimon_catalog.ad_dw.ads_order_attribution_di;

CREATE VIEW ad_ads.v_realtime_metric AS
SELECT window_start,window_end,dt,
  SUM(delivery_count) AS delivery_count,
  SUM(impression_count) AS impression_count,
  SUM(click_count) AS click_count,
  SUM(conversion_count) AS conversion_count,
  SUM(cost) AS cost,
  SUM(closed_cost) AS closed_cost,
  SUM(pay_order_count) AS pay_order_count,
  SUM(refund_order_count) AS refund_order_count,
  SUM(pay_order_gmv) AS pay_order_gmv,
  SUM(refund_order_gmv) AS refund_order_gmv,
  SUM(short_video_pay_order_gmv) AS short_video_pay_order_gmv,
  SUM(live_pay_order_gmv) AS live_pay_order_gmv,
  SUM(image_text_pay_order_gmv) AS image_text_pay_order_gmv,
  SUM(other_ad_type_pay_order_gmv) AS other_ad_type_pay_order_gmv,
  SUM(search_pay_order_gmv) AS search_pay_order_gmv,
  SUM(splash_pay_order_gmv) AS splash_pay_order_gmv,
  SUM(feed_pay_order_gmv) AS feed_pay_order_gmv,
  SUM(rewarded_pay_order_gmv) AS rewarded_pay_order_gmv,
  SUM(banner_pay_order_gmv) AS banner_pay_order_gmv,
  SUM(other_placement_pay_order_gmv) AS other_placement_pay_order_gmv,
  CASE WHEN SUM(closed_cost)>0 THEN SUM(pay_order_gmv)*1.0/SUM(closed_cost) ELSE 0 END AS realtime_roas
FROM paimon_catalog.ad_dw.ads_realtime_metric_10s
GROUP BY window_start,window_end,dt;

CREATE VIEW ad_ads.v_offline_metric AS
SELECT dt,
  delivery_count,
  impression_count,
  click_count,
  conversion_count,
  cost,
  closed_cost,
  pay_order_count,
  refund_order_count,
  pay_order_gmv,
  refund_order_gmv,
  short_video_pay_order_gmv,
  live_pay_order_gmv,
  image_text_pay_order_gmv,
  search_pay_order_gmv,
  splash_pay_order_gmv,
  feed_pay_order_gmv,
  rewarded_pay_order_gmv,
  banner_pay_order_gmv,
  other_placement_pay_order_gmv,
  CASE WHEN impression_count>0 THEN click_count*1.0/impression_count ELSE 0 END AS ctr,
  CASE WHEN click_count>0 THEN conversion_count*1.0/click_count ELSE 0 END AS cvr,
  CASE WHEN closed_cost>0 THEN pay_order_gmv*1.0/closed_cost ELSE 0 END AS roas
FROM paimon_catalog.ad_dw.ads_offline_metric_di;

CREATE VIEW ad_ads.v_order_attribution AS SELECT * FROM paimon_catalog.ad_dw.ads_order_attribution_di;
CREATE VIEW ad_ads.v_ads_advertiser_retention_di AS SELECT * FROM paimon_catalog.ad_dw.ads_advertiser_retention_di;
CREATE VIEW ad_ads.v_dm_advertiser_df AS SELECT * FROM paimon_catalog.ad_dw.dm_advertiser_df;
CREATE VIEW ad_ads.v_dm_campaign_df AS SELECT * FROM paimon_catalog.ad_dw.dm_campaign_df;
CREATE VIEW ad_ads.v_dm_unit_df AS SELECT * FROM paimon_catalog.ad_dw.dm_unit_df;
CREATE VIEW ad_ads.v_dm_creative_df AS SELECT * FROM paimon_catalog.ad_dw.dm_creative_df;
