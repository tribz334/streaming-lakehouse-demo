CREATE DATABASE IF NOT EXISTS ad_ads;
-- Local demo has one BE; thesis uses replication_num=2 on a multi-BE cluster.
CREATE TABLE IF NOT EXISTS ad_ads.dws_ad_creative_10s (
  dt DATE NOT NULL,
  window_start DATETIME NOT NULL,
  creative_id BIGINT NOT NULL,
  window_end DATETIME,
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
  other_placement_pay_order_gmv BIGINT
) PRIMARY KEY (dt, window_start, creative_id)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(creative_id) BUCKETS 4
PROPERTIES ("replication_num"="1", "enable_persistent_index"="true", "compression"="LZ4");

DROP VIEW IF EXISTS ad_ads.v_ads_realtime_metric_30s;
DROP VIEW IF EXISTS ad_ads.v_ads_realtime_metric_10s;
DROP VIEW IF EXISTS ad_ads.v_ads_offline_metric_1d;
DROP VIEW IF EXISTS ad_ads.v_ads_offline_metric_di;
DROP VIEW IF EXISTS ad_ads.v_ads_order_attribution_di;
DROP VIEW IF EXISTS ad_ads.v_realtime_metric_latest;
DROP VIEW IF EXISTS ad_ads.v_realtime_metric;
DROP VIEW IF EXISTS ad_ads.v_offline_metric;
DROP VIEW IF EXISTS ad_ads.v_order_attribution;
DROP VIEW IF EXISTS ad_ads.v_dws_ad_advertiser_di;
DROP VIEW IF EXISTS ad_ads.v_dws_ad_campaign_di;
DROP VIEW IF EXISTS ad_ads.v_dws_ad_unit_di;
DROP VIEW IF EXISTS ad_ads.v_dws_ad_creative_di;
DROP VIEW IF EXISTS ad_ads.v_dws_placement_di;
DROP VIEW IF EXISTS ad_ads.v_dws_ad_type_di;
DROP VIEW IF EXISTS ad_ads.v_ads_advertiser_retention_di;
DROP VIEW IF EXISTS ad_ads.v_dm_ad_advertiser_df;
DROP VIEW IF EXISTS ad_ads.v_dm_ad_campaign_df;
DROP VIEW IF EXISTS ad_ads.v_dm_ad_unit_df;
DROP VIEW IF EXISTS ad_ads.v_dm_ad_creative_df;
DROP VIEW IF EXISTS ad_ads.v_dm_ad_type_df;
DROP VIEW IF EXISTS ad_ads.v_dm_placement_df;

-- Re-register the filesystem catalog so removed/renamed Paimon tables do not
-- remain visible through StarRocks' external metadata cache.
DROP CATALOG IF EXISTS paimon_catalog;
CREATE EXTERNAL CATALOG paimon_catalog PROPERTIES (
  "type"="paimon","paimon.catalog.type"="filesystem","paimon.catalog.warehouse"="file:///warehouse/paimon"
);

CREATE VIEW ad_ads.v_dws_ad_advertiser_di AS SELECT * FROM paimon_catalog.ad_dw.dws_ad_advertiser_di;
CREATE VIEW ad_ads.v_dws_ad_campaign_di AS SELECT * FROM paimon_catalog.ad_dw.dws_ad_campaign_di;
CREATE VIEW ad_ads.v_dws_ad_unit_di AS SELECT * FROM paimon_catalog.ad_dw.dws_ad_unit_di;
CREATE VIEW ad_ads.v_dws_ad_creative_di AS SELECT * FROM paimon_catalog.ad_dw.dws_ad_creative_di;
CREATE VIEW ad_ads.v_ads_realtime_metric_10s AS SELECT * FROM ad_ads.dws_ad_creative_10s;
CREATE VIEW ad_ads.v_ads_offline_metric_di AS SELECT * FROM paimon_catalog.ad_dw.ads_offline_metric_di;
CREATE VIEW ad_ads.v_ads_order_attribution_di AS SELECT * FROM paimon_catalog.ad_dw.ads_order_attribution_di;

-- Monetary facts are stored as one-thousandth of a fen. Serving views expose yuan.
CREATE VIEW ad_ads.v_realtime_metric AS
SELECT window_start,window_end,dt,
  SUM(delivery_count) AS delivery_count,
  SUM(impression_count) AS impression_count,
  SUM(click_count) AS click_count,
  SUM(conversion_count) AS conversion_count,
  CAST(SUM(cost) AS DECIMAL(38,5))/100000 AS cost,
  CAST(SUM(closed_cost) AS DECIMAL(38,5))/100000 AS closed_cost,
  SUM(pay_order_count) AS pay_order_count,
  SUM(refund_order_count) AS refund_order_count,
  CAST(SUM(pay_order_gmv) AS DECIMAL(38,5))/100000 AS pay_order_gmv,
  CAST(SUM(refund_order_gmv) AS DECIMAL(38,5))/100000 AS refund_order_gmv,
  CAST(SUM(short_video_pay_order_gmv) AS DECIMAL(38,5))/100000 AS short_video_pay_order_gmv,
  CAST(SUM(live_pay_order_gmv) AS DECIMAL(38,5))/100000 AS live_pay_order_gmv,
  CAST(SUM(image_text_pay_order_gmv) AS DECIMAL(38,5))/100000 AS image_text_pay_order_gmv,
  CAST(SUM(other_ad_type_pay_order_gmv) AS DECIMAL(38,5))/100000 AS other_ad_type_pay_order_gmv,
  CAST(SUM(search_pay_order_gmv) AS DECIMAL(38,5))/100000 AS search_pay_order_gmv,
  CAST(SUM(splash_pay_order_gmv) AS DECIMAL(38,5))/100000 AS splash_pay_order_gmv,
  CAST(SUM(feed_pay_order_gmv) AS DECIMAL(38,5))/100000 AS feed_pay_order_gmv,
  CAST(SUM(rewarded_pay_order_gmv) AS DECIMAL(38,5))/100000 AS rewarded_pay_order_gmv,
  CAST(SUM(banner_pay_order_gmv) AS DECIMAL(38,5))/100000 AS banner_pay_order_gmv,
  CAST(SUM(other_placement_pay_order_gmv) AS DECIMAL(38,5))/100000 AS other_placement_pay_order_gmv,
  CASE WHEN SUM(closed_cost)>0 THEN SUM(pay_order_gmv)*1.0/SUM(closed_cost) ELSE 0 END AS realtime_roas
FROM ad_ads.dws_ad_creative_10s
GROUP BY window_start,window_end,dt;

CREATE VIEW ad_ads.v_realtime_metric_latest AS
SELECT metric.*
FROM ad_ads.v_realtime_metric metric
JOIN (
  SELECT MAX(window_start) AS window_start
  FROM ad_ads.v_realtime_metric
) latest ON metric.window_start=latest.window_start;

CREATE VIEW ad_ads.v_offline_metric AS
SELECT dt,
  delivery_count,
  impression_count,
  click_count,
  conversion_count,
  CAST(cost AS DECIMAL(38,5))/100000 AS cost,
  CAST(closed_cost AS DECIMAL(38,5))/100000 AS closed_cost,
  pay_order_count,
  refund_order_count,
  CAST(pay_order_gmv AS DECIMAL(38,5))/100000 AS pay_order_gmv,
  CAST(refund_order_gmv AS DECIMAL(38,5))/100000 AS refund_order_gmv,
  CAST(short_video_pay_order_gmv AS DECIMAL(38,5))/100000 AS short_video_pay_order_gmv,
  CAST(live_pay_order_gmv AS DECIMAL(38,5))/100000 AS live_pay_order_gmv,
  CAST(image_text_pay_order_gmv AS DECIMAL(38,5))/100000 AS image_text_pay_order_gmv,
  CAST(search_pay_order_gmv AS DECIMAL(38,5))/100000 AS search_pay_order_gmv,
  CAST(splash_pay_order_gmv AS DECIMAL(38,5))/100000 AS splash_pay_order_gmv,
  CAST(feed_pay_order_gmv AS DECIMAL(38,5))/100000 AS feed_pay_order_gmv,
  CAST(rewarded_pay_order_gmv AS DECIMAL(38,5))/100000 AS rewarded_pay_order_gmv,
  CAST(banner_pay_order_gmv AS DECIMAL(38,5))/100000 AS banner_pay_order_gmv,
  CAST(other_placement_pay_order_gmv AS DECIMAL(38,5))/100000 AS other_placement_pay_order_gmv,
  CASE WHEN impression_count>0 THEN click_count*1.0/impression_count ELSE 0 END AS ctr,
  CASE WHEN click_count>0 THEN conversion_count*1.0/click_count ELSE 0 END AS cvr,
  CASE WHEN closed_cost>0 THEN pay_order_gmv*1.0/closed_cost ELSE 0 END AS roas
FROM paimon_catalog.ad_dw.ads_offline_metric_di;

CREATE VIEW ad_ads.v_order_attribution AS
SELECT dt,order_id,uid,product_id,pay_time,
  CAST(pay_order_gmv AS DECIMAL(38,5))/100000 AS pay_order_gmv,
  last_click_time,advertiser_id,campaign_id,unit_id,creative_id,
  placement_type,ad_type,attribute_period
FROM paimon_catalog.ad_dw.ads_order_attribution_di;
CREATE VIEW ad_ads.v_ads_advertiser_retention_di AS SELECT * FROM paimon_catalog.ad_dw.ads_advertiser_retention_di;
CREATE VIEW ad_ads.v_dm_ad_advertiser_df AS SELECT * FROM paimon_catalog.ad_dw.dm_ad_advertiser_df;
CREATE VIEW ad_ads.v_dm_ad_campaign_df AS SELECT * FROM paimon_catalog.ad_dw.dm_ad_campaign_df;
CREATE VIEW ad_ads.v_dm_ad_unit_df AS SELECT * FROM paimon_catalog.ad_dw.dm_ad_unit_df;
CREATE VIEW ad_ads.v_dm_ad_creative_df AS SELECT * FROM paimon_catalog.ad_dw.dm_ad_creative_df;
