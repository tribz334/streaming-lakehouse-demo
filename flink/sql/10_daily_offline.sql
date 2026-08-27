-- Build daily ADS directly from advertiser DWS, then refresh DM snapshots.
SET 'execution.runtime-mode'='batch';
SET 'table.dml-sync'='true';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'pipeline.name'='paimon-ads-daily-__BIZ_DATE__';
CREATE CATALOG paimon WITH ('type'='paimon','metastore'='filesystem','warehouse'='file:///warehouse/paimon');

INSERT OVERWRITE paimon.ad_dw.ads_offline_metric_di
PARTITION (dt='__BIZ_DATE__')
SELECT 'all' AS metric_key,
  COALESCE(SUM(delivery_count),CAST(0 AS BIGINT)),
  COALESCE(SUM(impression_count),CAST(0 AS BIGINT)),
  COALESCE(SUM(click_count),CAST(0 AS BIGINT)),
  COALESCE(SUM(conversion_count),CAST(0 AS BIGINT)),
  COALESCE(SUM(cost),CAST(0 AS BIGINT)),
  COALESCE(SUM(closed_cost),CAST(0 AS BIGINT)),
  COALESCE(SUM(pay_order_count),CAST(0 AS BIGINT)),
  COALESCE(SUM(refund_order_count),CAST(0 AS BIGINT)),
  COALESCE(SUM(pay_order_gmv),CAST(0 AS BIGINT)),
  COALESCE(SUM(refund_order_gmv),CAST(0 AS BIGINT)),
  COALESCE(SUM(short_video_pay_order_gmv),CAST(0 AS BIGINT)),
  COALESCE(SUM(live_pay_order_gmv),CAST(0 AS BIGINT)),
  COALESCE(SUM(image_text_pay_order_gmv),CAST(0 AS BIGINT)),
  COALESCE(SUM(search_pay_order_gmv),CAST(0 AS BIGINT)),
  COALESCE(SUM(splash_pay_order_gmv),CAST(0 AS BIGINT)),
  COALESCE(SUM(feed_pay_order_gmv),CAST(0 AS BIGINT)),
  COALESCE(SUM(rewarded_pay_order_gmv),CAST(0 AS BIGINT)),
  COALESCE(SUM(banner_pay_order_gmv),CAST(0 AS BIGINT)),
  COALESCE(SUM(other_placement_pay_order_gmv),CAST(0 AS BIGINT))
FROM paimon.ad_dw.dws_advertiser_di
WHERE dt='__BIZ_DATE__';

-- Reuse the final six-hour DataStream attribution result; do not recompute LastClick in SQL.
INSERT OVERWRITE paimon.ad_dw.ads_order_attribution_di
PARTITION (dt='__BIZ_DATE__')
SELECT order_id,uid,product_id,
  TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(pay_time)*1000,3),total_amount,click_time,
  advertiser_id,campaign_id,unit_id,creative_id,placement_type,ad_type,
  CASE WHEN is_direct_attribution THEN 'DIRECT' ELSE 'ORGANIC' END
FROM paimon.ad_dw.dwd_ad_order_acc
WHERE dt='__BIZ_DATE__' AND pay_time IS NOT NULL;

-- Cohort retention: an advertiser is active on a day when its daily cost is positive.
-- The source intentionally reads the latest DWS snapshot so an older cohort can be
-- recalculated after its 1/7/15/30-day observation windows have elapsed.
INSERT OVERWRITE paimon.ad_dw.ads_advertiser_retention_di
PARTITION (dt='__BIZ_DATE__')
SELECT
  COUNT(DISTINCT a.advertiser_id) AS advertiser_count,
  CASE WHEN COUNT(DISTINCT a.advertiser_id)=0 THEN CAST(0 AS DOUBLE)
    ELSE CAST(COUNT(DISTINCT CASE
      WHEN TIMESTAMPDIFF(DAY,TO_DATE(a.dt),TO_DATE(b.dt))=1 THEN a.advertiser_id END) AS DOUBLE)
      / CAST(COUNT(DISTINCT a.advertiser_id) AS DOUBLE) END AS retention_rate_1d,
  CASE WHEN COUNT(DISTINCT a.advertiser_id)=0 THEN CAST(0 AS DOUBLE)
    ELSE CAST(COUNT(DISTINCT CASE
      WHEN TIMESTAMPDIFF(DAY,TO_DATE(a.dt),TO_DATE(b.dt))=7 THEN a.advertiser_id END) AS DOUBLE)
      / CAST(COUNT(DISTINCT a.advertiser_id) AS DOUBLE) END AS retention_rate_7d,
  CASE WHEN COUNT(DISTINCT a.advertiser_id)=0 THEN CAST(0 AS DOUBLE)
    ELSE CAST(COUNT(DISTINCT CASE
      WHEN TIMESTAMPDIFF(DAY,TO_DATE(a.dt),TO_DATE(b.dt))=15 THEN a.advertiser_id END) AS DOUBLE)
      / CAST(COUNT(DISTINCT a.advertiser_id) AS DOUBLE) END AS retention_rate_15d,
  CASE WHEN COUNT(DISTINCT a.advertiser_id)=0 THEN CAST(0 AS DOUBLE)
    ELSE CAST(COUNT(DISTINCT CASE
      WHEN TIMESTAMPDIFF(DAY,TO_DATE(a.dt),TO_DATE(b.dt))=30 THEN a.advertiser_id END) AS DOUBLE)
      / CAST(COUNT(DISTINCT a.advertiser_id) AS DOUBLE) END AS retention_rate_30d
FROM paimon.ad_dw.dws_advertiser_di a
LEFT JOIN paimon.ad_dw.dws_advertiser_di b
  ON a.advertiser_id=b.advertiser_id
 AND b.cost>0
 AND TIMESTAMPDIFF(DAY,TO_DATE(a.dt),TO_DATE(b.dt)) IN (1,7,15,30)
WHERE a.dt='__BIZ_DATE__' AND a.cost>0;

INSERT OVERWRITE paimon.ad_dw.dm_advertiser_df
PARTITION (dt='__BIZ_DATE__')
SELECT dim.advertiser_id,
  dim.advertiser_name,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_lifetime
FROM paimon.ad_dw.dim_advertiser_df dim
LEFT JOIN paimon.ad_dw.dws_advertiser_di d ON dim.advertiser_id=d.advertiser_id AND d.dt<='__BIZ_DATE__'
GROUP BY dim.advertiser_id,dim.advertiser_name;

INSERT OVERWRITE paimon.ad_dw.dm_campaign_df
PARTITION (dt='__BIZ_DATE__')
SELECT dim.campaign_id,
  dim.campaign_name,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_lifetime
FROM paimon.ad_dw.dim_campaign_df dim
LEFT JOIN paimon.ad_dw.dws_campaign_di d ON dim.campaign_id=d.campaign_id AND d.dt<='__BIZ_DATE__'
GROUP BY dim.campaign_id,dim.campaign_name;

INSERT OVERWRITE paimon.ad_dw.dm_unit_df
PARTITION (dt='__BIZ_DATE__')
SELECT dim.unit_id,
  dim.unit_name,
  dim.placement_type,
  dim.ad_type,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_lifetime
FROM paimon.ad_dw.dim_unit_df dim
LEFT JOIN paimon.ad_dw.dws_unit_di d ON dim.unit_id=d.unit_id AND d.dt<='__BIZ_DATE__'
GROUP BY dim.unit_id,dim.unit_name,dim.placement_type,dim.ad_type;

INSERT OVERWRITE paimon.ad_dw.dm_creative_df
PARTITION (dt='__BIZ_DATE__')
SELECT dim.creative_id,
  dim.creative_name,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.delivery_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS delivery_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.impression_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS impression_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.click_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS click_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.conversion_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS conversion_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS cost_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.closed_cost ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS closed_cost_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.pay_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.refund_order_count ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_count_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.pay_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS pay_order_gmv_lifetime,
  COALESCE(SUM(CASE WHEN d.dt='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_1d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_6__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_7d,
  COALESCE(SUM(CASE WHEN d.dt>='__DATE_MINUS_29__' AND d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_30d,
  COALESCE(SUM(CASE WHEN d.dt<='__BIZ_DATE__' THEN d.refund_order_gmv ELSE CAST(0 AS BIGINT) END),CAST(0 AS BIGINT)) AS refund_order_gmv_lifetime
FROM paimon.ad_dw.dim_creative_df dim
LEFT JOIN paimon.ad_dw.dws_creative_di d ON dim.creative_id=d.creative_id AND d.dt<='__BIZ_DATE__'
GROUP BY dim.creative_id,dim.creative_name;
