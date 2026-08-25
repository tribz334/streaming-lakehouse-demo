-- Build daily ADS partitions from tiered Paimon data.
SET 'execution.runtime-mode'='batch';
SET 'table.dml-sync'='true';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'pipeline.name'='paimon-ads-daily-__BIZ_DATE__';
CREATE CATALOG paimon WITH ('type'='paimon','metastore'='filesystem','warehouse'='file:///warehouse/paimon');

-- Daily advertising operating dashboard: recover the hierarchy through DIM tables.
INSERT OVERWRITE paimon.ad_dw.ads_offline_metric_di
PARTITION (dt='__BIZ_DATE__')
SELECT cp.advertiser_id,u.campaign_id,c.unit_id,d.creative_id,
  d.delivery_count,d.impression_count,d.click_count,d.conversion_count,
  d.cost,d.closed_cost,d.pay_order_count,d.refund_order_count,
  d.pay_order_gmv,d.refund_order_gmv,
  d.ecommerce_pay_order_count,d.ecommerce_refund_order_count,
  d.ecommerce_pay_order_gmv,d.ecommerce_refund_order_gmv,
  d.short_video_pay_order_count,d.short_video_refund_order_count,
  d.short_video_pay_order_gmv,d.short_video_refund_order_gmv,
  d.live_pay_order_count,d.live_refund_order_count,
  d.live_pay_order_gmv,d.live_refund_order_gmv
FROM paimon.ad_dw.dws_creative_di d
LEFT JOIN paimon.ad_dw.dim_creative_df c ON d.creative_id=c.creative_id
LEFT JOIN paimon.ad_dw.dim_unit_df u ON c.unit_id=u.unit_id
LEFT JOIN paimon.ad_dw.dim_campaign_df cp ON u.campaign_id=cp.campaign_id
WHERE d.dt='__BIZ_DATE__';

-- Every PAY order independently selects its latest valid click from all retained history.
INSERT OVERWRITE paimon.ad_dw.ads_order_attribution_di
PARTITION (dt='__BIZ_DATE__')
SELECT order_id,uid,product_id,pay_time,pay_order_gmv,click_time,
  advertiser_id,campaign_id,unit_id,creative_id,
  CASE
    WHEN click_time>=pay_time-INTERVAL '6' HOUR THEN 'DIRECT'
    WHEN click_time>=pay_time-INTERVAL '7' DAY THEN '7D'
    WHEN click_time>=pay_time-INTERVAL '30' DAY THEN '30D'
    WHEN click_time IS NOT NULL THEN 'LONG_TERM'
    ELSE 'ORGANIC'
  END
FROM (
  SELECT o.order_id,o.uid,o.product_id,o.pay_time,o.pay_order_gmv,
    c.event_time AS click_time,c.advertiser_id,c.campaign_id,c.unit_id,c.creative_id,
    ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY c.event_time DESC) AS rn
  FROM (
    SELECT order_id,uid,product_id,pay_time,pay_order_gmv
    FROM paimon.ad_dw.dwd_ad_order_di
    WHERE dt='__BIZ_DATE__' AND order_type='PAY'
  ) o
  LEFT JOIN paimon.ad_dw.dwd_ad_event_di c
    ON o.uid=c.uid AND o.product_id=c.product_id AND c.event_type='click'
    AND c.event_time<o.pay_time
) ranked
WHERE rn=1;

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

-- DM rolling mode requires the previous day's DM partition. Use 11_initialize_dm.sql once
-- when no previous partition exists, then run this incremental recurrence every day.
-- Advertiser daily-full rolling snapshot.
INSERT OVERWRITE paimon.ad_dw.dm_advertiser_df
PARTITION (dt='__BIZ_DATE__')
SELECT dim.advertiser_id,dim.advertiser_name,
  COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_1d,
  COALESCE(p.delivery_count_7d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(day_7.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_7d,
  COALESCE(p.delivery_count_30d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(day_30.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_30d,
  COALESCE(p.delivery_count_acc,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_acc,
  COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_1d,
  COALESCE(p.impression_count_7d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(day_7.impression_count,CAST(0 AS BIGINT)) AS impression_count_7d,
  COALESCE(p.impression_count_30d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(day_30.impression_count,CAST(0 AS BIGINT)) AS impression_count_30d,
  COALESCE(p.impression_count_acc,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_acc,
  COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_1d,
  COALESCE(p.click_count_7d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(day_7.click_count,CAST(0 AS BIGINT)) AS click_count_7d,
  COALESCE(p.click_count_30d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(day_30.click_count,CAST(0 AS BIGINT)) AS click_count_30d,
  COALESCE(p.click_count_acc,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_acc,
  COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_1d,
  COALESCE(p.conversion_count_7d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(day_7.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_7d,
  COALESCE(p.conversion_count_30d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(day_30.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_30d,
  COALESCE(p.conversion_count_acc,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_acc,
  COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_1d,
  COALESCE(p.cost_7d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(day_7.cost,CAST(0 AS BIGINT)) AS cost_7d,
  COALESCE(p.cost_30d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(day_30.cost,CAST(0 AS BIGINT)) AS cost_30d,
  COALESCE(p.cost_acc,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_acc,
  COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_1d,
  COALESCE(p.closed_cost_7d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(day_7.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_7d,
  COALESCE(p.closed_cost_30d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(day_30.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_30d,
  COALESCE(p.closed_cost_acc,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_acc,
  COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_1d,
  COALESCE(p.pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_7d,
  COALESCE(p.pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_30d,
  COALESCE(p.pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_acc,
  COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_1d,
  COALESCE(p.refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_7d,
  COALESCE(p.refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_30d,
  COALESCE(p.refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_acc,
  COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_1d,
  COALESCE(p.pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_7d,
  COALESCE(p.pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_30d,
  COALESCE(p.pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_acc,
  COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_1d,
  COALESCE(p.refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_7d,
  COALESCE(p.refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_30d,
  COALESCE(p.refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_acc,
  COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_1d,
  COALESCE(p.ecommerce_pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_7d,
  COALESCE(p.ecommerce_pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_30d,
  COALESCE(p.ecommerce_pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_acc,
  COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_1d,
  COALESCE(p.ecommerce_refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_7d,
  COALESCE(p.ecommerce_refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_30d,
  COALESCE(p.ecommerce_refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_acc,
  COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_1d,
  COALESCE(p.ecommerce_pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_7d,
  COALESCE(p.ecommerce_pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_30d,
  COALESCE(p.ecommerce_pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_acc,
  COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_1d,
  COALESCE(p.ecommerce_refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_7d,
  COALESCE(p.ecommerce_refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_30d,
  COALESCE(p.ecommerce_refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_acc,
  COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_1d,
  COALESCE(p.short_video_pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_7d,
  COALESCE(p.short_video_pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_30d,
  COALESCE(p.short_video_pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_acc,
  COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_1d,
  COALESCE(p.short_video_refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_7d,
  COALESCE(p.short_video_refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_30d,
  COALESCE(p.short_video_refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_acc,
  COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_1d,
  COALESCE(p.short_video_pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_7d,
  COALESCE(p.short_video_pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_30d,
  COALESCE(p.short_video_pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_acc,
  COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_1d,
  COALESCE(p.short_video_refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_7d,
  COALESCE(p.short_video_refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_30d,
  COALESCE(p.short_video_refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_acc,
  COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_1d,
  COALESCE(p.live_pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_7d,
  COALESCE(p.live_pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_30d,
  COALESCE(p.live_pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_acc,
  COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_1d,
  COALESCE(p.live_refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_7d,
  COALESCE(p.live_refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_30d,
  COALESCE(p.live_refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_acc,
  COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_1d,
  COALESCE(p.live_pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_7d,
  COALESCE(p.live_pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_30d,
  COALESCE(p.live_pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_acc,
  COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_1d,
  COALESCE(p.live_refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_7d,
  COALESCE(p.live_refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_30d,
  COALESCE(p.live_refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_acc
FROM paimon.ad_dw.dim_advertiser_df dim
LEFT JOIN paimon.ad_dw.dm_advertiser_df p
  ON dim.advertiser_id=p.advertiser_id AND p.dt='__DATE_MINUS_1__'
LEFT JOIN paimon.ad_dw.dws_advertiser_di today
  ON dim.advertiser_id=today.advertiser_id AND today.dt='__BIZ_DATE__'
LEFT JOIN paimon.ad_dw.dws_advertiser_di day_7
  ON dim.advertiser_id=day_7.advertiser_id AND day_7.dt='__DATE_MINUS_7__'
LEFT JOIN paimon.ad_dw.dws_advertiser_di day_30
  ON dim.advertiser_id=day_30.advertiser_id AND day_30.dt='__DATE_MINUS_30__';

-- Unit daily-full rolling snapshot.
INSERT OVERWRITE paimon.ad_dw.dm_unit_df
PARTITION (dt='__BIZ_DATE__')
SELECT dim.unit_id,dim.unit_name,
  COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_1d,
  COALESCE(p.delivery_count_7d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(day_7.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_7d,
  COALESCE(p.delivery_count_30d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(day_30.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_30d,
  COALESCE(p.delivery_count_acc,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_acc,
  COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_1d,
  COALESCE(p.impression_count_7d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(day_7.impression_count,CAST(0 AS BIGINT)) AS impression_count_7d,
  COALESCE(p.impression_count_30d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(day_30.impression_count,CAST(0 AS BIGINT)) AS impression_count_30d,
  COALESCE(p.impression_count_acc,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_acc,
  COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_1d,
  COALESCE(p.click_count_7d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(day_7.click_count,CAST(0 AS BIGINT)) AS click_count_7d,
  COALESCE(p.click_count_30d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(day_30.click_count,CAST(0 AS BIGINT)) AS click_count_30d,
  COALESCE(p.click_count_acc,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_acc,
  COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_1d,
  COALESCE(p.conversion_count_7d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(day_7.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_7d,
  COALESCE(p.conversion_count_30d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(day_30.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_30d,
  COALESCE(p.conversion_count_acc,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_acc,
  COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_1d,
  COALESCE(p.cost_7d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(day_7.cost,CAST(0 AS BIGINT)) AS cost_7d,
  COALESCE(p.cost_30d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(day_30.cost,CAST(0 AS BIGINT)) AS cost_30d,
  COALESCE(p.cost_acc,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_acc,
  COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_1d,
  COALESCE(p.closed_cost_7d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(day_7.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_7d,
  COALESCE(p.closed_cost_30d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(day_30.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_30d,
  COALESCE(p.closed_cost_acc,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_acc,
  COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_1d,
  COALESCE(p.pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_7d,
  COALESCE(p.pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_30d,
  COALESCE(p.pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_acc,
  COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_1d,
  COALESCE(p.refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_7d,
  COALESCE(p.refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_30d,
  COALESCE(p.refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_acc,
  COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_1d,
  COALESCE(p.pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_7d,
  COALESCE(p.pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_30d,
  COALESCE(p.pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_acc,
  COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_1d,
  COALESCE(p.refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_7d,
  COALESCE(p.refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_30d,
  COALESCE(p.refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_acc,
  COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_1d,
  COALESCE(p.ecommerce_pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_7d,
  COALESCE(p.ecommerce_pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_30d,
  COALESCE(p.ecommerce_pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_acc,
  COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_1d,
  COALESCE(p.ecommerce_refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_7d,
  COALESCE(p.ecommerce_refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_30d,
  COALESCE(p.ecommerce_refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_acc,
  COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_1d,
  COALESCE(p.ecommerce_pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_7d,
  COALESCE(p.ecommerce_pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_30d,
  COALESCE(p.ecommerce_pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_acc,
  COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_1d,
  COALESCE(p.ecommerce_refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_7d,
  COALESCE(p.ecommerce_refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_30d,
  COALESCE(p.ecommerce_refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_acc,
  COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_1d,
  COALESCE(p.short_video_pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_7d,
  COALESCE(p.short_video_pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_30d,
  COALESCE(p.short_video_pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_acc,
  COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_1d,
  COALESCE(p.short_video_refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_7d,
  COALESCE(p.short_video_refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_30d,
  COALESCE(p.short_video_refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_acc,
  COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_1d,
  COALESCE(p.short_video_pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_7d,
  COALESCE(p.short_video_pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_30d,
  COALESCE(p.short_video_pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_acc,
  COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_1d,
  COALESCE(p.short_video_refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_7d,
  COALESCE(p.short_video_refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_30d,
  COALESCE(p.short_video_refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_acc,
  COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_1d,
  COALESCE(p.live_pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_7d,
  COALESCE(p.live_pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_30d,
  COALESCE(p.live_pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_acc,
  COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_1d,
  COALESCE(p.live_refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_7d,
  COALESCE(p.live_refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_30d,
  COALESCE(p.live_refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_acc,
  COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_1d,
  COALESCE(p.live_pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_7d,
  COALESCE(p.live_pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_30d,
  COALESCE(p.live_pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_acc,
  COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_1d,
  COALESCE(p.live_refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_7d,
  COALESCE(p.live_refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_30d,
  COALESCE(p.live_refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_acc
FROM paimon.ad_dw.dim_unit_df dim
LEFT JOIN paimon.ad_dw.dm_unit_df p
  ON dim.unit_id=p.unit_id AND p.dt='__DATE_MINUS_1__'
LEFT JOIN paimon.ad_dw.dws_unit_di today
  ON dim.unit_id=today.unit_id AND today.dt='__BIZ_DATE__'
LEFT JOIN paimon.ad_dw.dws_unit_di day_7
  ON dim.unit_id=day_7.unit_id AND day_7.dt='__DATE_MINUS_7__'
LEFT JOIN paimon.ad_dw.dws_unit_di day_30
  ON dim.unit_id=day_30.unit_id AND day_30.dt='__DATE_MINUS_30__';

-- Creative daily-full rolling snapshot.
INSERT OVERWRITE paimon.ad_dw.dm_creative_df
PARTITION (dt='__BIZ_DATE__')
SELECT dim.creative_id,dim.creative_name,
  COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_1d,
  COALESCE(p.delivery_count_7d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(day_7.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_7d,
  COALESCE(p.delivery_count_30d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(day_30.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_30d,
  COALESCE(p.delivery_count_acc,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_acc,
  COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_1d,
  COALESCE(p.impression_count_7d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(day_7.impression_count,CAST(0 AS BIGINT)) AS impression_count_7d,
  COALESCE(p.impression_count_30d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(day_30.impression_count,CAST(0 AS BIGINT)) AS impression_count_30d,
  COALESCE(p.impression_count_acc,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_acc,
  COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_1d,
  COALESCE(p.click_count_7d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(day_7.click_count,CAST(0 AS BIGINT)) AS click_count_7d,
  COALESCE(p.click_count_30d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(day_30.click_count,CAST(0 AS BIGINT)) AS click_count_30d,
  COALESCE(p.click_count_acc,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_acc,
  COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_1d,
  COALESCE(p.conversion_count_7d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(day_7.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_7d,
  COALESCE(p.conversion_count_30d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(day_30.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_30d,
  COALESCE(p.conversion_count_acc,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_acc,
  COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_1d,
  COALESCE(p.cost_7d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(day_7.cost,CAST(0 AS BIGINT)) AS cost_7d,
  COALESCE(p.cost_30d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(day_30.cost,CAST(0 AS BIGINT)) AS cost_30d,
  COALESCE(p.cost_acc,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_acc,
  COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_1d,
  COALESCE(p.closed_cost_7d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(day_7.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_7d,
  COALESCE(p.closed_cost_30d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(day_30.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_30d,
  COALESCE(p.closed_cost_acc,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_acc,
  COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_1d,
  COALESCE(p.pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_7d,
  COALESCE(p.pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_30d,
  COALESCE(p.pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_acc,
  COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_1d,
  COALESCE(p.refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_7d,
  COALESCE(p.refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_30d,
  COALESCE(p.refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_acc,
  COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_1d,
  COALESCE(p.pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_7d,
  COALESCE(p.pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_30d,
  COALESCE(p.pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_acc,
  COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_1d,
  COALESCE(p.refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_7d,
  COALESCE(p.refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_30d,
  COALESCE(p.refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_acc,
  COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_1d,
  COALESCE(p.ecommerce_pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_7d,
  COALESCE(p.ecommerce_pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_30d,
  COALESCE(p.ecommerce_pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_count,CAST(0 AS BIGINT)) AS ecommerce_pay_order_count_acc,
  COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_1d,
  COALESCE(p.ecommerce_refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_7d,
  COALESCE(p.ecommerce_refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_30d,
  COALESCE(p.ecommerce_refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_count,CAST(0 AS BIGINT)) AS ecommerce_refund_order_count_acc,
  COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_1d,
  COALESCE(p.ecommerce_pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_7d,
  COALESCE(p.ecommerce_pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_30d,
  COALESCE(p.ecommerce_pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_pay_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_pay_order_gmv_acc,
  COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_1d,
  COALESCE(p.ecommerce_refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_7d,
  COALESCE(p.ecommerce_refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_30d,
  COALESCE(p.ecommerce_refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.ecommerce_refund_order_gmv,CAST(0 AS BIGINT)) AS ecommerce_refund_order_gmv_acc,
  COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_1d,
  COALESCE(p.short_video_pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_7d,
  COALESCE(p.short_video_pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_30d,
  COALESCE(p.short_video_pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_count,CAST(0 AS BIGINT)) AS short_video_pay_order_count_acc,
  COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_1d,
  COALESCE(p.short_video_refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_7d,
  COALESCE(p.short_video_refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_30d,
  COALESCE(p.short_video_refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_count,CAST(0 AS BIGINT)) AS short_video_refund_order_count_acc,
  COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_1d,
  COALESCE(p.short_video_pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_7d,
  COALESCE(p.short_video_pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_30d,
  COALESCE(p.short_video_pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_pay_order_gmv,CAST(0 AS BIGINT)) AS short_video_pay_order_gmv_acc,
  COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_1d,
  COALESCE(p.short_video_refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_7d,
  COALESCE(p.short_video_refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_30d,
  COALESCE(p.short_video_refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.short_video_refund_order_gmv,CAST(0 AS BIGINT)) AS short_video_refund_order_gmv_acc,
  COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_1d,
  COALESCE(p.live_pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_7d,
  COALESCE(p.live_pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_30d,
  COALESCE(p.live_pay_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_count,CAST(0 AS BIGINT)) AS live_pay_order_count_acc,
  COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_1d,
  COALESCE(p.live_refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_7.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_7d,
  COALESCE(p.live_refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT))-COALESCE(day_30.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_30d,
  COALESCE(p.live_refund_order_count_acc,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_count,CAST(0 AS BIGINT)) AS live_refund_order_count_acc,
  COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_1d,
  COALESCE(p.live_pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_7d,
  COALESCE(p.live_pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_30d,
  COALESCE(p.live_pay_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.live_pay_order_gmv,CAST(0 AS BIGINT)) AS live_pay_order_gmv_acc,
  COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_1d,
  COALESCE(p.live_refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_7.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_7d,
  COALESCE(p.live_refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(day_30.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_30d,
  COALESCE(p.live_refund_order_gmv_acc,CAST(0 AS BIGINT))+COALESCE(today.live_refund_order_gmv,CAST(0 AS BIGINT)) AS live_refund_order_gmv_acc
FROM paimon.ad_dw.dim_creative_df dim
LEFT JOIN paimon.ad_dw.dm_creative_df p
  ON dim.creative_id=p.creative_id AND p.dt='__DATE_MINUS_1__'
LEFT JOIN paimon.ad_dw.dws_creative_di today
  ON dim.creative_id=today.creative_id AND today.dt='__BIZ_DATE__'
LEFT JOIN paimon.ad_dw.dws_creative_di day_7
  ON dim.creative_id=day_7.creative_id AND day_7.dt='__DATE_MINUS_7__'
LEFT JOIN paimon.ad_dw.dws_creative_di day_30
  ON dim.creative_id=day_30.creative_id AND day_30.dt='__DATE_MINUS_30__';
