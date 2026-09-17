-- Build daily ADS, then roll 1d/7d/30d/lifetime DM from the previous DM partition.
-- Prerequisite: initialize __PREV_DATE__ with 11_initialize_dm.sql before the first daily roll.
SET 'execution.runtime-mode'='batch';
SET 'table.dml-sync'='true';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'pipeline.name'='paimon-ads-dm-daily-__BIZ_DATE__';
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
FROM paimon.ad_dw.dws_ad_advertiser_di
WHERE dt='__BIZ_DATE__';

-- Rebuild the click details from the schema-converged DWD facts. The cumulative
-- order keeps only creative_id; names and hierarchy remain DIM attributes.
INSERT OVERWRITE paimon.ad_dw.ads_order_attribution_di
PARTITION (dt='__BIZ_DATE__')
SELECT order_id,uid,product_id,pay_ts,total_amount,
  candidate_click_time,
  COALESCE(direct_advertiser_id,candidate_advertiser_id),
  COALESCE(direct_campaign_id,candidate_campaign_id),
  COALESCE(direct_unit_id,candidate_unit_id),
  COALESCE(direct_creative_id,candidate_creative_id),
  COALESCE(direct_placement_type,candidate_placement_type),
  COALESCE(direct_ad_type,candidate_ad_type),
  CASE
    WHEN direct_creative_id IS NOT NULL THEN 'DIRECT'
    WHEN candidate_click_time >= pay_ts-INTERVAL '1' DAY THEN '1D'
    WHEN candidate_click_time >= pay_ts-INTERVAL '7' DAY THEN '7D'
    WHEN candidate_click_time >= pay_ts-INTERVAL '30' DAY THEN '30D'
    ELSE 'ORGANIC'
  END
FROM (
  SELECT o.order_id,o.uid,o.product_id,
    TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(o.pay_time)*1000,3) AS pay_ts,
    o.total_amount,
    o.advertiser_id AS direct_advertiser_id,
    o.campaign_id AS direct_campaign_id,
    o.unit_id AS direct_unit_id,
    o.creative_id AS direct_creative_id,
    o.placement_type AS direct_placement_type,
    o.ad_type AS direct_ad_type,
    TO_TIMESTAMP_LTZ(c.ts,3) AS candidate_click_time,
    ccp.advertiser_id AS candidate_advertiser_id,
    cu.campaign_id AS candidate_campaign_id,
    cc.unit_id AS candidate_unit_id,
    c.creative_id AS candidate_creative_id,
    cu.placement_type AS candidate_placement_type,
    cu.ad_type AS candidate_ad_type,
    ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY c.ts DESC,c.event_id DESC) AS rn
  FROM paimon.ad_dw.dwd_order_acc o
  LEFT JOIN paimon.ad_dw.dwd_ad_event_di c
    ON o.uid=c.uid
   AND o.product_id=c.product_id
   AND c.event_type='click'
   AND TO_TIMESTAMP_LTZ(c.ts,3)<=TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(o.pay_time)*1000,3)
   AND TO_TIMESTAMP_LTZ(c.ts,3)>=TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(o.pay_time)*1000,3)-INTERVAL '30' DAY
  LEFT JOIN paimon.ad_dw.dim_creative_df cc ON c.creative_id=cc.creative_id
  LEFT JOIN paimon.ad_dw.dim_unit_df cu ON cc.unit_id=cu.unit_id
  LEFT JOIN paimon.ad_dw.dim_campaign_df ccp ON cu.campaign_id=ccp.campaign_id
  WHERE REPLACE(SUBSTRING(o.pay_time,1,10),'-','')='__BIZ_DATE__'
) ranked_order_clicks
WHERE rn=1;

-- Cohort retention: an advertiser is active on a day when its daily cost is positive.
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
FROM paimon.ad_dw.dws_ad_advertiser_di a
LEFT JOIN paimon.ad_dw.dws_ad_advertiser_di b
  ON a.advertiser_id=b.advertiser_id
 AND b.cost>0
 AND TIMESTAMPDIFF(DAY,TO_DATE(a.dt),TO_DATE(b.dt)) IN (1,7,15,30)
WHERE a.dt='__BIZ_DATE__' AND a.cost>0;

-- Daily rolling DM: previous snapshot + today's DWS - the partition leaving each window.

INSERT OVERWRITE paimon.ad_dw.dm_ad_advertiser_df
PARTITION (dt='__BIZ_DATE__')
SELECT
  dim.advertiser_id,
  dim.advertiser_name,
  COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_1d,
  COALESCE(prev.delivery_count_7d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(expired7.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_7d,
  COALESCE(prev.delivery_count_30d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(expired30.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_30d,
  COALESCE(prev.delivery_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_lifetime,
  COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_1d,
  COALESCE(prev.impression_count_7d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(expired7.impression_count,CAST(0 AS BIGINT)) AS impression_count_7d,
  COALESCE(prev.impression_count_30d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(expired30.impression_count,CAST(0 AS BIGINT)) AS impression_count_30d,
  COALESCE(prev.impression_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_lifetime,
  COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_1d,
  COALESCE(prev.click_count_7d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(expired7.click_count,CAST(0 AS BIGINT)) AS click_count_7d,
  COALESCE(prev.click_count_30d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(expired30.click_count,CAST(0 AS BIGINT)) AS click_count_30d,
  COALESCE(prev.click_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_lifetime,
  COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_1d,
  COALESCE(prev.conversion_count_7d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(expired7.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_7d,
  COALESCE(prev.conversion_count_30d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(expired30.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_30d,
  COALESCE(prev.conversion_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_lifetime,
  COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_1d,
  COALESCE(prev.cost_7d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(expired7.cost,CAST(0 AS BIGINT)) AS cost_7d,
  COALESCE(prev.cost_30d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(expired30.cost,CAST(0 AS BIGINT)) AS cost_30d,
  COALESCE(prev.cost_lifetime,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_lifetime,
  COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_1d,
  COALESCE(prev.closed_cost_7d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(expired7.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_7d,
  COALESCE(prev.closed_cost_30d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(expired30.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_30d,
  COALESCE(prev.closed_cost_lifetime,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_lifetime,
  COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_1d,
  COALESCE(prev.pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(expired7.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_7d,
  COALESCE(prev.pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(expired30.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_30d,
  COALESCE(prev.pay_order_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_lifetime,
  COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_1d,
  COALESCE(prev.refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(expired7.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_7d,
  COALESCE(prev.refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(expired30.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_30d,
  COALESCE(prev.refund_order_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_lifetime,
  COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_1d,
  COALESCE(prev.pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired7.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_7d,
  COALESCE(prev.pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired30.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_30d,
  COALESCE(prev.pay_order_gmv_lifetime,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_lifetime,
  COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_1d,
  COALESCE(prev.refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired7.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_7d,
  COALESCE(prev.refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired30.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_30d,
  COALESCE(prev.refund_order_gmv_lifetime,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_lifetime
FROM paimon.ad_dw.dim_advertiser_df dim
LEFT JOIN paimon.ad_dw.dm_ad_advertiser_df prev
  ON dim.advertiser_id=prev.advertiser_id AND prev.dt='__PREV_DATE__'
LEFT JOIN paimon.ad_dw.dws_ad_advertiser_di today
  ON dim.advertiser_id=today.advertiser_id AND today.dt='__BIZ_DATE__'
LEFT JOIN paimon.ad_dw.dws_ad_advertiser_di expired7
  ON dim.advertiser_id=expired7.advertiser_id AND expired7.dt='__DATE_MINUS_7__'
LEFT JOIN paimon.ad_dw.dws_ad_advertiser_di expired30
  ON dim.advertiser_id=expired30.advertiser_id AND expired30.dt='__DATE_MINUS_30__';

INSERT OVERWRITE paimon.ad_dw.dm_ad_campaign_df
PARTITION (dt='__BIZ_DATE__')
SELECT
  dim.campaign_id,
  dim.campaign_name,
  COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_1d,
  COALESCE(prev.delivery_count_7d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(expired7.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_7d,
  COALESCE(prev.delivery_count_30d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(expired30.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_30d,
  COALESCE(prev.delivery_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_lifetime,
  COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_1d,
  COALESCE(prev.impression_count_7d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(expired7.impression_count,CAST(0 AS BIGINT)) AS impression_count_7d,
  COALESCE(prev.impression_count_30d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(expired30.impression_count,CAST(0 AS BIGINT)) AS impression_count_30d,
  COALESCE(prev.impression_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_lifetime,
  COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_1d,
  COALESCE(prev.click_count_7d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(expired7.click_count,CAST(0 AS BIGINT)) AS click_count_7d,
  COALESCE(prev.click_count_30d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(expired30.click_count,CAST(0 AS BIGINT)) AS click_count_30d,
  COALESCE(prev.click_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_lifetime,
  COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_1d,
  COALESCE(prev.conversion_count_7d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(expired7.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_7d,
  COALESCE(prev.conversion_count_30d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(expired30.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_30d,
  COALESCE(prev.conversion_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_lifetime,
  COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_1d,
  COALESCE(prev.cost_7d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(expired7.cost,CAST(0 AS BIGINT)) AS cost_7d,
  COALESCE(prev.cost_30d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(expired30.cost,CAST(0 AS BIGINT)) AS cost_30d,
  COALESCE(prev.cost_lifetime,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_lifetime,
  COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_1d,
  COALESCE(prev.closed_cost_7d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(expired7.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_7d,
  COALESCE(prev.closed_cost_30d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(expired30.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_30d,
  COALESCE(prev.closed_cost_lifetime,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_lifetime,
  COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_1d,
  COALESCE(prev.pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(expired7.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_7d,
  COALESCE(prev.pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(expired30.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_30d,
  COALESCE(prev.pay_order_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_lifetime,
  COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_1d,
  COALESCE(prev.refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(expired7.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_7d,
  COALESCE(prev.refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(expired30.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_30d,
  COALESCE(prev.refund_order_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_lifetime,
  COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_1d,
  COALESCE(prev.pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired7.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_7d,
  COALESCE(prev.pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired30.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_30d,
  COALESCE(prev.pay_order_gmv_lifetime,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_lifetime,
  COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_1d,
  COALESCE(prev.refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired7.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_7d,
  COALESCE(prev.refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired30.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_30d,
  COALESCE(prev.refund_order_gmv_lifetime,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_lifetime
FROM paimon.ad_dw.dim_campaign_df dim
LEFT JOIN paimon.ad_dw.dm_ad_campaign_df prev
  ON dim.campaign_id=prev.campaign_id AND prev.dt='__PREV_DATE__'
LEFT JOIN paimon.ad_dw.dws_ad_campaign_di today
  ON dim.campaign_id=today.campaign_id AND today.dt='__BIZ_DATE__'
LEFT JOIN paimon.ad_dw.dws_ad_campaign_di expired7
  ON dim.campaign_id=expired7.campaign_id AND expired7.dt='__DATE_MINUS_7__'
LEFT JOIN paimon.ad_dw.dws_ad_campaign_di expired30
  ON dim.campaign_id=expired30.campaign_id AND expired30.dt='__DATE_MINUS_30__';

INSERT OVERWRITE paimon.ad_dw.dm_ad_unit_df
PARTITION (dt='__BIZ_DATE__')
SELECT
  dim.unit_id,
  dim.unit_name,
  dim.placement_type,
  dim.ad_type,
  COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_1d,
  COALESCE(prev.delivery_count_7d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(expired7.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_7d,
  COALESCE(prev.delivery_count_30d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(expired30.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_30d,
  COALESCE(prev.delivery_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_lifetime,
  COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_1d,
  COALESCE(prev.impression_count_7d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(expired7.impression_count,CAST(0 AS BIGINT)) AS impression_count_7d,
  COALESCE(prev.impression_count_30d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(expired30.impression_count,CAST(0 AS BIGINT)) AS impression_count_30d,
  COALESCE(prev.impression_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_lifetime,
  COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_1d,
  COALESCE(prev.click_count_7d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(expired7.click_count,CAST(0 AS BIGINT)) AS click_count_7d,
  COALESCE(prev.click_count_30d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(expired30.click_count,CAST(0 AS BIGINT)) AS click_count_30d,
  COALESCE(prev.click_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_lifetime,
  COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_1d,
  COALESCE(prev.conversion_count_7d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(expired7.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_7d,
  COALESCE(prev.conversion_count_30d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(expired30.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_30d,
  COALESCE(prev.conversion_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_lifetime,
  COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_1d,
  COALESCE(prev.cost_7d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(expired7.cost,CAST(0 AS BIGINT)) AS cost_7d,
  COALESCE(prev.cost_30d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(expired30.cost,CAST(0 AS BIGINT)) AS cost_30d,
  COALESCE(prev.cost_lifetime,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_lifetime,
  COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_1d,
  COALESCE(prev.closed_cost_7d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(expired7.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_7d,
  COALESCE(prev.closed_cost_30d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(expired30.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_30d,
  COALESCE(prev.closed_cost_lifetime,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_lifetime,
  COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_1d,
  COALESCE(prev.pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(expired7.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_7d,
  COALESCE(prev.pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(expired30.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_30d,
  COALESCE(prev.pay_order_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_lifetime,
  COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_1d,
  COALESCE(prev.refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(expired7.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_7d,
  COALESCE(prev.refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(expired30.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_30d,
  COALESCE(prev.refund_order_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_lifetime,
  COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_1d,
  COALESCE(prev.pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired7.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_7d,
  COALESCE(prev.pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired30.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_30d,
  COALESCE(prev.pay_order_gmv_lifetime,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_lifetime,
  COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_1d,
  COALESCE(prev.refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired7.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_7d,
  COALESCE(prev.refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired30.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_30d,
  COALESCE(prev.refund_order_gmv_lifetime,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_lifetime
FROM paimon.ad_dw.dim_unit_df dim
LEFT JOIN paimon.ad_dw.dm_ad_unit_df prev
  ON dim.unit_id=prev.unit_id AND prev.dt='__PREV_DATE__'
LEFT JOIN paimon.ad_dw.dws_ad_unit_di today
  ON dim.unit_id=today.unit_id AND today.dt='__BIZ_DATE__'
LEFT JOIN paimon.ad_dw.dws_ad_unit_di expired7
  ON dim.unit_id=expired7.unit_id AND expired7.dt='__DATE_MINUS_7__'
LEFT JOIN paimon.ad_dw.dws_ad_unit_di expired30
  ON dim.unit_id=expired30.unit_id AND expired30.dt='__DATE_MINUS_30__';

INSERT OVERWRITE paimon.ad_dw.dm_ad_creative_df
PARTITION (dt='__BIZ_DATE__')
SELECT
  dim.creative_id,
  dim.creative_name,
  COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_1d,
  COALESCE(prev.delivery_count_7d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(expired7.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_7d,
  COALESCE(prev.delivery_count_30d,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT))-COALESCE(expired30.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_30d,
  COALESCE(prev.delivery_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.delivery_count,CAST(0 AS BIGINT)) AS delivery_count_lifetime,
  COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_1d,
  COALESCE(prev.impression_count_7d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(expired7.impression_count,CAST(0 AS BIGINT)) AS impression_count_7d,
  COALESCE(prev.impression_count_30d,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT))-COALESCE(expired30.impression_count,CAST(0 AS BIGINT)) AS impression_count_30d,
  COALESCE(prev.impression_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.impression_count,CAST(0 AS BIGINT)) AS impression_count_lifetime,
  COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_1d,
  COALESCE(prev.click_count_7d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(expired7.click_count,CAST(0 AS BIGINT)) AS click_count_7d,
  COALESCE(prev.click_count_30d,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT))-COALESCE(expired30.click_count,CAST(0 AS BIGINT)) AS click_count_30d,
  COALESCE(prev.click_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.click_count,CAST(0 AS BIGINT)) AS click_count_lifetime,
  COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_1d,
  COALESCE(prev.conversion_count_7d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(expired7.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_7d,
  COALESCE(prev.conversion_count_30d,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT))-COALESCE(expired30.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_30d,
  COALESCE(prev.conversion_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.conversion_count,CAST(0 AS BIGINT)) AS conversion_count_lifetime,
  COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_1d,
  COALESCE(prev.cost_7d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(expired7.cost,CAST(0 AS BIGINT)) AS cost_7d,
  COALESCE(prev.cost_30d,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT))-COALESCE(expired30.cost,CAST(0 AS BIGINT)) AS cost_30d,
  COALESCE(prev.cost_lifetime,CAST(0 AS BIGINT))+COALESCE(today.cost,CAST(0 AS BIGINT)) AS cost_lifetime,
  COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_1d,
  COALESCE(prev.closed_cost_7d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(expired7.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_7d,
  COALESCE(prev.closed_cost_30d,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT))-COALESCE(expired30.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_30d,
  COALESCE(prev.closed_cost_lifetime,CAST(0 AS BIGINT))+COALESCE(today.closed_cost,CAST(0 AS BIGINT)) AS closed_cost_lifetime,
  COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_1d,
  COALESCE(prev.pay_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(expired7.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_7d,
  COALESCE(prev.pay_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT))-COALESCE(expired30.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_30d,
  COALESCE(prev.pay_order_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.pay_order_count,CAST(0 AS BIGINT)) AS pay_order_count_lifetime,
  COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_1d,
  COALESCE(prev.refund_order_count_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(expired7.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_7d,
  COALESCE(prev.refund_order_count_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT))-COALESCE(expired30.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_30d,
  COALESCE(prev.refund_order_count_lifetime,CAST(0 AS BIGINT))+COALESCE(today.refund_order_count,CAST(0 AS BIGINT)) AS refund_order_count_lifetime,
  COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_1d,
  COALESCE(prev.pay_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired7.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_7d,
  COALESCE(prev.pay_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired30.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_30d,
  COALESCE(prev.pay_order_gmv_lifetime,CAST(0 AS BIGINT))+COALESCE(today.pay_order_gmv,CAST(0 AS BIGINT)) AS pay_order_gmv_lifetime,
  COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_1d,
  COALESCE(prev.refund_order_gmv_7d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired7.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_7d,
  COALESCE(prev.refund_order_gmv_30d,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT))-COALESCE(expired30.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_30d,
  COALESCE(prev.refund_order_gmv_lifetime,CAST(0 AS BIGINT))+COALESCE(today.refund_order_gmv,CAST(0 AS BIGINT)) AS refund_order_gmv_lifetime
FROM paimon.ad_dw.dim_creative_df dim
LEFT JOIN paimon.ad_dw.dm_ad_creative_df prev
  ON dim.creative_id=prev.creative_id AND prev.dt='__PREV_DATE__'
LEFT JOIN paimon.ad_dw.dws_ad_creative_di today
  ON dim.creative_id=today.creative_id AND today.dt='__BIZ_DATE__'
LEFT JOIN paimon.ad_dw.dws_ad_creative_di expired7
  ON dim.creative_id=expired7.creative_id AND expired7.dt='__DATE_MINUS_7__'
LEFT JOIN paimon.ad_dw.dws_ad_creative_di expired30
  ON dim.creative_id=expired30.creative_id AND expired30.dt='__DATE_MINUS_30__';
