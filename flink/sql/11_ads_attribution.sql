SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.ads_order_attribution_detail_di;
TRUNCATE TABLE paimon.ad_dw.ads_attribution_summary_di;

INSERT INTO paimon.ad_dw.ads_order_attribution_detail_di
SELECT
  c.stat_date,c.outcome_event_id,c.order_id,c.outcome_time,c.user_id,
  c.order_advertiser_id,c.order_advertiser_name,c.order_campaign_id,c.order_campaign_name,
  c.order_gmv,c.touch_event_id,c.touch_time,c.creative_id,c.campaign_id,c.campaign_name,
  c.advertiser_id,c.advertiser_name,c.touch_spend,m.attribution_model,
  CASE
    WHEN c.touch_event_id IS NULL THEN 'natural'
    WHEN c.lag_minutes <= 30 THEN 'direct'
    ELSE 'indirect'
  END,
  CASE
    WHEN c.touch_event_id IS NULL THEN '自然订单'
    WHEN c.lag_minutes <= 30 THEN '30分钟直接归因'
    WHEN c.lag_minutes <= 1440 THEN '1日间接归因'
    WHEN c.lag_minutes <= 4320 THEN '3日间接归因'
    WHEN c.lag_minutes <= 10080 THEN '7日间接归因'
    ELSE '30日间接归因'
  END,
  CASE
    WHEN c.touch_event_id IS NULL THEN 6
    WHEN c.lag_minutes <= 30 THEN 1
    WHEN c.lag_minutes <= 1440 THEN 2
    WHEN c.lag_minutes <= 4320 THEN 3
    WHEN c.lag_minutes <= 10080 THEN 4
    ELSE 5
  END,
  c.lag_minutes,c.touch_event_id IS NOT NULL,
  CURRENT_TIMESTAMP
FROM paimon.ad_dw.dm_attribution_touchpoint_df m
JOIN paimon.ad_dw.dws_attribution_candidate_df c
  ON m.stat_date=c.stat_date AND m.attribution_id=c.candidate_id
WHERE m.is_last_click OR m.touchpoint_type='organic';

INSERT INTO paimon.ad_dw.ads_attribution_summary_di
SELECT
  event_date,
  order_advertiser_id,
  MAX(order_advertiser_name),
  COALESCE(campaign_id, '__organic__'),
  MAX(COALESCE(campaign_name, '自然订单')),
  attribution_period,
  CAST(0 AS BIGINT),
  COUNT(*),
  CAST(SUM(order_gmv) AS DECIMAL(18,2)),
  CAST(SUM(COALESCE(touch_spend, CAST(0 AS DECIMAL(18,4)))) AS DECIMAL(18,4)),
  CURRENT_TIMESTAMP
FROM paimon.ad_dw.ads_order_attribution_detail_di
GROUP BY event_date, order_advertiser_id, COALESCE(campaign_id, '__organic__'), attribution_period;
