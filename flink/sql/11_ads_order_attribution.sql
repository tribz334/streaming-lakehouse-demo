-- Order-grain offline last-click attribution.
-- Contract: same user + same product, click strictly before order creation,
-- maximum 30-day lookback, and only the latest eligible click receives credit.
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'table.local-time-zone' = 'Asia/Shanghai';

TRUNCATE TABLE paimon.ad_dw.ads_order_attribution_detail_di;
TRUNCATE TABLE paimon.ad_dw.ads_attribution_summary_di;

CREATE TEMPORARY VIEW order_base AS
SELECT
  order_id,
  uid AS user_id,
  product_id,
  total_amount,
  TO_TIMESTAMP(create_time) AS order_ts,
  SUBSTRING(create_time, 1, 10) AS event_date
FROM paimon.ad_dw.dwd_order_detail_acc
WHERE create_time IS NOT NULL
  AND product_id IS NOT NULL;

CREATE TEMPORARY VIEW click_base AS
SELECT
  a.uid AS user_id,
  u.product_id,
  a.creative_id,
  a.ts AS click_event_id,
  -- Normalize epoch-millisecond event time to TIMESTAMP so it can be safely
  -- compared with the order-side create_time string parsed by TO_TIMESTAMP.
  TO_TIMESTAMP(
    DATE_FORMAT(TO_TIMESTAMP_LTZ(a.ts, 3), 'yyyy-MM-dd HH:mm:ss.SSS')
  ) AS click_ts,
  u.campaign_id,
  c.campaign_name,
  c.advertiser_id,
  adv.advertiser_name
FROM paimon.ad_dw.dwd_ad_action_log_inc AS a
JOIN paimon.ad_dw.dim_creative AS cr
  ON a.creative_id = cr.creative_id
JOIN paimon.ad_dw.dim_unit AS u
  ON cr.unit_id = u.unit_id
JOIN paimon.ad_dw.dim_campaign AS c
  ON u.campaign_id = c.campaign_id
LEFT JOIN paimon.ad_dw.dim_advertiser_zip AS adv
  ON c.advertiser_id = adv.advertiser_id
WHERE a.action_type = 'click'
  AND a.uid IS NOT NULL
  AND u.product_id IS NOT NULL;

CREATE TEMPORARY VIEW ranked_touch AS
SELECT *
FROM (
  SELECT
    o.order_id,
    o.user_id,
    o.product_id,
    o.total_amount,
    o.order_ts,
    o.event_date,
    c.click_event_id,
    c.click_ts,
    c.creative_id,
    c.campaign_id,
    c.campaign_name,
    c.advertiser_id,
    c.advertiser_name,
    TIMESTAMPDIFF(MINUTE, c.click_ts, o.order_ts) AS lag_minutes,
    ROW_NUMBER() OVER (
      PARTITION BY o.order_id
      ORDER BY c.click_ts DESC, c.creative_id DESC
    ) AS rn
  FROM order_base AS o
  LEFT JOIN click_base AS c
    ON o.user_id = c.user_id
   AND o.product_id = c.product_id
   AND c.click_ts < o.order_ts
   AND c.click_ts >= o.order_ts - INTERVAL '30' DAY
)
WHERE rn = 1;

INSERT INTO paimon.ad_dw.ads_order_attribution_detail_di
SELECT
  event_date,
  order_id AS order_event_id,
  order_id,
  order_ts,
  user_id,
  COALESCE(advertiser_id, CAST(-1 AS BIGINT)) AS order_advertiser_id,
  COALESCE(advertiser_name, '自然订单') AS order_advertiser_name,
  COALESCE(campaign_id, CAST(-1 AS BIGINT)) AS order_campaign_id,
  COALESCE(campaign_name, '自然订单') AS order_campaign_name,
  CAST(total_amount / 100000.0 AS DECIMAL(18,2)) AS order_gmv,
  click_event_id,
  click_ts,
  creative_id,
  campaign_id,
  campaign_name,
  advertiser_id,
  advertiser_name,
  CAST(0 AS DECIMAL(18,4)) AS touch_spend,
  'last_click_30d' AS attribution_model,
  CASE
    WHEN click_event_id IS NULL THEN 'organic'
    WHEN lag_minutes <= 30 THEN 'direct'
    ELSE 'indirect'
  END AS attribution_type,
  CASE
    WHEN click_event_id IS NULL THEN '自然订单'
    WHEN lag_minutes <= 30 THEN '30分钟直接归因'
    WHEN lag_minutes < 1440 THEN '1日间接归因'
    WHEN lag_minutes < 4320 THEN '3日间接归因'
    WHEN lag_minutes < 10080 THEN '7日间接归因'
    ELSE '30日间接归因'
  END AS attribution_period,
  CASE
    WHEN click_event_id IS NULL THEN 6
    WHEN lag_minutes <= 30 THEN 1
    WHEN lag_minutes < 1440 THEN 2
    WHEN lag_minutes < 4320 THEN 3
    WHEN lag_minutes < 10080 THEN 4
    ELSE 5
  END AS attribution_sort,
  lag_minutes,
  click_event_id IS NOT NULL AS is_attributed,
  CURRENT_TIMESTAMP AS updated_at
FROM ranked_touch;

-- Spend is deliberately not copied to every order: doing so would multiply
-- creative spend when one click/creative produces multiple orders.  Spend and
-- ROI remain authoritative in ads_creative_offline_di.
INSERT INTO paimon.ad_dw.ads_attribution_summary_di
SELECT
  event_date,
  order_advertiser_id AS advertiser_id,
  MAX(order_advertiser_name) AS advertiser_name,
  order_campaign_id AS campaign_id,
  MAX(order_campaign_name) AS campaign_name,
  attribution_period AS attribution_model,
  SUM(CASE WHEN is_attributed THEN 1 ELSE 0 END) AS conversions,
  COUNT(*) AS orders,
  CAST(SUM(order_gmv) AS DECIMAL(18,2)) AS attributed_gmv,
  CAST(0 AS DECIMAL(18,4)) AS attributed_spend,
  CURRENT_TIMESTAMP AS updated_at
FROM paimon.ad_dw.ads_order_attribution_detail_di
GROUP BY event_date, order_advertiser_id, order_campaign_id, attribution_period;
