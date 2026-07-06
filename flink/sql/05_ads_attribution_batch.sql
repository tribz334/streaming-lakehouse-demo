SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.ads_attribution_summary_di;

CREATE TEMPORARY VIEW dwd_ad_events_latest AS
SELECT *
FROM paimon.ad_dw.dwd_ad_events_di /*+ OPTIONS('scan.mode' = 'latest') */;

INSERT INTO paimon.ad_dw.ads_attribution_summary_di
WITH touchpoints AS (
  SELECT
    event_id AS touch_event_id,
    event_ts AS touch_ts,
    advertiser_id,
    advertiser_name,
    campaign_id,
    campaign_name,
    user_id,
    spend AS touch_spend
  FROM dwd_ad_events_latest
  WHERE event_type = 'click'
),
outcomes AS (
  SELECT
    event_date,
    event_id AS outcome_event_id,
    event_ts AS outcome_ts,
    advertiser_id,
    advertiser_name,
    campaign_id,
    campaign_name,
    user_id,
    event_type,
    gmv
  FROM dwd_ad_events_latest
  WHERE event_type IN ('conversion', 'order')
),
ranked AS (
  SELECT
    o.event_date,
    o.outcome_event_id,
    o.event_type,
    o.gmv,
    t.advertiser_id,
    t.advertiser_name,
    t.campaign_id,
    t.campaign_name,
    t.touch_spend,
    ROW_NUMBER() OVER (
      PARTITION BY o.outcome_event_id
      ORDER BY t.touch_ts DESC
    ) AS rn
  FROM outcomes AS o
  JOIN touchpoints AS t
    ON o.user_id = t.user_id
   AND o.advertiser_id = t.advertiser_id
   AND t.touch_ts <= o.outcome_ts
   AND t.touch_ts >= o.outcome_ts - INTERVAL '7' DAY
)
SELECT
  event_date,
  advertiser_id,
  MAX(advertiser_name) AS advertiser_name,
  campaign_id,
  MAX(campaign_name) AS campaign_name,
  'last_click_7d' AS attribution_model,
  SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END) AS conversions,
  SUM(CASE WHEN event_type = 'order' THEN 1 ELSE 0 END) AS orders,
  CAST(SUM(gmv) AS DECIMAL(18,2)) AS attributed_gmv,
  CAST(SUM(touch_spend) AS DECIMAL(18,4)) AS attributed_spend,
  CURRENT_TIMESTAMP AS updated_at
FROM ranked
WHERE rn = 1
GROUP BY event_date, advertiser_id, campaign_id;
