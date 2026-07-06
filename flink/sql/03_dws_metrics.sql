SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.dws_ad_metric_10s;

CREATE TEMPORARY VIEW dwd_ad_events_latest AS
SELECT *
FROM paimon.ad_dw.dwd_ad_events_di /*+ OPTIONS('scan.mode' = 'latest') */;

INSERT INTO paimon.ad_dw.dws_ad_metric_10s
SELECT
  window_start,
  window_end,
  advertiser_id,
  MAX(advertiser_name) AS advertiser_name,
  campaign_id,
  unit_id,
  creative_id,
  CAST(SUM(spend) AS DECIMAL(18,4)) AS spend,
  CAST(SUM(gmv) AS DECIMAL(18,2)) AS gmv,
  SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) AS impressions,
  SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
  SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END) AS conversions,
  SUM(CASE WHEN event_type = 'order' THEN 1 ELSE 0 END) AS orders,
  CAST(
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) /
    NULLIF(SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END), 0)
    AS DECIMAL(18,6)
  ) AS ctr,
  CAST(
    SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END) /
    NULLIF(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END), 0)
    AS DECIMAL(18,6)
  ) AS cvr,
  CAST(SUM(gmv) / NULLIF(SUM(spend), 0) AS DECIMAL(18,6)) AS roi,
  CURRENT_TIMESTAMP AS updated_at
FROM TABLE(
  TUMBLE(TABLE dwd_ad_events_latest, DESCRIPTOR(event_ts), INTERVAL '10' SECOND)
)
GROUP BY window_start, window_end, advertiser_id, campaign_id, unit_id, creative_id;
