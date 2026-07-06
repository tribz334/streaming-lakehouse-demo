SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.ads_fraud_signal_di;

CREATE TEMPORARY VIEW dwd_ad_events_latest AS
SELECT *
FROM paimon.ad_dw.dwd_ad_events_di /*+ OPTIONS('scan.mode' = 'latest') */;

INSERT INTO paimon.ad_dw.ads_fraud_signal_di
WITH traffic_window AS (
  SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd') AS event_date,
    window_start,
    window_end,
    advertiser_id,
    MAX(advertiser_name) AS advertiser_name,
    media,
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS click_count,
    SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) AS impression_count,
    COUNT(DISTINCT user_id) AS unique_users,
    CAST(SUM(CASE WHEN event_type = 'click' THEN spend ELSE CAST(0 AS DECIMAL(18,4)) END) AS DECIMAL(18,4)) AS suspicious_spend
  FROM TABLE(
    TUMBLE(TABLE dwd_ad_events_latest, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
  )
  GROUP BY window_start, window_end, advertiser_id, media
),
scored AS (
  SELECT
    event_date,
    window_start,
    window_end,
    advertiser_id,
    advertiser_name,
    media,
    click_count,
    impression_count,
    unique_users,
    suspicious_spend,
    CASE
      WHEN click_count >= 30 THEN 'HIGH_CLICK_BURST'
      WHEN impression_count > 0 AND click_count * 1.0 / impression_count >= 5.0 AND click_count >= 20 THEN 'ABNORMAL_CTR'
      WHEN unique_users <= 3 AND click_count >= 20 THEN 'CONCENTRATED_USERS'
      ELSE 'LOW_RISK'
    END AS rule_code,
    CASE
      WHEN click_count >= 30 THEN 'Injected/demo one-minute click burst exceeds threshold'
      WHEN impression_count > 0 AND click_count * 1.0 / impression_count >= 5.0 AND click_count >= 20 THEN 'Injected/demo click-through rate is abnormally high'
      WHEN unique_users <= 3 AND click_count >= 20 THEN 'Clicks are concentrated on too few users'
      ELSE 'No fraud rule triggered'
    END AS rule_desc,
    CASE
      WHEN click_count >= 30 THEN CAST(0.950000 AS DECIMAL(18,6))
      WHEN impression_count > 0 AND click_count * 1.0 / impression_count >= 5.0 AND click_count >= 20 THEN CAST(0.850000 AS DECIMAL(18,6))
      WHEN unique_users <= 3 AND click_count >= 20 THEN CAST(0.750000 AS DECIMAL(18,6))
      ELSE CAST(0.100000 AS DECIMAL(18,6))
    END AS risk_score
  FROM traffic_window
)
SELECT
  event_date,
  window_start,
  window_end,
  advertiser_id,
  advertiser_name,
  media,
  rule_code,
  rule_desc,
  click_count,
  impression_count,
  unique_users,
  suspicious_spend,
  risk_score,
  CURRENT_TIMESTAMP AS updated_at
FROM scored
WHERE rule_code <> 'LOW_RISK';
