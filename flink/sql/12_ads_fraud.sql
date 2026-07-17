SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.ads_fraud_signal_di;

INSERT INTO paimon.ad_dw.ads_fraud_signal_di
WITH model_features AS (
  SELECT
    w.event_time,w.advertiser_id,w.advertiser_name,w.media,w.user_id,w.spend,
    w.impression_cnt_1m,m.fraud_label,m.fraud_score
  FROM paimon.ad_dw.dm_antifraud_feature_df m
  JOIN paimon.ad_dw.dws_user_click_window_df w
    ON m.stat_date=w.stat_date AND m.event_id=w.event_id
  WHERE m.fraud_label<>'NORMAL'
), feature_windows AS (
  SELECT
    window_start,window_end,advertiser_id,MAX(advertiser_name) AS advertiser_name,media,
    fraud_label,COUNT(*) AS click_count,MAX(impression_cnt_1m) AS impression_count,
    COUNT(DISTINCT user_id) AS unique_users,CAST(SUM(spend) AS DECIMAL(18,4)) AS suspicious_spend,
    CAST(MAX(fraud_score) AS DECIMAL(18,6)) AS risk_score
  FROM TABLE(
    TUMBLE(TABLE model_features, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
  )
  GROUP BY window_start,window_end,advertiser_id,media,fraud_label
)
SELECT
  DATE_FORMAT(window_start,'yyyy-MM-dd'),window_start,window_end,advertiser_id,
  advertiser_name,media,fraud_label,
  CASE
    WHEN fraud_label='HIGH_CLICK_BURST' THEN 'User click count in the rolling hour exceeds the burst threshold'
    WHEN fraud_label='ABNORMAL_CTR' THEN 'User rolling click-through rate deviates strongly from baseline'
    WHEN fraud_label='HIGH_DAILY_CLICKS' THEN 'User daily click count exceeds the threshold'
    ELSE 'DM anti-fraud model signal'
  END,
  click_count,impression_count,unique_users,suspicious_spend,risk_score,
  CURRENT_TIMESTAMP AS updated_at
FROM feature_windows;
