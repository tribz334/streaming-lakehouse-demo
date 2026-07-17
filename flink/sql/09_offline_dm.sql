SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.dm_attribution_touchpoint_df;
TRUNCATE TABLE paimon.ad_dw.dm_antifraud_feature_df;

INSERT INTO paimon.ad_dw.dm_attribution_touchpoint_df
SELECT candidate_id,stat_date,outcome_event_id,order_id,user_id,
  CASE WHEN touch_event_id IS NULL THEN 0 ELSE touchpoint_seq END,
  CASE WHEN touch_event_id IS NULL THEN 'organic' ELSE 'click' END,
  touch_time,creative_id,campaign_id,advertiser_id,
  touch_event_id IS NOT NULL AND touchpoint_seq=1,
  'last_click_30d',
  CAST(CASE WHEN touch_event_id IS NOT NULL AND touchpoint_seq=1 THEN 1 ELSE 0 END AS DECIMAL(10,6)),
  CAST(CASE WHEN touch_event_id IS NOT NULL AND touchpoint_seq=1 THEN order_gmv ELSE 0 END AS DECIMAL(18,4)),
  CAST(CASE WHEN touch_event_id IS NOT NULL AND touchpoint_seq=1 THEN 1 ELSE 0 END AS DECIMAL(10,6)),
  30
FROM paimon.ad_dw.dws_attribution_candidate_df;

INSERT INTO paimon.ad_dw.dm_antifraud_feature_df
SELECT stat_date,event_id,user_id,device_id,device_ip,creative_id,slot_id,
  click_cnt_1h,click_cnt_1d,ip_click_cnt_1h,ip_uv_1h,click_interval_ms,
  ctr_deviation,CAST(0 AS DECIMAL(10,6)),FALSE,is_night_burst,
  CAST(CASE
    WHEN click_cnt_1h>=10 THEN 0.9500
    WHEN click_cnt_1h>=5 AND ctr_deviation>=0.700000 THEN 0.8500
    WHEN click_cnt_1d>=20 THEN 0.7500
    ELSE 0.1000
  END AS DECIMAL(6,4)),
  CASE
    WHEN click_cnt_1h>=10 THEN 'HIGH_CLICK_BURST'
    WHEN click_cnt_1h>=5 AND ctr_deviation>=0.700000 THEN 'ABNORMAL_CTR'
    WHEN click_cnt_1d>=20 THEN 'HIGH_DAILY_CLICKS'
    ELSE 'NORMAL'
  END,
  CASE
    WHEN click_cnt_1h>=10 THEN ARRAY['HIGH_CLICK_BURST']
    WHEN click_cnt_1h>=5 AND ctr_deviation>=0.700000 THEN ARRAY['ABNORMAL_CTR']
    WHEN click_cnt_1d>=20 THEN ARRAY['HIGH_DAILY_CLICKS']
    ELSE ARRAY['NONE']
  END
FROM paimon.ad_dw.dws_user_click_window_df;
