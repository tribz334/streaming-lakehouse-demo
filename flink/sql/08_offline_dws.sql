SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.dws_creative_df;
TRUNCATE TABLE paimon.ad_dw.dws_attribution_candidate_df;
TRUNCATE TABLE paimon.ad_dw.dws_user_click_window_df;

CREATE TEMPORARY VIEW thesis_events AS
SELECT * FROM paimon.ad_dw.dwd_ad_events_di /*+ OPTIONS('scan.mode'='latest') */;

INSERT INTO paimon.ad_dw.dws_creative_df
SELECT event_date,creative_id,MAX(campaign_id),MAX(creative_name),
  SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END),
  CAST(SUM(spend) AS DECIMAL(18,2)),CAST(SUM(gmv) AS DECIMAL(18,2)),
  CAST(SUM(spend) AS DECIMAL(18,2)),CAST(SUM(gmv) AS DECIMAL(18,2)),
  CAST(SUM(spend) AS DECIMAL(18,2)),
  CAST(SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),0) AS DECIMAL(8,6)),
  CAST(SUM(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),0) AS DECIMAL(8,6)),
  CAST(SUM(gmv)/NULLIF(SUM(spend),0) AS DECIMAL(10,4)),
  SUM(CASE WHEN event_type='order' THEN 1 ELSE 0 END)
FROM thesis_events GROUP BY event_date,creative_id;

INSERT INTO paimon.ad_dw.dws_attribution_candidate_df
WITH outcomes AS (
  SELECT event_date,event_id,order_id,event_ts,user_id,advertiser_id,advertiser_name,
    campaign_id,campaign_name,gmv
  FROM thesis_events
  WHERE event_type='order'
), touchpoints AS (
  SELECT event_id,event_ts,user_id,creative_id,campaign_id,campaign_name,
    advertiser_id,advertiser_name,spend
  FROM thesis_events
  WHERE event_type='click'
), candidates AS (
  SELECT
    o.event_date,
    CONCAT(o.event_id,'_',COALESCE(t.event_id,'organic')) AS candidate_id,
    o.event_id AS outcome_event_id,o.order_id,o.event_ts AS outcome_time,o.user_id,
    o.advertiser_id AS order_advertiser_id,o.advertiser_name AS order_advertiser_name,
    o.campaign_id AS order_campaign_id,o.campaign_name AS order_campaign_name,o.gmv AS order_gmv,
    t.event_id AS touch_event_id,t.event_ts AS touch_time,t.creative_id,
    t.campaign_id,t.campaign_name,t.advertiser_id,t.advertiser_name,t.spend AS touch_spend,
    ROW_NUMBER() OVER(PARTITION BY o.event_id ORDER BY t.event_ts DESC) AS touchpoint_seq
  FROM outcomes o
  LEFT JOIN touchpoints t
    ON o.user_id=t.user_id
   AND o.advertiser_id=t.advertiser_id
   AND t.event_ts<=o.event_ts
   AND t.event_ts>=o.event_ts-INTERVAL '30' DAY
)
SELECT event_date,candidate_id,outcome_event_id,order_id,outcome_time,user_id,
  order_advertiser_id,order_advertiser_name,order_campaign_id,order_campaign_name,
  CAST(order_gmv AS DECIMAL(18,2)),touch_event_id,touch_time,creative_id,campaign_id,
  campaign_name,advertiser_id,advertiser_name,touch_spend,CAST(touchpoint_seq AS INT),
  CASE WHEN touch_event_id IS NULL THEN CAST(NULL AS BIGINT)
       ELSE TIMESTAMPDIFF(MINUTE,touch_time,outcome_time) END
FROM candidates;

INSERT INTO paimon.ad_dw.dws_user_click_window_df
WITH raw_clicks AS (
  SELECT event_date,event_id,event_ts,user_id,creative_id,media,advertiser_id,
    advertiser_name,spend
  FROM thesis_events
  WHERE event_type='click'
), daily_clicks AS (
  SELECT event_date,user_id,COUNT(*) AS click_cnt_1d
  FROM raw_clicks
  GROUP BY event_date,user_id
), sequenced_clicks AS (
  SELECT event_date,event_id,event_ts,user_id,creative_id,media,advertiser_id,
    advertiser_name,spend,
    LAG(event_ts) OVER(PARTITION BY user_id ORDER BY event_ts) AS previous_click_time
  FROM raw_clicks
), clicks AS (
  SELECT s.event_date,s.event_id,s.event_ts,s.user_id,s.creative_id,s.media,
    s.advertiser_id,s.advertiser_name,s.spend,d.click_cnt_1d,s.previous_click_time
  FROM sequenced_clicks s
  JOIN daily_clicks d ON s.event_date=d.event_date AND s.user_id=d.user_id
), rolling_features AS (
  SELECT
    c.event_date,c.event_id,c.event_ts,c.user_id,c.creative_id,c.media,
    c.advertiser_id,c.advertiser_name,c.spend,c.click_cnt_1d,c.previous_click_time,
    SUM(CASE WHEN h.event_type='click' THEN 1 ELSE 0 END) AS click_cnt_1h,
    SUM(CASE WHEN h.event_type='impression' THEN 1 ELSE 0 END) AS impression_cnt_1h,
    SUM(CASE WHEN h.event_type='impression'
              AND h.event_ts>=c.event_ts-INTERVAL '1' MINUTE THEN 1 ELSE 0 END) AS impression_cnt_1m
  FROM clicks c
  LEFT JOIN thesis_events h
    ON h.user_id=c.user_id
   AND h.media=c.media
   AND h.event_ts<=c.event_ts
   AND h.event_ts>=c.event_ts-INTERVAL '1' HOUR
  GROUP BY c.event_date,c.event_id,c.event_ts,c.user_id,c.creative_id,c.media,
    c.advertiser_id,c.advertiser_name,c.spend,c.click_cnt_1d,c.previous_click_time
)
SELECT event_date,event_id,event_ts,user_id,user_id,CAST(NULL AS STRING),creative_id,media,
  advertiser_id,advertiser_name,media,spend,
  CAST(click_cnt_1h AS INT),CAST(click_cnt_1d AS INT),CAST(impression_cnt_1h AS INT),
  CAST(impression_cnt_1m AS INT),CAST(click_cnt_1h AS INT),1,
  CASE WHEN previous_click_time IS NULL THEN CAST(NULL AS BIGINT)
       ELSE TIMESTAMPDIFF(SECOND,previous_click_time,event_ts)*1000 END,
  CAST(
    CASE WHEN impression_cnt_1h=0 THEN click_cnt_1h
         ELSE click_cnt_1h*1.0/impression_cnt_1h-0.10 END
    AS DECIMAL(10,6)),
  EXTRACT(HOUR FROM event_ts) BETWEEN 0 AND 5
FROM rolling_features;
