-- Offline-only expansion from the shared DWD event spine.
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.dwd_ad_bid_di;
TRUNCATE TABLE paimon.ad_dw.dwd_ad_impression_di;
TRUNCATE TABLE paimon.ad_dw.dwd_ad_click_di;
TRUNCATE TABLE paimon.ad_dw.dwd_ad_conversion_di;
TRUNCATE TABLE paimon.ad_dw.dwd_ad_cost_di;
TRUNCATE TABLE paimon.ad_dw.dwm_ad_event_wide;

CREATE TEMPORARY VIEW thesis_events AS
SELECT * FROM paimon.ad_dw.dwd_ad_events_di /*+ OPTIONS('scan.mode'='latest') */;

INSERT INTO paimon.ad_dw.dwd_ad_bid_di
SELECT event_id,event_ts,event_date,advertiser_id,campaign_id,unit_id,creative_id,
  media,user_id,CAST(0 AS DECIMAL(10,4)),CAST(0 AS DECIMAL(10,4)),TRUE,
  CAST(NULL AS STRING)
FROM thesis_events WHERE event_type='bid';

INSERT INTO paimon.ad_dw.dwd_ad_impression_di
SELECT event_id,CAST(NULL AS STRING),event_ts,event_date,advertiser_id,campaign_id,
  unit_id,creative_id,media,user_id,1000,TRUE
FROM thesis_events WHERE event_type='impression';

INSERT INTO paimon.ad_dw.dwd_ad_click_di
SELECT event_id,CAST(NULL AS STRING),event_ts,event_date,advertiser_id,campaign_id,
  unit_id,creative_id,media,user_id,CAST(NULL AS STRING),CAST(NULL AS STRING),TRUE
FROM thesis_events WHERE event_type='click';

INSERT INTO paimon.ad_dw.dwd_ad_conversion_di
SELECT event_id,CAST(NULL AS STRING),event_ts,event_date,advertiser_id,campaign_id,
  unit_id,creative_id,media,user_id,'order',gmv,order_id,168
FROM thesis_events WHERE event_type IN ('conversion','order');

INSERT INTO paimon.ad_dw.dwd_ad_cost_di
SELECT CONCAT('cost_',event_id),event_ts,event_date,advertiser_id,campaign_id,
  unit_id,creative_id,media,'CPC',event_id,spend,spend,'CNY'
FROM thesis_events WHERE spend > 0;

INSERT INTO paimon.ad_dw.dwm_ad_event_wide
SELECT event_id,event_type,event_ts,event_date,advertiser_id,advertiser_name,industry,
  campaign_id,campaign_name,CAST(NULL AS STRING),creative_id,creative_name,unit_id,
  media,media,CAST(NULL AS STRING),user_id,region,CAST(NULL AS STRING),
  CAST(CASE WHEN event_type='impression' THEN 1 ELSE 0 END AS TINYINT),
  CAST(CASE WHEN event_type='click' THEN 1 ELSE 0 END AS TINYINT),
  CAST(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END AS TINYINT),
  'CPC',spend,gmv,order_id,gmv,event_date
FROM thesis_events;

TRUNCATE TABLE paimon.ad_dw.dws_advertiser_df;
TRUNCATE TABLE paimon.ad_dw.dws_campaign_df;
TRUNCATE TABLE paimon.ad_dw.dws_creative_df;
TRUNCATE TABLE paimon.ad_dw.dws_slot_df;
TRUNCATE TABLE paimon.ad_dw.dws_user_df;
TRUNCATE TABLE paimon.ad_dw.dws_region_df;

INSERT INTO paimon.ad_dw.dws_advertiser_df
SELECT event_date,advertiser_id,MAX(industry),
  SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END),
  CAST(SUM(spend) AS DECIMAL(18,2)),CAST(SUM(gmv) AS DECIMAL(18,2)),
  SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),
  CAST(SUM(spend) AS DECIMAL(18,2)),CAST(SUM(gmv) AS DECIMAL(18,2)),
  CAST(SUM(spend) AS DECIMAL(18,2)),CAST(SUM(gmv) AS DECIMAL(18,2)),
  CAST(SUM(spend) AS DECIMAL(18,2)),CAST(SUM(gmv) AS DECIMAL(18,2)),
  CAST(SUM(spend)*1000/NULLIF(SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),0) AS DECIMAL(10,4)),
  CAST(SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),0) AS DECIMAL(8,6)),
  CAST(SUM(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),0) AS DECIMAL(8,6)),
  CAST(SUM(gmv)/NULLIF(SUM(spend),0) AS DECIMAL(10,4)),
  MIN(CASE WHEN spend>0 THEN event_date END),MAX(CASE WHEN spend>0 THEN event_date END)
FROM thesis_events GROUP BY event_date,advertiser_id;

INSERT INTO paimon.ad_dw.dws_campaign_df
SELECT event_date,campaign_id,MAX(advertiser_id),
  SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END),
  CAST(SUM(spend) AS DECIMAL(18,2)),CAST(SUM(spend) AS DECIMAL(18,2)),
  CAST(SUM(spend) AS DECIMAL(18,2)),CAST(SUM(spend) AS DECIMAL(18,2)),
  CAST(SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),0) AS DECIMAL(8,6)),
  CAST(SUM(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),0) AS DECIMAL(8,6)),
  CAST(SUM(spend)/NULLIF(SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),0) AS DECIMAL(10,4)),
  CAST(SUM(spend)/NULLIF(SUM(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END),0) AS DECIMAL(10,4)),
  CAST(0 AS DECIMAL(8,6))
FROM thesis_events GROUP BY event_date,campaign_id;

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

INSERT INTO paimon.ad_dw.dws_slot_df
SELECT event_date,media,media,media,CAST(NULL AS STRING),
  SUM(CASE WHEN event_type='bid' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='bid' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),CAST(SUM(spend) AS DECIMAL(18,2)),
  CAST(1 AS DECIMAL(8,6)),
  CAST(SUM(spend)*1000/NULLIF(SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),0) AS DECIMAL(10,4)),
  CAST(SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN event_type='bid' THEN 1 ELSE 0 END),0) AS DECIMAL(8,6))
FROM thesis_events GROUP BY event_date,media;

INSERT INTO paimon.ad_dw.dws_user_df
SELECT event_date,user_id,MAX(region),
  SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='order' THEN 1 ELSE 0 END),CAST(SUM(gmv) AS DECIMAL(18,2)),
  1,MAX(event_date),CAST(SUM(gmv) AS DECIMAL(18,2))
FROM thesis_events GROUP BY event_date,user_id;

INSERT INTO paimon.ad_dw.dws_region_df
SELECT event_date,region,region,
  SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END),
  CAST(SUM(spend) AS DECIMAL(18,2)),CAST(SUM(gmv) AS DECIMAL(18,2)),COUNT(DISTINCT user_id),
  CAST(SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END),0) AS DECIMAL(8,6)),
  CAST(SUM(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),0) AS DECIMAL(8,6))
FROM thesis_events GROUP BY event_date,region;

TRUNCATE TABLE paimon.ad_dw.dm_attribution_touchpoint_df;
TRUNCATE TABLE paimon.ad_dw.dm_antifraud_feature_df;

INSERT INTO paimon.ad_dw.dm_attribution_touchpoint_df
WITH outcomes AS (
  SELECT event_id,event_date,event_ts,user_id,advertiser_id,campaign_id,order_id,gmv
  FROM thesis_events WHERE event_type IN ('conversion','order')
), ranked AS (
  SELECT CONCAT(o.event_id,'_',c.event_id) attribution_id,o.event_date,o.event_id conversion_id,
    o.order_id,o.user_id,c.event_ts,c.creative_id,c.campaign_id,c.advertiser_id,o.gmv,
    ROW_NUMBER() OVER(PARTITION BY o.event_id ORDER BY c.event_ts DESC) rn
  FROM outcomes o JOIN thesis_events c ON o.user_id=c.user_id AND o.advertiser_id=c.advertiser_id
    AND c.event_type='click' AND c.event_ts<=o.event_ts AND c.event_ts>=o.event_ts-INTERVAL '7' DAY
)
SELECT attribution_id,event_date,conversion_id,order_id,user_id,CAST(rn AS INT),'click',event_ts,
  creative_id,campaign_id,advertiser_id,rn=1,'last_click_7d',
  CAST(CASE WHEN rn=1 THEN 1 ELSE 0 END AS DECIMAL(10,6)),
  CAST(CASE WHEN rn=1 THEN gmv ELSE 0 END AS DECIMAL(18,4)),
  CAST(CASE WHEN rn=1 THEN 1 ELSE 0 END AS DECIMAL(10,6)),7
FROM ranked;

INSERT INTO paimon.ad_dw.dm_antifraud_feature_df
SELECT event_date,event_id,user_id,CAST(NULL AS STRING),CAST(NULL AS STRING),creative_id,media,
  1,1,1,1,CAST(NULL AS BIGINT),CAST(0 AS DECIMAL(10,6)),CAST(0 AS DECIMAL(10,6)),
  FALSE,FALSE,CAST(0 AS DECIMAL(6,4)),'normal',ARRAY['NONE']
FROM thesis_events WHERE event_type='click';
