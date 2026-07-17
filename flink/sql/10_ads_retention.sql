SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

TRUNCATE TABLE paimon.ad_dw.ads_advertiser_retention_di;

CREATE TEMPORARY VIEW dwd_ad_events_latest AS
SELECT *
FROM paimon.ad_dw.dwd_ad_events_di /*+ OPTIONS('scan.mode' = 'latest') */;

INSERT INTO paimon.ad_dw.ads_advertiser_retention_di
WITH active AS (
  SELECT DISTINCT event_date, advertiser_id
  FROM dwd_ad_events_latest
  WHERE spend > 0
),
cohort AS (
  SELECT event_date AS cohort_date, 
  advertiser_id
  FROM active
)
SELECT
  b.cohort_date,
  COUNT(DISTINCT b.advertiser_id) AS cohort_size,
  COUNT(DISTINCT r1.advertiser_id) AS retained_1d,
  COUNT(DISTINCT r7.advertiser_id) AS retained_7d,
  COUNT(DISTINCT r15.advertiser_id) AS retained_15d,
  COUNT(DISTINCT r30.advertiser_id) AS retained_30d,
  CAST(COUNT(DISTINCT r1.advertiser_id) / NULLIF(COUNT(DISTINCT b.advertiser_id), 0) AS DECIMAL(18,6)) AS rate_1d,
  CAST(COUNT(DISTINCT r7.advertiser_id) / NULLIF(COUNT(DISTINCT b.advertiser_id), 0) AS DECIMAL(18,6)) AS rate_7d,
  CAST(COUNT(DISTINCT r15.advertiser_id) / NULLIF(COUNT(DISTINCT b.advertiser_id), 0) AS DECIMAL(18,6)) AS rate_15d,
  CAST(COUNT(DISTINCT r30.advertiser_id) / NULLIF(COUNT(DISTINCT b.advertiser_id), 0) AS DECIMAL(18,6)) AS rate_30d,
  CURRENT_TIMESTAMP AS updated_at
FROM cohort b
LEFT JOIN active r1
  ON b.advertiser_id = r1.advertiser_id
 AND r1.event_date = CAST(CAST(b.cohort_date AS DATE) + INTERVAL '1' DAY AS STRING)
LEFT JOIN active r7
  ON b.advertiser_id = r7.advertiser_id
 AND r7.event_date = CAST(CAST(b.cohort_date AS DATE) + INTERVAL '7' DAY AS STRING)
LEFT JOIN active r15
  ON b.advertiser_id = r15.advertiser_id
 AND r15.event_date = CAST(CAST(b.cohort_date AS DATE) + INTERVAL '15' DAY AS STRING)
LEFT JOIN active r30
  ON b.advertiser_id = r30.advertiser_id
 AND r30.event_date = CAST(CAST(b.cohort_date AS DATE) + INTERVAL '30' DAY AS STRING)
GROUP BY b.cohort_date;
