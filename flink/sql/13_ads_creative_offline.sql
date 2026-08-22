-- Creative-grain offline serving dataset for Superset multi-dimensional BI.
-- The DM creative wide table is the fact-side source; dimensions are only used to
-- enrich stable descriptive attributes.  Ratios are recalculated from
-- additive facts to remain correct after arbitrary BI grouping.
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.ads_creative_offline_di;

INSERT INTO paimon.ad_dw.ads_creative_offline_di
SELECT
  d.dt,
  d.creative_id,
  COALESCE(cr.creative_name, 'UNKNOWN') AS creative_name,
  u.campaign_id,
  COALESCE(c.campaign_name, 'UNKNOWN') AS campaign_name,
  COALESCE(CAST(c.market_goal AS STRING), 'UNKNOWN') AS campaign_objective,
  CAST(c.budget / 100000.0 AS DECIMAL(18,2)) AS campaign_budget,
  COALESCE(CAST(c.status AS STRING), 'UNKNOWN') AS campaign_status,
  c.advertiser_id,
  COALESCE(a.advertiser_name, 'UNKNOWN') AS advertiser_name,
  COALESCE(a.industry_l2_name, 'UNKNOWN') AS industry,
  COALESCE(CAST(a.qualification_type AS STRING), 'UNKNOWN') AS advertiser_tier,
  cr.unit_id,
  COALESCE(u.unit_name, 'UNKNOWN') AS unit_name,
  COALESCE(u.bid_type, 'UNKNOWN') AS bid_type,
  CAST(u.bid / 100000.0 AS DECIMAL(18,4)) AS bid_amount,
  d.impression_cnt_1d AS impressions,
  d.click_cnt_1d AS clicks,
  d.conversion_cnt_1d AS conversions,
  d.order_pay_cnt_1d AS orders,
  CAST(d.cost_1d / 100000.0 AS DECIMAL(18,4)) AS cost,
  CAST(d.order_pay_gmv_1d / 100000.0 AS DECIMAL(18,2)) AS gmv,
  CAST(d.click_cnt_1d * 1.0 / NULLIF(d.impression_cnt_1d, 0) AS DECIMAL(18,6)) AS ctr,
  CAST(d.conversion_cnt_1d * 1.0 / NULLIF(d.click_cnt_1d, 0) AS DECIMAL(18,6)) AS cvr,
  CAST(d.cost_1d / 100000.0 / NULLIF(d.click_cnt_1d, 0) AS DECIMAL(18,4)) AS cpc,
  CAST(d.cost_1d / 100000.0 / NULLIF(d.conversion_cnt_1d, 0) AS DECIMAL(18,4)) AS cpa,
  CAST(d.order_pay_gmv_1d * 1.0 / NULLIF(d.cost_1d, 0) AS DECIMAL(18,6)) AS roi,
  CURRENT_TIMESTAMP AS updated_at
FROM paimon.ad_dw.dm_creative AS d
LEFT JOIN paimon.ad_dw.dim_creative AS cr
  ON d.creative_id = cr.creative_id
LEFT JOIN paimon.ad_dw.dim_unit AS u
  ON cr.unit_id = u.unit_id
LEFT JOIN paimon.ad_dw.dim_campaign AS c
  ON u.campaign_id = c.campaign_id
LEFT JOIN paimon.ad_dw.dim_advertiser_zip AS a
  ON c.advertiser_id = a.advertiser_id
WHERE d.dt < CAST(CURRENT_DATE AS STRING);
