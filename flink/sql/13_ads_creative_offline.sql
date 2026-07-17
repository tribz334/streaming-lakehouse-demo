-- Creative-grain offline serving dataset for Superset multi-dimensional BI.
-- The DWS subject table is the fact-side source; dimensions are only used to
-- enrich stable descriptive attributes.  Ratios are recalculated from
-- additive facts to remain correct after arbitrary BI grouping.
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.ads_creative_offline_di;

INSERT INTO paimon.ad_dw.ads_creative_offline_di
SELECT
  d.stat_date,
  d.creative_id,
  COALESCE(cr.creative_name, d.creative_type, 'UNKNOWN') AS creative_name,
  COALESCE(cr.format, 'unknown') AS creative_format,
  d.campaign_id,
  COALESCE(c.campaign_name, 'UNKNOWN') AS campaign_name,
  COALESCE(CASE WHEN c.objective = 'ROI' THEN 'ROAS' ELSE c.objective END, 'UNKNOWN') AS campaign_objective,
  c.budget AS campaign_budget,
  COALESCE(c.status, 'UNKNOWN') AS campaign_status,
  c.advertiser_id,
  COALESCE(a.advertiser_name, 'UNKNOWN') AS advertiser_name,
  COALESCE(a.industry, 'UNKNOWN') AS industry,
  COALESCE(a.tier, 'UNKNOWN') AS advertiser_tier,
  cr.unit_id,
  COALESCE(u.unit_name, 'UNKNOWN') AS unit_name,
  COALESCE(u.bid_type, 'UNKNOWN') AS bid_type,
  u.bid_amount,
  d.imp_cnt_1d AS impressions,
  d.click_cnt_1d AS clicks,
  d.conv_cnt_1d AS conversions,
  d.order_cnt_1d AS orders,
  d.cost_1d AS cost,
  d.gmv_1d AS gmv,
  CAST(d.click_cnt_1d * 1.0 / NULLIF(d.imp_cnt_1d, 0) AS DECIMAL(18,6)) AS ctr,
  CAST(d.conv_cnt_1d * 1.0 / NULLIF(d.click_cnt_1d, 0) AS DECIMAL(18,6)) AS cvr,
  CAST(d.cost_1d / NULLIF(d.click_cnt_1d, 0) AS DECIMAL(18,4)) AS cpc,
  CAST(d.cost_1d / NULLIF(d.conv_cnt_1d, 0) AS DECIMAL(18,4)) AS cpa,
  CAST(d.gmv_1d / NULLIF(d.cost_1d, 0) AS DECIMAL(18,6)) AS roi,
  CURRENT_TIMESTAMP AS updated_at
FROM paimon.ad_dw.dws_creative_df AS d
LEFT JOIN paimon.ad_dw.dim_creative_df AS cr
  ON d.creative_id = cr.creative_id
LEFT JOIN paimon.ad_dw.dim_campaign_df AS c
  ON d.campaign_id = c.campaign_id
LEFT JOIN paimon.ad_dw.dim_advertiser_df AS a
  ON c.advertiser_id = a.advertiser_id
LEFT JOIN paimon.ad_dw.dim_unit_df AS u
  ON cr.unit_id = u.unit_id
WHERE d.stat_date < CAST(CURRENT_DATE AS STRING);
