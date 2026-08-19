-- Daily current-existence DIM snapshot (not an SCD/zipper table).
-- `dt` is the snapshot date and the leading part of every primary key. A
-- business id may occur at most once in one dt partition. Source DELETE events
-- disappear from the CDC current mirror; units only exist from start through
-- end (both boundaries inclusive), and creatives inherit that lifetime.
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

DELETE FROM paimon.ad_dw.dim_creative_df
WHERE dt = CAST(CURRENT_DATE AS STRING);
DELETE FROM paimon.ad_dw.dim_unit_df
WHERE dt = CAST(CURRENT_DATE AS STRING);
DELETE FROM paimon.ad_dw.dim_campaign_df
WHERE dt = CAST(CURRENT_DATE AS STRING);
DELETE FROM paimon.ad_dw.dim_advertiser_df
WHERE dt = CAST(CURRENT_DATE AS STRING);

INSERT INTO paimon.ad_dw.dim_advertiser_df
SELECT CAST(CURRENT_DATE AS STRING), advertiser_id, advertiser_name, industry,
  tier, home_region, signup_date, updated_at
FROM paimon.ad_dw.ods_dim_advertiser_current;

INSERT INTO paimon.ad_dw.dim_campaign_df
SELECT CAST(CURRENT_DATE AS STRING), campaign_id, advertiser_id, campaign_name,
  promotion_goal, ad_type, bidding_strategy, budget_mode, budget, status, updated_at
FROM paimon.ad_dw.ods_dim_campaign_current;

INSERT INTO paimon.ad_dw.dim_unit_df
SELECT CAST(CURRENT_DATE AS STRING), unit_id, campaign_id, `广告组名称`, `投放位置`,
  `推广落地页网址`, `关联商品ID`, `目标人群`, `投放日期类型`, `开始日期`, `结束日期`,
  `单日预算模式`, `单日预算`, `出价方式`, `转化目标`, `转化出价`, status, updated_at
FROM paimon.ad_dw.ods_dim_unit_current
WHERE `开始日期` <= CURRENT_DATE
  AND (`结束日期` IS NULL OR `结束日期` >= CURRENT_DATE);

INSERT INTO paimon.ad_dw.dim_creative_df
SELECT CAST(CURRENT_DATE AS STRING), cr.creative_id, u.campaign_id, cr.unit_id,
  cr.creative_name, cr.format, cr.updated_at
FROM paimon.ad_dw.ods_dim_creative_current AS cr
JOIN paimon.ad_dw.ods_dim_unit_current AS u ON cr.unit_id = u.unit_id
WHERE u.`开始日期` <= CURRENT_DATE
  AND (u.`结束日期` IS NULL OR u.`结束日期` >= CURRENT_DATE);
