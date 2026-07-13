-- Refresh JDBC dimensions and re-enrich the shared DWD spine after seed changes.
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

INSERT INTO paimon.ad_dw.dim_advertiser_df
SELECT advertiser_id, advertiser_name, industry, tier, home_region, signup_date, updated_at
FROM default_catalog.default_database.mysql_advertiser;

INSERT INTO paimon.ad_dw.dim_campaign_df
SELECT campaign_id, advertiser_id, campaign_name, objective, budget, status, updated_at
FROM default_catalog.default_database.mysql_campaign;

INSERT INTO paimon.ad_dw.dim_creative_df
SELECT creative_id, campaign_id, unit_id, creative_name, format, updated_at
FROM default_catalog.default_database.mysql_creative;

INSERT INTO paimon.ad_dw.dim_unit_df
SELECT unit_id, campaign_id, unit_name, bid_type, bid_amount, status, updated_at
FROM default_catalog.default_database.mysql_unit;

INSERT INTO paimon.ad_dw.dwd_ad_events_di
SELECT
  o.event_date,
  o.event_id,
  o.event_ts,
  o.advertiser_id,
  COALESCE(a.advertiser_name, 'UNKNOWN'),
  COALESCE(a.industry, 'UNKNOWN'),
  COALESCE(a.tier, 'UNKNOWN'),
  o.campaign_id,
  COALESCE(c.campaign_name, 'UNKNOWN'),
  o.unit_id,
  o.creative_id,
  COALESCE(cr.creative_name, 'UNKNOWN'),
  o.media,
  o.region,
  o.user_id,
  o.event_type,
  o.spend,
  o.gmv,
  o.order_id,
  CURRENT_TIMESTAMP
FROM paimon.ad_dw.ods_ad_events_di o
LEFT JOIN paimon.ad_dw.dim_advertiser_df a
  ON o.advertiser_id = a.advertiser_id
LEFT JOIN paimon.ad_dw.dim_campaign_df c
  ON o.campaign_id = c.campaign_id
LEFT JOIN paimon.ad_dw.dim_creative_df cr
  ON o.creative_id = cr.creative_id;
