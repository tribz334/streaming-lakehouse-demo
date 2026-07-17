-- Daily bounded refresh of MySQL dimensions and order lifecycle data.
-- ODS events are continuously maintained by the real-time ingestion workflow;
-- the offline workflow consumes their latest Paimon snapshot.
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
