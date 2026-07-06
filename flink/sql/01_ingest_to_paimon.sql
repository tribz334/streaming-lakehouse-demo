SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

EXECUTE STATEMENT SET
BEGIN
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

  INSERT INTO paimon.ad_dw.dwd_order_lifecycle_df
  SELECT order_id, advertiser_id, creative_id, user_id, gmv, order_status,
         create_time, payment_time, refund_time, finish_time, updated_at
  FROM default_catalog.default_database.mysql_order;

  INSERT INTO paimon.ad_dw.ods_ad_events_di
  SELECT
    SUBSTRING(ts, 1, 10) AS event_date,
    event_id,
    CAST(REPLACE(SUBSTRING(ts, 1, 23), 'T', ' ') AS TIMESTAMP(3)) AS event_ts,
    advertiser_id,
    campaign_id,
    unit_id,
    creative_id,
    media,
    region,
    user_id,
    event_type,
    bid_price,
    spend,
    gmv,
    order_id,
    'ods_log' AS source_topic,
    schema_version
  FROM default_catalog.default_database.ods_log_kafka;
END;
