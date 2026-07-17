SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

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
