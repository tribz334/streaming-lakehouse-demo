SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';

-- Upsert Kafka preserves Paimon's primary-key changelog. StarRocks consumes
-- each JSON value as an idempotent Primary Key UPSERT.
INSERT INTO default_catalog.default_database.starrocks_realtime_metric_kafka
SELECT
  window_start,
  advertiser_id,
  campaign_id,
  unit_id,
  creative_id,
  window_end,
  advertiser_name,
  spend,
  gmv,
  impressions,
  clicks,
  conversions,
  orders,
  ctr,
  cvr,
  roi,
  updated_at
FROM paimon.ad_dw.dws_ad_metric_stream_10s;
