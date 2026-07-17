SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

INSERT INTO paimon.ad_dw.dwd_ad_events_di
SELECT
  o.event_date,
  o.event_id,
  o.event_ts,
  o.advertiser_id,
  COALESCE(a.advertiser_name, 'UNKNOWN') AS advertiser_name,
  COALESCE(a.industry, 'UNKNOWN') AS industry,
  COALESCE(a.tier, 'UNKNOWN') AS tier,
  o.campaign_id,
  COALESCE(c.campaign_name, 'UNKNOWN') AS campaign_name,
  o.unit_id,
  o.creative_id,
  COALESCE(cr.creative_name, 'UNKNOWN') AS creative_name,
  o.media,
  o.region,
  o.user_id,
  o.event_type,
  o.spend,
  o.gmv,
  o.order_id,
  CURRENT_TIMESTAMP AS loaded_at
FROM paimon.ad_dw.ods_ad_events_di /*+ OPTIONS('scan.mode' = 'latest') */ AS o
LEFT JOIN paimon.ad_dw.dim_advertiser_df AS a
  ON o.advertiser_id = a.advertiser_id
LEFT JOIN paimon.ad_dw.dim_campaign_df AS c
  ON o.campaign_id = c.campaign_id
LEFT JOIN paimon.ad_dw.dim_creative_df AS cr
  ON o.creative_id = cr.creative_id;
