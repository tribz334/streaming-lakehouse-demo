SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

INSERT INTO paimon.ad_dw.dws_ad_stream_10s
SELECT
  window_start,
  window_end,
  advertiser_id,
  SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END),
  CAST(SUM(spend) AS DECIMAL(18,4)),
  CAST(SUM(gmv) AS DECIMAL(18,2)),
  CAST(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) /
    NULLIF(SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END),0) AS DECIMAL(8,6)),
  CAST(SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END) /
    NULLIF(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END),0) AS DECIMAL(8,6))
FROM TABLE(TUMBLE(
  TABLE paimon.ad_dw.dwd_ad_events_di,
  DESCRIPTOR(event_ts), INTERVAL '10' SECOND
))
GROUP BY window_start, window_end, advertiser_id;
