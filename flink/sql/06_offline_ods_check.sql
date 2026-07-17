-- The ODS layer is continuously written by the real-time Kafka ingestion job.
-- The daily workflow validates and consumes the bounded latest Paimon snapshot.
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT
  COUNT(*) AS ods_event_count,
  MIN(event_date) AS first_event_date,
  MAX(event_date) AS last_event_date
FROM paimon.ad_dw.ods_ad_events_di /*+ OPTIONS('scan.mode' = 'latest') */;
