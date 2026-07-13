-- Reset only generated fact data before a clean deterministic demo replay.
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

TRUNCATE TABLE paimon.ad_dw.dws_ad_metric_10s;
TRUNCATE TABLE paimon.ad_dw.dws_ad_metric_stream_10s;
TRUNCATE TABLE paimon.ad_dw.dws_ad_stream_10s;
TRUNCATE TABLE paimon.ad_dw.dwd_ad_events_di;
TRUNCATE TABLE paimon.ad_dw.ods_ad_events_di;
