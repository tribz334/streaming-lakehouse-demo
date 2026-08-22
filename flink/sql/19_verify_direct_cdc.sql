SET 'execution.runtime-mode'='batch';
SET 'sql-client.execution.result-mode'='TABLEAU';
CREATE CATALOG paimon WITH (
  'type'='paimon','metastore'='hive','uri'='thrift://hive-metastore:9083',
  'warehouse'='file:///warehouse/paimon'
);

SELECT advertiser_id,advertiser_name,status,updated_at
FROM paimon.ad_dw.dim_advertiser_zip
WHERE advertiser_id=1;

SELECT tag_name,snapshot_id,time_retained
FROM paimon.ad_dw.`dim_advertiser_zip$tags`;

SELECT 'action' AS table_name,tag_name,time_retained
FROM paimon.ad_dw.`dwd_ad_action_log_inc$tags`
UNION ALL SELECT 'bill',tag_name,time_retained
FROM paimon.ad_dw.`dwd_ad_bill_detail_inc$tags`
UNION ALL SELECT 'order',tag_name,time_retained
FROM paimon.ad_dw.`dwd_order_detail_acc$tags`;
