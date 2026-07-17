SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://hive-metastore:9083',
  'warehouse' = 'file:///warehouse/paimon'
);

SELECT 'advertiser' AS source_table, COUNT(*) AS row_count
FROM paimon.ad_dw.dim_advertiser_df
UNION ALL
SELECT 'campaign', COUNT(*) FROM paimon.ad_dw.dim_campaign_df
UNION ALL
SELECT 'unit', COUNT(*) FROM paimon.ad_dw.dim_unit_df
UNION ALL
SELECT 'creative', COUNT(*) FROM paimon.ad_dw.dim_creative_df
UNION ALL
SELECT 'ad_order', COUNT(*) FROM paimon.ad_dw.ods_ad_order;
