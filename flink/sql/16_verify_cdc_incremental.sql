SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://hive-metastore:9083',
  'warehouse' = 'file:///warehouse/paimon'
);

SELECT advertiser_id, advertiser_name, industry, tier
FROM paimon.ad_dw.dim_advertiser_df
WHERE advertiser_id = 'ADV_CDC_VERIFY';
