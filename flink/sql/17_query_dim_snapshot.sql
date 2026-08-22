-- Query a DIM table exactly as it was at the daily Paimon tag.
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://hive-metastore:9083',
  'warehouse' = 'file:///warehouse/paimon'
);

SELECT * FROM paimon.ad_dw.dim_user_zip
/*+ OPTIONS('scan.tag-name'='2026-08-16') */;
