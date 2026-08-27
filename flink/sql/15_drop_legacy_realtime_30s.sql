-- Remove the obsolete lake table after all producers have moved to 10 seconds.
CREATE CATALOG paimon WITH (
  'type'='paimon',
  'metastore'='filesystem',
  'warehouse'='file:///warehouse/paimon'
);
USE CATALOG paimon;
USE ad_dw;
DROP TABLE IF EXISTS ads_realtime_metric_30s;
