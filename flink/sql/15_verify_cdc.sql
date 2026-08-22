-- Verify direct MySQL-to-DIM refresh and downstream facts.
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://hive-metastore:9083',
  'warehouse' = 'file:///warehouse/paimon'
);

SELECT 'advertiser' AS dimension_name,COUNT(*) AS row_count FROM paimon.ad_dw.dim_advertiser_zip
UNION ALL SELECT 'campaign',COUNT(*) FROM paimon.ad_dw.dim_campaign
UNION ALL SELECT 'unit',COUNT(*) FROM paimon.ad_dw.dim_unit
UNION ALL SELECT 'creative',COUNT(*) FROM paimon.ad_dw.dim_creative
UNION ALL SELECT 'user',COUNT(*) FROM paimon.ad_dw.dim_user_zip
UNION ALL SELECT 'shop',COUNT(*) FROM paimon.ad_dw.dim_shop_zip
UNION ALL SELECT 'product',COUNT(*) FROM paimon.ad_dw.dim_product_zip;

SELECT
  (SELECT COUNT(*) FROM paimon.ad_dw.ods_log_inc) AS ods_log_rows,
  (SELECT COUNT(*) FROM paimon.ad_dw.dwd_ad_action_log_inc) AS action_rows,
  (SELECT COUNT(*) FROM paimon.ad_dw.dwd_ad_bill_detail_inc) AS bill_rows,
  (SELECT COUNT(*) FROM paimon.ad_dw.dwd_order_detail_acc) AS order_rows;

SELECT
  SUM(CASE WHEN delivery_cnt_1d > delivery_cnt_7d
             OR delivery_cnt_7d > delivery_cnt_30d
             OR delivery_cnt_30d > delivery_cnt_acc THEN 1 ELSE 0 END) AS invalid_delivery_windows,
  SUM(CASE WHEN cost_1d > cost_7d
             OR cost_7d > cost_30d
             OR cost_30d > cost_acc THEN 1 ELSE 0 END) AS invalid_cost_windows
FROM paimon.ad_dw.dm_creative;
