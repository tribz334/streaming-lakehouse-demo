-- Runtime acceptance checks for the Unit-owned classification contract.
SET 'execution.runtime-mode'='batch';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'sql-client.execution.result-mode'='TABLEAU';
CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');
CREATE CATALOG paimon WITH ('type'='paimon','metastore'='filesystem','warehouse'='file:///warehouse/paimon');

USE CATALOG fluss;
USE ad_dw;
SHOW TABLES;
DESCRIBE dim_unit_df;
DESCRIBE dwd_ad_event_di;
DESCRIBE dwd_ad_bill_di;
DESCRIBE dwd_ad_order_acc;
DESCRIBE dws_creative_di;
DESCRIBE dws_unit_di;
DESCRIBE dws_campaign_di;
DESCRIBE dws_advertiser_di;

USE CATALOG paimon;
USE ad_dw;
SHOW TABLES;
DESCRIBE ads_offline_metric_di;
DESCRIBE ads_order_attribution_di;

SELECT dt,COUNT(*) AS advertiser_rows,SUM(pay_order_gmv) AS total_gmv,
  SUM(short_video_pay_order_gmv+live_pay_order_gmv+image_text_pay_order_gmv+other_ad_type_pay_order_gmv) AS ad_type_gmv,
  SUM(search_pay_order_gmv+splash_pay_order_gmv+feed_pay_order_gmv+rewarded_pay_order_gmv+banner_pay_order_gmv+other_placement_pay_order_gmv) AS placement_gmv
FROM dws_advertiser_di
GROUP BY dt
ORDER BY dt DESC;

SELECT COUNT(*) AS dirty_event_count FROM dwd_ad_event_dirty_di;
