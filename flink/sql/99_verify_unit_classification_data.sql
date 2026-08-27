SET 'execution.runtime-mode'='batch';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'sql-client.execution.result-mode'='TABLEAU';
CREATE CATALOG paimon WITH ('type'='paimon','metastore'='filesystem','warehouse'='file:///warehouse/paimon');
USE CATALOG paimon;
USE ad_dw;

SELECT COUNT(*) AS raw_event_count FROM ods_log_di;
SELECT COUNT(*) AS enriched_event_count,
  SUM(CASE WHEN placement_type BETWEEN 1 AND 6 AND ad_type BETWEEN 1 AND 4 THEN 1 ELSE 0 END) AS classified_event_count
FROM dwd_ad_event_di;
SELECT COUNT(*) AS enriched_bill_count,
  SUM(CASE WHEN placement_type BETWEEN 1 AND 6 AND ad_type BETWEEN 1 AND 4 THEN 1 ELSE 0 END) AS classified_bill_count
FROM dwd_ad_bill_di;
SELECT COUNT(*) AS dirty_event_count FROM dwd_ad_event_dirty_di;
SELECT dt,COUNT(*) AS advertiser_rows,SUM(pay_order_gmv) AS total_gmv,
  SUM(short_video_pay_order_gmv+live_pay_order_gmv+image_text_pay_order_gmv+other_ad_type_pay_order_gmv) AS ad_type_gmv,
  SUM(search_pay_order_gmv+splash_pay_order_gmv+feed_pay_order_gmv+rewarded_pay_order_gmv+banner_pay_order_gmv+other_placement_pay_order_gmv) AS placement_gmv
FROM dws_advertiser_di
GROUP BY dt
ORDER BY dt DESC;
