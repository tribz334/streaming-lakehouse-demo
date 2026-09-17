SET 'execution.runtime-mode'='batch';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'sql-client.execution.result-mode'='TABLEAU';
CREATE CATALOG paimon WITH ('type'='paimon','metastore'='filesystem','warehouse'='file:///warehouse/paimon');
USE CATALOG paimon;
USE ad_dw;

SELECT COUNT(*) AS raw_event_count FROM ods_log;
SELECT COUNT(*) AS enriched_event_count,
  SUM(CASE WHEN u.placement_type BETWEEN 1 AND 6 AND u.ad_type BETWEEN 1 AND 4 THEN 1 ELSE 0 END) AS classified_event_count
FROM dwd_ad_event_di e
LEFT JOIN dim_creative_df c ON e.creative_id=c.creative_id
LEFT JOIN dim_unit_df u ON c.unit_id=u.unit_id;
SELECT COUNT(*) AS enriched_bill_count,
  SUM(CASE WHEN u.placement_type BETWEEN 1 AND 6 AND u.ad_type BETWEEN 1 AND 4 THEN 1 ELSE 0 END) AS classified_bill_count
FROM dwd_ad_bill_di b
LEFT JOIN dim_unit_df u ON b.unit_id=u.unit_id;
-- Dirty events are emitted through the Print connector and are not persisted as DWD tables.
SELECT dt,COUNT(*) AS advertiser_rows,SUM(pay_order_gmv) AS total_gmv,
  SUM(short_video_pay_order_gmv+live_pay_order_gmv+image_text_pay_order_gmv+other_ad_type_pay_order_gmv) AS ad_type_gmv,
  SUM(search_pay_order_gmv+splash_pay_order_gmv+feed_pay_order_gmv+rewarded_pay_order_gmv+banner_pay_order_gmv+other_placement_pay_order_gmv) AS placement_gmv
FROM dws_ad_advertiser_di
GROUP BY dt
ORDER BY dt DESC;
