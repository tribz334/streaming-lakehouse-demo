-- Rebuild one business date of daily DWS topics from Paimon history.
SET 'execution.runtime-mode'='batch';
SET 'table.dml-sync'='true';
SET 'pipeline.name'='paimon-daily-dws-__BIZ_DATE__';
SET 'table.local-time-zone'='Asia/Shanghai';
CREATE CATALOG paimon WITH ('type'='paimon','metastore'='filesystem','warehouse'='file:///warehouse/paimon');

CREATE TEMPORARY VIEW ad_metric_change AS
SELECT CONCAT(SUBSTRING(e.dt,1,4),'-',SUBSTRING(e.dt,5,2),'-',SUBSTRING(e.dt,7,2)) AS dt,
  cp.advertiser_id,u.campaign_id,c.unit_id,e.creative_id,u.placement_type,u.ad_type,
  CAST(CASE WHEN e.event_type='delivery' THEN 1 ELSE 0 END AS BIGINT) delivery_count,
  CAST(CASE WHEN e.event_type='show' THEN 1 ELSE 0 END AS BIGINT) impression_count,
  CAST(CASE WHEN e.event_type='click' THEN 1 ELSE 0 END AS BIGINT) click_count,
  CAST(CASE WHEN e.event_type='convert' THEN 1 ELSE 0 END AS BIGINT) conversion_count,
  CAST(0 AS BIGINT) cost,CAST(0 AS BIGINT) closed_cost,CAST(0 AS BIGINT) pay_order_count,
  CAST(0 AS BIGINT) refund_order_count,CAST(0 AS BIGINT) pay_order_gmv,CAST(0 AS BIGINT) refund_order_gmv
FROM paimon.ad_dw.dwd_ad_event_di e
JOIN paimon.ad_dw.dim_creative_df c ON e.creative_id=c.creative_id
JOIN paimon.ad_dw.dim_unit_df u ON c.unit_id=u.unit_id
JOIN paimon.ad_dw.dim_campaign_df cp ON u.campaign_id=cp.campaign_id
WHERE e.dt='__BIZ_DATE_COMPACT__'
UNION ALL
SELECT CONCAT(SUBSTRING(b.dt,1,4),'-',SUBSTRING(b.dt,5,2),'-',SUBSTRING(b.dt,7,2)),
  b.advertiser_id,b.campaign_id,b.unit_id,b.creative_id,u.placement_type,u.ad_type,0,0,0,0,b.cost,
  CASE WHEN b.is_closed=1 THEN b.cost ELSE CAST(0 AS BIGINT) END,0,0,0,0
FROM paimon.ad_dw.dwd_ad_bill_di b
JOIN paimon.ad_dw.dim_unit_df u ON b.unit_id=u.unit_id
WHERE b.dt='__BIZ_DATE_COMPACT__'
UNION ALL
SELECT DATE_FORMAT(TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(o.pay_time)*1000,3),'yyyy-MM-dd'),
  o.advertiser_id,o.campaign_id,o.unit_id,o.creative_id,o.placement_type,o.ad_type,
  0,0,0,0,0,0,CAST(1 AS BIGINT),0,o.total_amount,0
FROM paimon.ad_dw.dwd_order_acc o
WHERE o.pay_time IS NOT NULL
  AND REPLACE(SUBSTRING(o.pay_time,1,10),'-','')='__BIZ_DATE_COMPACT__'
UNION ALL
SELECT DATE_FORMAT(TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(o.refund_time)*1000,3),'yyyy-MM-dd'),
  o.advertiser_id,o.campaign_id,o.unit_id,o.creative_id,o.placement_type,o.ad_type,
  0,0,0,0,0,0,0,CAST(1 AS BIGINT),0,o.total_amount
FROM paimon.ad_dw.dwd_order_acc o
WHERE o.refund_time IS NOT NULL
  AND REPLACE(SUBSTRING(o.refund_time,1,10),'-','')='__BIZ_DATE_COMPACT__';

EXECUTE STATEMENT SET
BEGIN
  INSERT OVERWRITE paimon.ad_dw.dws_ad_advertiser_di PARTITION (dt='__BIZ_DATE__')
  SELECT m.advertiser_id,a.advertiser_name,
    SUM(m.delivery_count),
    SUM(m.impression_count),
    SUM(m.click_count),
    SUM(m.conversion_count),
    SUM(m.cost),
    SUM(m.closed_cost),
    SUM(m.pay_order_count),
    SUM(m.refund_order_count),
    SUM(m.pay_order_gmv),
    SUM(m.refund_order_gmv),
    SUM(CASE WHEN m.ad_type=1 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS short_video_pay_order_count,
    SUM(CASE WHEN m.ad_type=1 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS short_video_refund_order_count,
    SUM(CASE WHEN m.ad_type=1 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS short_video_pay_order_gmv,
    SUM(CASE WHEN m.ad_type=1 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS short_video_refund_order_gmv,
    SUM(CASE WHEN m.ad_type=2 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS live_pay_order_count,
    SUM(CASE WHEN m.ad_type=2 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS live_refund_order_count,
    SUM(CASE WHEN m.ad_type=2 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS live_pay_order_gmv,
    SUM(CASE WHEN m.ad_type=2 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS live_refund_order_gmv,
    SUM(CASE WHEN m.ad_type=3 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS image_text_pay_order_count,
    SUM(CASE WHEN m.ad_type=3 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS image_text_refund_order_count,
    SUM(CASE WHEN m.ad_type=3 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS image_text_pay_order_gmv,
    SUM(CASE WHEN m.ad_type=3 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS image_text_refund_order_gmv,
    SUM(CASE WHEN m.ad_type=4 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS other_ad_type_pay_order_count,
    SUM(CASE WHEN m.ad_type=4 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS other_ad_type_refund_order_count,
    SUM(CASE WHEN m.ad_type=4 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS other_ad_type_pay_order_gmv,
    SUM(CASE WHEN m.ad_type=4 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS other_ad_type_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=2 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS search_pay_order_count,
    SUM(CASE WHEN m.placement_type=2 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS search_refund_order_count,
    SUM(CASE WHEN m.placement_type=2 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS search_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=2 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS search_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=3 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS splash_pay_order_count,
    SUM(CASE WHEN m.placement_type=3 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS splash_refund_order_count,
    SUM(CASE WHEN m.placement_type=3 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS splash_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=3 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS splash_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=1 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS feed_pay_order_count,
    SUM(CASE WHEN m.placement_type=1 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS feed_refund_order_count,
    SUM(CASE WHEN m.placement_type=1 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS feed_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=1 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS feed_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=4 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS rewarded_pay_order_count,
    SUM(CASE WHEN m.placement_type=4 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS rewarded_refund_order_count,
    SUM(CASE WHEN m.placement_type=4 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS rewarded_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=4 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS rewarded_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=5 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS banner_pay_order_count,
    SUM(CASE WHEN m.placement_type=5 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS banner_refund_order_count,
    SUM(CASE WHEN m.placement_type=5 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS banner_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=5 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS banner_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=6 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS other_placement_pay_order_count,
    SUM(CASE WHEN m.placement_type=6 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS other_placement_refund_order_count,
    SUM(CASE WHEN m.placement_type=6 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS other_placement_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=6 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS other_placement_refund_order_gmv
  FROM ad_metric_change m
  LEFT JOIN paimon.ad_dw.dim_advertiser_df a ON m.advertiser_id=a.advertiser_id
  WHERE m.advertiser_id IS NOT NULL
  GROUP BY m.dt,m.advertiser_id,a.advertiser_name;

  INSERT OVERWRITE paimon.ad_dw.dws_ad_campaign_di PARTITION (dt='__BIZ_DATE__')
  SELECT m.campaign_id,c.campaign_name,
    SUM(m.delivery_count),
    SUM(m.impression_count),
    SUM(m.click_count),
    SUM(m.conversion_count),
    SUM(m.cost),
    SUM(m.closed_cost),
    SUM(m.pay_order_count),
    SUM(m.refund_order_count),
    SUM(m.pay_order_gmv),
    SUM(m.refund_order_gmv),
    SUM(CASE WHEN m.ad_type=1 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS short_video_pay_order_count,
    SUM(CASE WHEN m.ad_type=1 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS short_video_refund_order_count,
    SUM(CASE WHEN m.ad_type=1 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS short_video_pay_order_gmv,
    SUM(CASE WHEN m.ad_type=1 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS short_video_refund_order_gmv,
    SUM(CASE WHEN m.ad_type=2 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS live_pay_order_count,
    SUM(CASE WHEN m.ad_type=2 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS live_refund_order_count,
    SUM(CASE WHEN m.ad_type=2 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS live_pay_order_gmv,
    SUM(CASE WHEN m.ad_type=2 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS live_refund_order_gmv,
    SUM(CASE WHEN m.ad_type=3 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS image_text_pay_order_count,
    SUM(CASE WHEN m.ad_type=3 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS image_text_refund_order_count,
    SUM(CASE WHEN m.ad_type=3 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS image_text_pay_order_gmv,
    SUM(CASE WHEN m.ad_type=3 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS image_text_refund_order_gmv,
    SUM(CASE WHEN m.ad_type=4 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS other_ad_type_pay_order_count,
    SUM(CASE WHEN m.ad_type=4 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS other_ad_type_refund_order_count,
    SUM(CASE WHEN m.ad_type=4 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS other_ad_type_pay_order_gmv,
    SUM(CASE WHEN m.ad_type=4 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS other_ad_type_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=2 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS search_pay_order_count,
    SUM(CASE WHEN m.placement_type=2 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS search_refund_order_count,
    SUM(CASE WHEN m.placement_type=2 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS search_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=2 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS search_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=3 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS splash_pay_order_count,
    SUM(CASE WHEN m.placement_type=3 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS splash_refund_order_count,
    SUM(CASE WHEN m.placement_type=3 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS splash_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=3 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS splash_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=1 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS feed_pay_order_count,
    SUM(CASE WHEN m.placement_type=1 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS feed_refund_order_count,
    SUM(CASE WHEN m.placement_type=1 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS feed_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=1 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS feed_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=4 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS rewarded_pay_order_count,
    SUM(CASE WHEN m.placement_type=4 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS rewarded_refund_order_count,
    SUM(CASE WHEN m.placement_type=4 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS rewarded_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=4 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS rewarded_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=5 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS banner_pay_order_count,
    SUM(CASE WHEN m.placement_type=5 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS banner_refund_order_count,
    SUM(CASE WHEN m.placement_type=5 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS banner_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=5 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS banner_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=6 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS other_placement_pay_order_count,
    SUM(CASE WHEN m.placement_type=6 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS other_placement_refund_order_count,
    SUM(CASE WHEN m.placement_type=6 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS other_placement_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=6 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS other_placement_refund_order_gmv
  FROM ad_metric_change m
  LEFT JOIN paimon.ad_dw.dim_campaign_df c ON m.campaign_id=c.campaign_id
  WHERE m.campaign_id IS NOT NULL
  GROUP BY m.dt,m.campaign_id,c.campaign_name;

  INSERT OVERWRITE paimon.ad_dw.dws_ad_unit_di PARTITION (dt='__BIZ_DATE__')
  SELECT m.unit_id,u.unit_name,m.placement_type,m.ad_type,
    SUM(m.delivery_count),
    SUM(m.impression_count),
    SUM(m.click_count),
    SUM(m.conversion_count),
    SUM(m.cost),
    SUM(m.closed_cost),
    SUM(m.pay_order_count),
    SUM(m.refund_order_count),
    SUM(m.pay_order_gmv),
    SUM(m.refund_order_gmv)
  FROM ad_metric_change m
  LEFT JOIN paimon.ad_dw.dim_unit_df u ON m.unit_id=u.unit_id
  WHERE m.unit_id IS NOT NULL AND m.placement_type BETWEEN 1 AND 6 AND m.ad_type BETWEEN 1 AND 4
  GROUP BY m.dt,m.unit_id,u.unit_name,m.placement_type,m.ad_type;

  INSERT OVERWRITE paimon.ad_dw.dws_ad_creative_di PARTITION (dt='__BIZ_DATE__')
  SELECT m.creative_id,c.creative_name,
    SUM(m.delivery_count),
    SUM(m.impression_count),
    SUM(m.click_count),
    SUM(m.conversion_count),
    SUM(m.cost),
    SUM(m.closed_cost),
    SUM(m.pay_order_count),
    SUM(m.refund_order_count),
    SUM(m.pay_order_gmv),
    SUM(m.refund_order_gmv)
  FROM ad_metric_change m
  LEFT JOIN paimon.ad_dw.dim_creative_df c ON m.creative_id=c.creative_id
  WHERE m.creative_id IS NOT NULL
  GROUP BY m.dt,m.creative_id,c.creative_name;
END;
