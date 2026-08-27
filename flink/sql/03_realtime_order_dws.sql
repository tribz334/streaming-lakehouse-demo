-- Realtime daily topic aggregation. Classification is enriched once in DWD and reused here.
SET 'execution.runtime-mode'='streaming';
SET 'execution.checkpointing.interval'='30s';
SET 'pipeline.name'='fluss-dwd-to-dws-topic-di';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'table.dynamic-table-options.enabled'='true';
SET 'table.exec.sink.upsert-materialize'='NONE';
CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');

CREATE TEMPORARY VIEW ad_metric_change AS
SELECT dt,advertiser_id,campaign_id,unit_id,creative_id,placement_type,ad_type,
  CAST(CASE WHEN event_type='delivery' THEN 1 ELSE 0 END AS BIGINT) delivery_count,
  CAST(CASE WHEN event_type='impression' THEN 1 ELSE 0 END AS BIGINT) impression_count,
  CAST(CASE WHEN event_type='click' THEN 1 ELSE 0 END AS BIGINT) click_count,
  CAST(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END AS BIGINT) conversion_count,
  CAST(0 AS BIGINT) cost,CAST(0 AS BIGINT) closed_cost,CAST(0 AS BIGINT) pay_order_count,
  CAST(0 AS BIGINT) refund_order_count,CAST(0 AS BIGINT) pay_order_gmv,CAST(0 AS BIGINT) refund_order_gmv
FROM fluss.ad_dw.dwd_ad_event_di /*+ OPTIONS('scan.startup.mode'='earliest') */
UNION ALL
SELECT dt,advertiser_id,campaign_id,unit_id,creative_id,placement_type,ad_type,0,0,0,0,cost,
  CASE WHEN is_closed=1 THEN cost ELSE CAST(0 AS BIGINT) END,0,0,0,0
FROM fluss.ad_dw.dwd_ad_bill_di /*+ OPTIONS('scan.startup.mode'='earliest') */
UNION ALL
SELECT dt,advertiser_id,campaign_id,unit_id,creative_id,placement_type,ad_type,0,0,0,0,0,0,
  CASE WHEN pay_time IS NOT NULL THEN CAST(1 AS BIGINT) ELSE 0 END,
  CASE WHEN refund_time IS NOT NULL THEN CAST(1 AS BIGINT) ELSE 0 END,
  CASE WHEN pay_time IS NOT NULL THEN total_amount ELSE CAST(0 AS BIGINT) END,
  CASE WHEN refund_time IS NOT NULL THEN total_amount ELSE CAST(0 AS BIGINT) END
FROM fluss.ad_dw.dwd_ad_order_acc /*+ OPTIONS('scan.startup.mode'='earliest') */
WHERE creative_id IS NOT NULL AND placement_type BETWEEN 1 AND 6 AND ad_type BETWEEN 1 AND 4;

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO fluss.ad_dw.dws_advertiser_di
  SELECT m.dt,m.advertiser_id,a.advertiser_name,
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
    SUM(CASE WHEN m.placement_type=1 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS search_pay_order_count,
    SUM(CASE WHEN m.placement_type=1 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS search_refund_order_count,
    SUM(CASE WHEN m.placement_type=1 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS search_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=1 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS search_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=2 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS splash_pay_order_count,
    SUM(CASE WHEN m.placement_type=2 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS splash_refund_order_count,
    SUM(CASE WHEN m.placement_type=2 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS splash_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=2 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS splash_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=3 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS feed_pay_order_count,
    SUM(CASE WHEN m.placement_type=3 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS feed_refund_order_count,
    SUM(CASE WHEN m.placement_type=3 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS feed_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=3 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS feed_refund_order_gmv,
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
  LEFT JOIN fluss.ad_dw.dim_advertiser_df a ON m.advertiser_id=a.advertiser_id
  WHERE m.advertiser_id IS NOT NULL
  GROUP BY m.dt,m.advertiser_id,a.advertiser_name;

  INSERT INTO fluss.ad_dw.dws_campaign_di
  SELECT m.dt,m.campaign_id,c.campaign_name,
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
    SUM(CASE WHEN m.placement_type=1 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS search_pay_order_count,
    SUM(CASE WHEN m.placement_type=1 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS search_refund_order_count,
    SUM(CASE WHEN m.placement_type=1 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS search_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=1 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS search_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=2 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS splash_pay_order_count,
    SUM(CASE WHEN m.placement_type=2 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS splash_refund_order_count,
    SUM(CASE WHEN m.placement_type=2 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS splash_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=2 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS splash_refund_order_gmv,
    SUM(CASE WHEN m.placement_type=3 THEN m.pay_order_count ELSE CAST(0 AS BIGINT) END) AS feed_pay_order_count,
    SUM(CASE WHEN m.placement_type=3 THEN m.refund_order_count ELSE CAST(0 AS BIGINT) END) AS feed_refund_order_count,
    SUM(CASE WHEN m.placement_type=3 THEN m.pay_order_gmv ELSE CAST(0 AS BIGINT) END) AS feed_pay_order_gmv,
    SUM(CASE WHEN m.placement_type=3 THEN m.refund_order_gmv ELSE CAST(0 AS BIGINT) END) AS feed_refund_order_gmv,
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
  LEFT JOIN fluss.ad_dw.dim_campaign_df c ON m.campaign_id=c.campaign_id
  WHERE m.campaign_id IS NOT NULL
  GROUP BY m.dt,m.campaign_id,c.campaign_name;

  INSERT INTO fluss.ad_dw.dws_unit_di
  SELECT m.dt,m.unit_id,u.unit_name,m.placement_type,m.ad_type,
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
  LEFT JOIN fluss.ad_dw.dim_unit_df u ON m.unit_id=u.unit_id
  WHERE m.unit_id IS NOT NULL AND m.placement_type BETWEEN 1 AND 6 AND m.ad_type BETWEEN 1 AND 4
  GROUP BY m.dt,m.unit_id,u.unit_name,m.placement_type,m.ad_type;

  INSERT INTO fluss.ad_dw.dws_creative_di
  SELECT m.dt,m.creative_id,c.creative_name,
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
  LEFT JOIN fluss.ad_dw.dim_creative_df c ON m.creative_id=c.creative_id
  WHERE m.creative_id IS NOT NULL
  GROUP BY m.dt,m.creative_id,c.creative_name;
END;
