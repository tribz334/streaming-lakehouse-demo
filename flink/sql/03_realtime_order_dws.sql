-- Three daily topic tables. Each sink keeps only its own id, name and additive metrics.
SET 'execution.runtime-mode'='streaming';
SET 'execution.checkpointing.interval'='30s';
SET 'pipeline.name'='fluss-dwd-to-three-dws-topic-di';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'table.dynamic-table-options.enabled'='true';
SET 'table.exec.sink.upsert-materialize'='NONE';
CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');

CREATE TEMPORARY VIEW ad_metric_change AS
SELECT dt,advertiser_id,campaign_id,unit_id,creative_id,
  CAST(CASE WHEN event_type='delivery' THEN 1 ELSE 0 END AS BIGINT) delivery_count,
  CAST(CASE WHEN event_type='impression' THEN 1 ELSE 0 END AS BIGINT) impression_count,
  CAST(CASE WHEN event_type='click' THEN 1 ELSE 0 END AS BIGINT) click_count,
  CAST(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END AS BIGINT) conversion_count,
  CAST(0 AS BIGINT) cost,CAST(0 AS BIGINT) closed_cost,
  CAST(0 AS BIGINT) pay_order_count,CAST(0 AS BIGINT) refund_order_count,
  CAST(0 AS BIGINT) pay_order_gmv,CAST(0 AS BIGINT) refund_order_gmv
FROM fluss.ad_dw.dwd_ad_event_di /*+ OPTIONS('scan.startup.mode'='earliest') */
UNION ALL
SELECT dt,advertiser_id,campaign_id,unit_id,creative_id,0,0,0,0,cost,
  CASE WHEN is_closed=1 THEN cost ELSE CAST(0 AS BIGINT) END,0,0,0,0
FROM fluss.ad_dw.dwd_ad_bill_di /*+ OPTIONS('scan.startup.mode'='earliest') */
UNION ALL
SELECT dt,advertiser_id,campaign_id,unit_id,creative_id,0,0,0,0,0,0,
  CASE WHEN pay_time IS NOT NULL THEN CAST(1 AS BIGINT) ELSE 0 END,
  CASE WHEN refund_time IS NOT NULL THEN CAST(1 AS BIGINT) ELSE 0 END,
  CASE WHEN pay_time IS NOT NULL THEN total_amount ELSE CAST(0 AS BIGINT) END,
  CASE WHEN refund_time IS NOT NULL THEN total_amount ELSE CAST(0 AS BIGINT) END
FROM fluss.ad_dw.dwd_ad_order_acc /*+ OPTIONS('scan.startup.mode'='earliest') */
WHERE creative_id IS NOT NULL;

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO fluss.ad_dw.dws_advertiser_di
  SELECT m.dt,m.advertiser_id,a.advertiser_name,
    SUM(delivery_count),SUM(impression_count),SUM(click_count),SUM(conversion_count),
    SUM(cost),SUM(closed_cost),SUM(pay_order_count),SUM(refund_order_count),
    SUM(pay_order_gmv),SUM(refund_order_gmv),
    SUM(CASE WHEN u.delivery_type=1 THEN pay_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=1 THEN refund_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=1 THEN pay_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=1 THEN refund_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN pay_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN refund_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN pay_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN refund_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN pay_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN refund_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN pay_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN refund_order_gmv ELSE 0 END)
  FROM ad_metric_change m
  LEFT JOIN fluss.ad_dw.dim_advertiser_df a ON m.advertiser_id=a.advertiser_id
  LEFT JOIN fluss.ad_dw.dim_unit_df u ON m.unit_id=u.unit_id
  WHERE m.advertiser_id IS NOT NULL
  GROUP BY m.dt,m.advertiser_id,a.advertiser_name;

  INSERT INTO fluss.ad_dw.dws_unit_di
  SELECT m.dt,m.unit_id,u.unit_name,
    SUM(delivery_count),SUM(impression_count),SUM(click_count),SUM(conversion_count),
    SUM(cost),SUM(closed_cost),SUM(pay_order_count),SUM(refund_order_count),
    SUM(pay_order_gmv),SUM(refund_order_gmv),
    SUM(CASE WHEN u.delivery_type=1 THEN pay_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=1 THEN refund_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=1 THEN pay_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=1 THEN refund_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN pay_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN refund_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN pay_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN refund_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN pay_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN refund_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN pay_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN refund_order_gmv ELSE 0 END)
  FROM ad_metric_change m
  LEFT JOIN fluss.ad_dw.dim_unit_df u ON m.unit_id=u.unit_id
  WHERE m.unit_id IS NOT NULL
  GROUP BY m.dt,m.unit_id,u.unit_name;

  INSERT INTO fluss.ad_dw.dws_creative_di
  SELECT m.dt,m.creative_id,c.creative_name,
    SUM(delivery_count),SUM(impression_count),SUM(click_count),SUM(conversion_count),
    SUM(cost),SUM(closed_cost),SUM(pay_order_count),SUM(refund_order_count),
    SUM(pay_order_gmv),SUM(refund_order_gmv),
    SUM(CASE WHEN u.delivery_type=1 THEN pay_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=1 THEN refund_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=1 THEN pay_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=1 THEN refund_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN pay_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN refund_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN pay_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=2 THEN refund_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN pay_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN refund_order_count ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN pay_order_gmv ELSE 0 END),
    SUM(CASE WHEN u.delivery_type=3 THEN refund_order_gmv ELSE 0 END)
  FROM ad_metric_change m
  LEFT JOIN fluss.ad_dw.dim_creative_df c ON m.creative_id=c.creative_id
  LEFT JOIN fluss.ad_dw.dim_unit_df u ON m.unit_id=u.unit_id
  WHERE m.creative_id IS NOT NULL
  GROUP BY m.dt,m.creative_id,c.creative_name;
END;
