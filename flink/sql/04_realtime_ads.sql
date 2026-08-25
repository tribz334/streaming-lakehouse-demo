-- Materialize the latest Creative daily-full state observed in each 30-second processing window.
SET 'execution.runtime-mode'='streaming';
SET 'execution.checkpointing.interval'='30s';
SET 'pipeline.name'='fluss-dws-creative-di-to-ads-realtime-30s';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'table.dynamic-table-options.enabled'='true';
SET 'table.exec.sink.upsert-materialize'='NONE';
CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');

CREATE TEMPORARY VIEW creative_state_change AS
SELECT d.dt,cp.advertiser_id,u.campaign_id,c.unit_id,d.creative_id,
  d.delivery_count,d.impression_count,d.click_count,d.conversion_count,
  d.cost,d.closed_cost,d.pay_order_count,d.refund_order_count,
  d.pay_order_gmv,d.refund_order_gmv,
  d.ecommerce_pay_order_count,d.ecommerce_refund_order_count,
  d.ecommerce_pay_order_gmv,d.ecommerce_refund_order_gmv,
  d.short_video_pay_order_count,d.short_video_refund_order_count,
  d.short_video_pay_order_gmv,d.short_video_refund_order_gmv,
  d.live_pay_order_count,d.live_refund_order_count,
  d.live_pay_order_gmv,d.live_refund_order_gmv,
  PROCTIME() AS proc_time
FROM fluss.ad_dw.dws_creative_di /*+ OPTIONS('scan.startup.mode'='earliest') */ d
LEFT JOIN fluss.ad_dw.dim_creative_df c ON d.creative_id=c.creative_id
LEFT JOIN fluss.ad_dw.dim_unit_df u ON c.unit_id=u.unit_id
LEFT JOIN fluss.ad_dw.dim_campaign_df cp ON u.campaign_id=cp.campaign_id;

INSERT INTO fluss.ad_dw.ads_realtime_metric_30s
SELECT window_start,window_end,advertiser_id,campaign_id,unit_id,creative_id,
  MAX(delivery_count),MAX(impression_count),MAX(click_count),MAX(conversion_count),
  MAX(cost),MAX(closed_cost),MAX(pay_order_count),MAX(refund_order_count),
  MAX(pay_order_gmv),MAX(refund_order_gmv),
  MAX(ecommerce_pay_order_count),MAX(ecommerce_refund_order_count),
  MAX(ecommerce_pay_order_gmv),MAX(ecommerce_refund_order_gmv),
  MAX(short_video_pay_order_count),MAX(short_video_refund_order_count),
  MAX(short_video_pay_order_gmv),MAX(short_video_refund_order_gmv),
  MAX(live_pay_order_count),MAX(live_refund_order_count),
  MAX(live_pay_order_gmv),MAX(live_refund_order_gmv),dt
FROM TABLE(
  TUMBLE(TABLE creative_state_change,DESCRIPTOR(proc_time),INTERVAL '30' SECOND)
)
GROUP BY window_start,window_end,dt,advertiser_id,campaign_id,unit_id,creative_id;
