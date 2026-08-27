-- Expand the attributed accumulation table into classified PAY/REFUND facts.
SET 'execution.runtime-mode'='streaming';
SET 'execution.checkpointing.interval'='30s';
SET 'pipeline.name'='fluss-attributed-order-to-event-di';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'table.dynamic-table-options.enabled'='true';
SET 'table.exec.sink.upsert-materialize'='NONE';
CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');

CREATE TEMPORARY VIEW order_changes AS
SELECT `before` AS row_before,`after` AS row_after
FROM fluss.ad_dw.`dwd_ad_order_acc$binlog`
  /*+ OPTIONS('scan.startup.mode'='earliest') */;

INSERT INTO fluss.ad_dw.dwd_ad_order_event_di
SELECT CONCAT(CAST(row_after.order_id AS STRING),'-pay'),row_after.order_id,
  row_after.uid,row_after.product_id,'PAY',
  TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(row_after.pay_time)*1000,3),row_after.total_amount,
  row_after.advertiser_id,row_after.campaign_id,row_after.unit_id,row_after.creative_id,
  row_after.placement_type,row_after.ad_type,SUBSTRING(row_after.pay_time,1,10)
FROM order_changes
WHERE row_before.pay_time IS NULL AND row_after.pay_time IS NOT NULL
UNION ALL
SELECT CONCAT(CAST(row_after.order_id AS STRING),'-refund'),row_after.order_id,
  row_after.uid,row_after.product_id,'REFUND',
  TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(row_after.refund_time)*1000,3),row_after.total_amount,
  row_after.advertiser_id,row_after.campaign_id,row_after.unit_id,row_after.creative_id,
  row_after.placement_type,row_after.ad_type,SUBSTRING(row_after.refund_time,1,10)
FROM order_changes
WHERE row_before.refund_time IS NULL AND row_after.refund_time IS NOT NULL;
