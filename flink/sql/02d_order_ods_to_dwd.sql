-- Build the paid-order DWD source used by the DataStream LastClick job.
-- This SQL only normalizes the ODS changelog; attribution is not performed here.
SET 'execution.runtime-mode'='streaming';
SET 'execution.checkpointing.interval'='30s';
SET 'pipeline.name'='fluss-order-ods-to-dwd-ad-order-di';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'table.dynamic-table-options.enabled'='true';
SET 'table.exec.sink.upsert-materialize'='NONE';
CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');

INSERT INTO fluss.ad_dw.dwd_ad_order_di
SELECT CONCAT(CAST(order_id AS STRING),'-pay'),order_id,user_id,product_id,'PAY',pay_time,
  total_amount,shop_id,product_price,product_num,total_amount,payment_method,receiver_name,
  receiver_phone,shipping_address,tracking_number,order_status,create_time,cancel_time,
  confirm_time,refund_time,updated_at,SUBSTRING(pay_time,1,10)
FROM fluss.ad_dw.ods_mysql_order_acc /*+ OPTIONS('scan.startup.mode'='earliest') */
WHERE pay_time IS NOT NULL;
