-- Preserve every order and enrich only orders having a click in the six hours before payment.
-- Multiple valid clicks use Last Click.
SET 'execution.runtime-mode'='streaming';
SET 'execution.checkpointing.interval'='30s';
SET 'pipeline.name'='fluss-order-direct-attribution-6h';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'table.dynamic-table-options.enabled'='true';
SET 'table.exec.state.ttl'='370 min';
SET 'table.exec.sink.upsert-materialize'='NONE';
CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');

CREATE TEMPORARY VIEW valid_click AS
SELECT uid,product_id,creative_id,slot_id,unit_id,campaign_id,advertiser_id,ts AS click_ts
FROM fluss.ad_dw.dwd_ad_event_di /*+ OPTIONS('scan.startup.mode'='earliest') */
WHERE event_type='click' AND uid IS NOT NULL AND product_id IS NOT NULL;

CREATE TEMPORARY VIEW ranked_order_attribution AS
SELECT *,ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY click_ts DESC) AS rn
FROM (
  SELECT o.*,c.creative_id AS attributed_creative_id,c.slot_id AS attributed_slot_id,
    c.unit_id AS attributed_unit_id,c.campaign_id AS attributed_campaign_id,
    c.advertiser_id AS attributed_advertiser_id,c.click_ts
  FROM fluss.ad_dw.ods_mysql_order_acc /*+ OPTIONS('scan.startup.mode'='earliest') */ o
  LEFT JOIN valid_click c
    ON o.user_id=c.uid AND o.product_id=c.product_id
    AND c.click_ts<UNIX_TIMESTAMP(o.pay_time)*1000
    AND c.click_ts>=UNIX_TIMESTAMP(o.pay_time)*1000-21600000
);

INSERT INTO fluss.ad_dw.dwd_ad_order_acc
SELECT order_id,user_id,product_id,shop_id,attributed_creative_id,attributed_slot_id,
  product_price,product_num,total_amount,payment_method,receiver_name,receiver_phone,
  shipping_address,tracking_number,order_status,create_time,cancel_time,pay_time,
  confirm_time,refund_time,updated_at,attributed_advertiser_id,attributed_campaign_id,
  attributed_unit_id,
  TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(COALESCE(refund_time,confirm_time,pay_time,
    cancel_time,create_time))*1000,3),dt,SUBSTRING(create_time,12,2)
FROM ranked_order_attribution WHERE rn=1;
