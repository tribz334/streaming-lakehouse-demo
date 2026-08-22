-- Continuous accumulating order fact in thousandths of a cent.
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '32 d';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'dwd-order-lifecycle-acc';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://hive-metastore:9083',
  'warehouse' = 'file:///warehouse/paimon'
);

CREATE TEMPORARY TABLE mysql_order_detail (
  order_id BIGINT,user_id BIGINT,product_id BIGINT,shop_id BIGINT,
  product_price BIGINT,product_num INT,total_amount BIGINT,payment_method INT,
  receiver_name STRING,receiver_phone STRING,shipping_address STRING,
  tracking_number STRING,order_status INT,create_time TIMESTAMP(3),
  cancel_time TIMESTAMP(3),payment_time TIMESTAMP(3),confirm_time TIMESTAMP(3),
  refund_time TIMESTAMP(3),refund_finish_time TIMESTAMP(3),finish_time TIMESTAMP(3),
  updated_at TIMESTAMP(3),PRIMARY KEY(order_id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root',
  'password'='root','database-name'='ad_ods','table-name'='order_detail',
  'server-id'='5531-5538','server-time-zone'='UTC','scan.startup.mode'='initial'
);

-- The Java realtime job publishes the finalized Last Click result here.
-- Join it directly with the MySQL lifecycle stream; no extra Paimon
-- extra Paimon attribution intermediate table is required.
CREATE TEMPORARY TABLE kafka_attributed_order (
  order_id BIGINT,
  creative_id BIGINT,
  slot_id BIGINT,
  attribution_status STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_order_detail',
  'properties.bootstrap.servers' = 'kafka-node-1:9092',
  'properties.group.id' = 'dwd-order-lifecycle-acc-v2',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

INSERT INTO paimon.ad_dw.dwd_order_detail_acc (
  order_id, uid, product_id, shop_id, creative_id, slot_id,
  unit_price, order_num, total_amount, payment_method,
  receiver_name, receiver_phone, shipping_address, tracking_number,
  order_status, create_time, cancel_time, payment_time, confirm_time,
  refund_time, refund_finish_time, finish_time, update_time, dt, `hour`
)
SELECT
  o.order_id,
  o.user_id,
  o.product_id,
  o.shop_id,
  CASE WHEN a.attribution_status = 'attributed' THEN a.creative_id ELSE CAST(NULL AS BIGINT) END,
  CASE WHEN a.attribution_status = 'attributed' THEN a.slot_id ELSE CAST(NULL AS BIGINT) END,
  o.product_price,
  o.product_num,
  o.total_amount,
  o.payment_method,
  o.receiver_name,
  o.receiver_phone,
  o.shipping_address,
  o.tracking_number,
  o.order_status,
  CAST(o.create_time AS STRING),
  CAST(o.cancel_time AS STRING),
  CAST(o.payment_time AS STRING),
  CAST(o.confirm_time AS STRING),
  CAST(o.refund_time AS STRING),
  CAST(o.refund_finish_time AS STRING),
  CAST(o.finish_time AS STRING),
  CAST(o.updated_at AS STRING),
  DATE_FORMAT(o.updated_at,'yyyy-MM-dd'),
  DATE_FORMAT(o.updated_at,'HH')
FROM mysql_order_detail o
LEFT JOIN kafka_attributed_order a
  ON o.order_id = a.order_id
 AND a.attribution_status IN ('attributed', 'organic');
