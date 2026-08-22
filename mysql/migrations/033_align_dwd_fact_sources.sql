USE ad_ods;

DELIMITER $$
DROP PROCEDURE IF EXISTS migrate_dwd_fact_sources$$
CREATE PROCEDURE migrate_dwd_fact_sources()
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='ad_ods' AND TABLE_NAME='order_detail' AND COLUMN_NAME='payment_method') THEN
    ALTER TABLE order_detail
      ADD COLUMN payment_method INT NULL AFTER total_amount,
      ADD COLUMN receiver_name VARCHAR(128) NULL AFTER payment_method,
      ADD COLUMN receiver_phone VARCHAR(32) NULL AFTER receiver_name,
      ADD COLUMN shipping_address VARCHAR(512) NULL AFTER receiver_phone,
      ADD COLUMN tracking_number VARCHAR(128) NULL AFTER shipping_address;
  END IF;

  IF (SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='ad_ods' AND TABLE_NAME='order_detail' AND COLUMN_NAME='product_price') <> 'bigint' THEN
    UPDATE order_detail
    SET product_price = ROUND(product_price * 100000),
        total_amount = ROUND(total_amount * 100000);
    ALTER TABLE order_detail
      MODIFY COLUMN product_price BIGINT NOT NULL COMMENT '商品单价，单位千分之一分',
      MODIFY COLUMN total_amount BIGINT NOT NULL COMMENT '订单金额，单位千分之一分';
  END IF;

  IF (SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='ad_ods' AND TABLE_NAME='order_detail' AND COLUMN_NAME='order_status') <> 'int' THEN
    ALTER TABLE order_detail ADD COLUMN order_status_new INT NULL AFTER tracking_number;
    UPDATE order_detail SET order_status_new = CASE LOWER(order_status)
      WHEN 'created' THEN 1 WHEN 'cancelled' THEN 2 WHEN 'paid' THEN 3
      WHEN 'confirmed' THEN 4 WHEN 'refunding' THEN 5 WHEN 'refunded' THEN 6
      WHEN 'finished' THEN 7 WHEN 'completed' THEN 7 ELSE 1 END;
    ALTER TABLE order_detail DROP COLUMN order_status,
      CHANGE COLUMN order_status_new order_status INT NOT NULL;
  END IF;

  UPDATE order_detail
  SET payment_method = COALESCE(payment_method, 1 + MOD(order_id, 2)),
      receiver_name = COALESCE(receiver_name, CONCAT('收货人', MOD(user_id, 10000))),
      receiver_phone = COALESCE(receiver_phone, CONCAT('138', LPAD(MOD(user_id, 100000000), 8, '0'))),
      shipping_address = COALESCE(shipping_address, CONCAT('示例配送地址-', MOD(shop_id, 1000))),
      tracking_number = CASE WHEN order_status >= 4 THEN COALESCE(tracking_number, CONCAT('SF', order_id)) ELSE tracking_number END;

  IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='ad_ods' AND TABLE_NAME='bill_detail' AND COLUMN_NAME='slot_id') THEN
    ALTER TABLE bill_detail
      ADD COLUMN slot_id BIGINT NULL AFTER user_id,
      ADD COLUMN billing_type TINYINT NULL AFTER slot_id;
  END IF;
  UPDATE bill_detail b
  LEFT JOIN unit_info u ON b.unit_id=u.unit_id
  SET b.slot_id=COALESCE(b.slot_id, 100 + MOD(b.creative_id, 12)),
      b.billing_type=COALESCE(b.billing_type, CASE UPPER(u.bid_type) WHEN 'CPM' THEN 1 WHEN 'CPC' THEN 2 ELSE 3 END);
  ALTER TABLE bill_detail
    MODIFY COLUMN slot_id BIGINT NOT NULL,
    MODIFY COLUMN billing_type TINYINT NOT NULL COMMENT '1-曝光，2-点击，3-转化';

  IF (SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='ad_ods' AND TABLE_NAME='bill_detail' AND COLUMN_NAME='cost') <> 'bigint' THEN
    UPDATE bill_detail SET cost=ROUND(cost * 100000);
    ALTER TABLE bill_detail MODIFY COLUMN cost BIGINT NOT NULL COMMENT '消耗，单位千分之一分';
  END IF;
END$$
DELIMITER ;

CALL migrate_dwd_fact_sources();
DROP PROCEDURE migrate_dwd_fact_sources;
