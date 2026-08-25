USE ad_ods;

DROP PROCEDURE IF EXISTS migrate_order_lifecycle_036;
DELIMITER //
CREATE PROCEDURE migrate_order_lifecycle_036()
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='ad_ods' AND table_name='order_detail' AND column_name='payment_time'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='ad_ods' AND table_name='order_detail' AND column_name='pay_time'
  ) THEN
    ALTER TABLE order_detail RENAME COLUMN payment_time TO pay_time;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='ad_ods' AND table_name='order_detail' AND column_name='refund_finish_time'
  ) THEN
    ALTER TABLE order_detail DROP COLUMN refund_finish_time;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='ad_ods' AND table_name='order_detail' AND column_name='finish_time'
  ) THEN
    ALTER TABLE order_detail DROP COLUMN finish_time;
  END IF;

  UPDATE order_detail
  SET order_status=CASE
    WHEN refund_time IS NOT NULL THEN 5
    WHEN confirm_time IS NOT NULL THEN 4
    WHEN pay_time IS NOT NULL THEN 3
    WHEN cancel_time IS NOT NULL THEN 2
    ELSE 1
  END;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.table_constraints
    WHERE constraint_schema='ad_ods' AND table_name='order_detail'
      AND constraint_name='chk_order_status' AND constraint_type='CHECK'
  ) THEN
    ALTER TABLE order_detail
      ADD CONSTRAINT chk_order_status CHECK (order_status BETWEEN 1 AND 5);
  END IF;
END//
DELIMITER ;
CALL migrate_order_lifecycle_036();
DROP PROCEDURE migrate_order_lifecycle_036;
