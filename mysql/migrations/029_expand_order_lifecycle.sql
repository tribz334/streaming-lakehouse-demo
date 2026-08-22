USE ad_ods;

-- Complete the accumulating order lifecycle. Existing refund_time is retained
-- as the refund-request time; refund_finish_time is the terminal refund event.
-- The procedure makes this migration safe to rerun against a persistent volume.
DROP PROCEDURE IF EXISTS migrate_order_lifecycle_029;
DELIMITER //
CREATE PROCEDURE migrate_order_lifecycle_029()
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'ad_ods' AND table_name = 'order_detail' AND column_name = 'cancel_time'
  ) THEN
    ALTER TABLE order_detail ADD COLUMN cancel_time TIMESTAMP NULL AFTER create_time;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'ad_ods' AND table_name = 'order_detail' AND column_name = 'confirm_time'
  ) THEN
    ALTER TABLE order_detail ADD COLUMN confirm_time TIMESTAMP NULL AFTER payment_time;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'ad_ods' AND table_name = 'order_detail' AND column_name = 'refund_finish_time'
  ) THEN
    ALTER TABLE order_detail ADD COLUMN refund_finish_time TIMESTAMP NULL AFTER refund_time;
  END IF;
END//
DELIMITER ;
CALL migrate_order_lifecycle_029();
DROP PROCEDURE migrate_order_lifecycle_029;
