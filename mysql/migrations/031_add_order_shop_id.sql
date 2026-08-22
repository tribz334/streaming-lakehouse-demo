USE ad_ods;

DROP PROCEDURE IF EXISTS migrate_order_shop_031;
DELIMITER //
CREATE PROCEDURE migrate_order_shop_031()
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'ad_ods' AND table_name = 'order_detail' AND column_name = 'shop_id'
  ) THEN
    ALTER TABLE order_detail ADD COLUMN shop_id BIGINT NULL AFTER product_id;
  END IF;

  UPDATE order_detail o
  LEFT JOIN product_info p ON p.product_id = o.product_id
  SET o.shop_id = p.shop_id
  WHERE o.shop_id IS NULL;
END//
DELIMITER ;
CALL migrate_order_shop_031();
DROP PROCEDURE migrate_order_shop_031;
