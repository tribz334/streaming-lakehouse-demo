USE ad_ods;

-- Replace the ambiguous order_amount with explicit price, quantity and total.
-- Existing demo orders represent one item, so their historical total is also
-- the unit price and product_num is initialized to 1.
DROP PROCEDURE IF EXISTS migrate_order_amount_030;
DELIMITER //
CREATE PROCEDURE migrate_order_amount_030()
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'ad_ods' AND table_name = 'order_detail' AND column_name = 'order_amount'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'ad_ods' AND table_name = 'order_detail' AND column_name = 'total_amount'
  ) THEN
    ALTER TABLE order_detail RENAME COLUMN order_amount TO total_amount;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'ad_ods' AND table_name = 'order_detail' AND column_name = 'product_price'
  ) THEN
    ALTER TABLE order_detail ADD COLUMN product_price DECIMAL(18,2) NULL AFTER product_id;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'ad_ods' AND table_name = 'order_detail' AND column_name = 'product_num'
  ) THEN
    ALTER TABLE order_detail ADD COLUMN product_num INT NULL AFTER product_price;
  END IF;

  UPDATE order_detail
  SET product_price = COALESCE(product_price, total_amount),
      product_num = COALESCE(product_num, 1);

  ALTER TABLE order_detail
    MODIFY COLUMN product_price DECIMAL(18,2) NOT NULL AFTER product_id,
    MODIFY COLUMN product_num INT NOT NULL DEFAULT 1 AFTER product_price,
    MODIFY COLUMN total_amount DECIMAL(18,2) NOT NULL AFTER product_num;
END//
DELIMITER ;
CALL migrate_order_amount_030();
DROP PROCEDURE migrate_order_amount_030;
