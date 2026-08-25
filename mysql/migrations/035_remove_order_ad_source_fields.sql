SET @drop_creative = IF(
  EXISTS(SELECT 1 FROM information_schema.columns
    WHERE table_schema='ad_ods' AND table_name='order_detail' AND column_name='creative_id'),
  'ALTER TABLE ad_ods.order_detail DROP COLUMN creative_id','SELECT 1'
);
PREPARE stmt FROM @drop_creative; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @drop_slot = IF(
  EXISTS(SELECT 1 FROM information_schema.columns
    WHERE table_schema='ad_ods' AND table_name='order_detail' AND column_name='slot_id'),
  'ALTER TABLE ad_ods.order_detail DROP COLUMN slot_id','SELECT 1'
);
PREPARE stmt FROM @drop_slot; EXECUTE stmt; DEALLOCATE PREPARE stmt;
