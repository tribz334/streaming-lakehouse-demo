USE ad_ods;

-- Existing Docker volumes need this one-time migration. New installations get
-- the same column from mysql/init/01_schema.sql.
-- Append instead of inserting in the middle: Hive-backed Paimon can evolve an
-- appended field without reordering the existing gmv and lifecycle columns.
ALTER TABLE ad_order ADD COLUMN product_id VARCHAR(64) NULL;
UPDATE ad_order
SET product_id = CONCAT('product_', creative_id)
WHERE product_id IS NULL OR product_id = '';
ALTER TABLE ad_order MODIFY COLUMN product_id VARCHAR(64) NOT NULL;
