USE ad_ods;

-- Accept either historical Unit column name, then converge on ad_type.
SET @drop_scene_check = (
  SELECT IF(EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_schema='ad_ods' AND table_name='unit_info' AND constraint_name='chk_unit_scene'),
    'ALTER TABLE unit_info DROP CHECK chk_unit_scene','SELECT 1')
);
PREPARE stmt FROM @drop_scene_check; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @drop_delivery_type_check = (
  SELECT IF(EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_schema='ad_ods' AND table_name='unit_info' AND constraint_name='chk_unit_delivery_type'),
    'ALTER TABLE unit_info DROP CHECK chk_unit_delivery_type','SELECT 1')
);
PREPARE stmt FROM @drop_delivery_type_check; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @drop_placement_check = (
  SELECT IF(EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_schema='ad_ods' AND table_name='unit_info' AND constraint_name='chk_unit_placement_type'),
    'ALTER TABLE unit_info DROP CHECK chk_unit_placement_type','SELECT 1')
);
PREPARE stmt FROM @drop_placement_check; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @drop_ad_type_check = (
  SELECT IF(EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_schema='ad_ods' AND table_name='unit_info' AND constraint_name='chk_unit_ad_type'),
    'ALTER TABLE unit_info DROP CHECK chk_unit_ad_type','SELECT 1')
);
PREPARE stmt FROM @drop_ad_type_check; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @migrate_ad_type = (
  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='unit_info' AND column_name='ad_type') THEN 'SELECT 1'
    WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='unit_info' AND column_name='delivery_type') THEN 'ALTER TABLE unit_info RENAME COLUMN delivery_type TO ad_type'
    WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='unit_info' AND column_name='scene') THEN 'ALTER TABLE unit_info RENAME COLUMN scene TO ad_type'
    ELSE 'ALTER TABLE unit_info ADD COLUMN ad_type INT NOT NULL DEFAULT 4'
  END
);
PREPARE stmt FROM @migrate_ad_type; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @add_placement_type = (
  SELECT IF(EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='unit_info' AND column_name='placement_type'),
    'SELECT 1','ALTER TABLE unit_info ADD COLUMN placement_type INT NOT NULL DEFAULT 6 AFTER is_closed')
);
PREPARE stmt FROM @add_placement_type; EXECUTE stmt; DEALLOCATE PREPARE stmt;

ALTER TABLE unit_info MODIFY COLUMN placement_type INT NOT NULL DEFAULT 6,
  MODIFY COLUMN ad_type INT NOT NULL DEFAULT 4;
UPDATE unit_info SET placement_type=6 WHERE placement_type NOT BETWEEN 1 AND 6;
UPDATE unit_info SET ad_type=4 WHERE ad_type NOT BETWEEN 1 AND 4;

SET @add_placement_check = (
  SELECT IF(EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_schema='ad_ods' AND table_name='unit_info' AND constraint_name='chk_unit_placement_type'),
    'SELECT 1','ALTER TABLE unit_info ADD CONSTRAINT chk_unit_placement_type CHECK (placement_type BETWEEN 1 AND 6)')
);
PREPARE stmt FROM @add_placement_check; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @add_ad_type_check = (
  SELECT IF(EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_schema='ad_ods' AND table_name='unit_info' AND constraint_name='chk_unit_ad_type'),
    'SELECT 1','ALTER TABLE unit_info ADD CONSTRAINT chk_unit_ad_type CHECK (ad_type BETWEEN 1 AND 4)')
);
PREPARE stmt FROM @add_ad_type_check; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @drop_campaign_ad_type = (
  SELECT IF(EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='campaign_info' AND column_name='ad_type'),
    'ALTER TABLE campaign_info DROP COLUMN ad_type','SELECT 1')
);
PREPARE stmt FROM @drop_campaign_ad_type; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @rename_commerce_channel = (
  SELECT IF(EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='bill_detail' AND column_name='commerce_scene'),
    'ALTER TABLE bill_detail RENAME COLUMN commerce_scene TO commerce_channel','SELECT 1')
);
PREPARE stmt FROM @rename_commerce_channel; EXECUTE stmt; DEALLOCATE PREPARE stmt;
