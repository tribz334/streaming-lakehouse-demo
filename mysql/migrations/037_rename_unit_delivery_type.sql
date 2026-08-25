USE ad_ods;

SET @rename_delivery_type = (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema='ad_ods'
        AND table_name='unit_info'
        AND column_name='delivery_way'
    ),
    'ALTER TABLE unit_info RENAME COLUMN delivery_way TO delivery_type',
    'SELECT 1'
  )
);
PREPARE rename_delivery_type_stmt FROM @rename_delivery_type;
EXECUTE rename_delivery_type_stmt;
DEALLOCATE PREPARE rename_delivery_type_stmt;

ALTER TABLE unit_info
  MODIFY COLUMN delivery_type INT NOT NULL DEFAULT 2
  COMMENT '1-电商广告，2-短视频广告，3-直播广告';
