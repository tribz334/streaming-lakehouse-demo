USE ad_ods;

-- Bring legacy creative tables to the current platform data dictionary.
DROP PROCEDURE IF EXISTS migrate_creative_config_032;
DELIMITER //
CREATE PROCEDURE migrate_creative_config_032()
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='status') THEN
    ALTER TABLE creative_info ADD COLUMN status INT NOT NULL DEFAULT 0 AFTER creative_name;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='creative_mode') THEN
    ALTER TABLE creative_info ADD COLUMN creative_mode INT NOT NULL DEFAULT 1 AFTER status;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='material_mode') THEN
    ALTER TABLE creative_info ADD COLUMN material_mode INT NOT NULL DEFAULT 1 AFTER creative_mode;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='creative_title') THEN
    ALTER TABLE creative_info ADD COLUMN creative_title VARCHAR(255) NULL AFTER material_mode;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='creative_text') THEN
    ALTER TABLE creative_info ADD COLUMN creative_text TEXT NULL AFTER creative_title;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='creative_tags') THEN
    ALTER TABLE creative_info ADD COLUMN creative_tags JSON NULL AFTER creative_text;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='creative_image_urls') THEN
    ALTER TABLE creative_info ADD COLUMN creative_image_urls VARCHAR(1024) NULL AFTER creative_tags;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='creative_video_id') THEN
    ALTER TABLE creative_info ADD COLUMN creative_video_id BIGINT NULL AFTER creative_image_urls;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='created_at') THEN
    ALTER TABLE creative_info ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER creative_video_id;
  END IF;

  UPDATE creative_info
  SET creative_title=COALESCE(creative_title, creative_name),
      creative_text=COALESCE(creative_text, CONCAT(creative_name, '，了解更多产品信息。')),
      creative_tags=COALESCE(creative_tags, JSON_ARRAY('video')),
      creative_video_id=CASE WHEN material_mode=1 THEN COALESCE(creative_video_id, creative_id * 10 + 1) ELSE NULL END;

  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='category') THEN
    ALTER TABLE creative_info DROP COLUMN category;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='tags') THEN
    ALTER TABLE creative_info DROP COLUMN tags;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='plc_description') THEN
    ALTER TABLE creative_info DROP COLUMN plc_description;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ad_ods' AND table_name='creative_info' AND column_name='material') THEN
    ALTER TABLE creative_info DROP COLUMN material;
  END IF;
END//
DELIMITER ;
CALL migrate_creative_config_032();
DROP PROCEDURE migrate_creative_config_032;
