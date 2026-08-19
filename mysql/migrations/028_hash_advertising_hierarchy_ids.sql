USE ad_ods;

-- Keep the four advertising hierarchy identifiers opaque and stable.
-- Existing 20-character hexadecimal hashes are left untouched, making this
-- migration safe to run more than once.
SET FOREIGN_KEY_CHECKS = 0;

UPDATE bill_detail
SET advertiser_id = CASE
      WHEN advertiser_id REGEXP '^[0-9a-f]{20}$' THEN advertiser_id
      ELSE LEFT(SHA2(CONCAT('advertiser:', advertiser_id), 256), 20)
    END,
    campaign_id = CASE
      WHEN campaign_id REGEXP '^[0-9a-f]{20}$' THEN campaign_id
      ELSE LEFT(SHA2(CONCAT('campaign:', campaign_id), 256), 20)
    END,
    unit_id = CASE
      WHEN unit_id REGEXP '^[0-9a-f]{20}$' THEN unit_id
      ELSE LEFT(SHA2(CONCAT('unit:', unit_id), 256), 20)
    END,
    creative_id = CASE
      WHEN creative_id REGEXP '^[0-9a-f]{20}$' THEN creative_id
      ELSE LEFT(SHA2(CONCAT('creative:', creative_id), 256), 20)
    END;

UPDATE creative_info
SET unit_id = CASE
      WHEN unit_id REGEXP '^[0-9a-f]{20}$' THEN unit_id
      ELSE LEFT(SHA2(CONCAT('unit:', unit_id), 256), 20)
    END,
    creative_id = CASE
      WHEN creative_id REGEXP '^[0-9a-f]{20}$' THEN creative_id
      ELSE LEFT(SHA2(CONCAT('creative:', creative_id), 256), 20)
    END;

UPDATE unit_info
SET campaign_id = CASE
      WHEN campaign_id REGEXP '^[0-9a-f]{20}$' THEN campaign_id
      ELSE LEFT(SHA2(CONCAT('campaign:', campaign_id), 256), 20)
    END,
    unit_id = CASE
      WHEN unit_id REGEXP '^[0-9a-f]{20}$' THEN unit_id
      ELSE LEFT(SHA2(CONCAT('unit:', unit_id), 256), 20)
    END;

UPDATE campaign_info
SET advertiser_id = CASE
      WHEN advertiser_id REGEXP '^[0-9a-f]{20}$' THEN advertiser_id
      ELSE LEFT(SHA2(CONCAT('advertiser:', advertiser_id), 256), 20)
    END,
    campaign_id = CASE
      WHEN campaign_id REGEXP '^[0-9a-f]{20}$' THEN campaign_id
      ELSE LEFT(SHA2(CONCAT('campaign:', campaign_id), 256), 20)
    END;

UPDATE advertiser_info
SET advertiser_id = CASE
  WHEN advertiser_id REGEXP '^[0-9a-f]{20}$' THEN advertiser_id
  ELSE LEFT(SHA2(CONCAT('advertiser:', advertiser_id), 256), 20)
END;

SET FOREIGN_KEY_CHECKS = 1;
