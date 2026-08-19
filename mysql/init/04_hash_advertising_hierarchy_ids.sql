USE ad_ods;

SET FOREIGN_KEY_CHECKS = 0;

UPDATE bill_detail
SET advertiser_id = LEFT(SHA2(CONCAT('advertiser:', advertiser_id), 256), 20),
    campaign_id = LEFT(SHA2(CONCAT('campaign:', campaign_id), 256), 20),
    unit_id = LEFT(SHA2(CONCAT('unit:', unit_id), 256), 20),
    creative_id = LEFT(SHA2(CONCAT('creative:', creative_id), 256), 20);

UPDATE creative_info
SET unit_id = LEFT(SHA2(CONCAT('unit:', unit_id), 256), 20),
    creative_id = LEFT(SHA2(CONCAT('creative:', creative_id), 256), 20);

UPDATE unit_info
SET campaign_id = LEFT(SHA2(CONCAT('campaign:', campaign_id), 256), 20),
    unit_id = LEFT(SHA2(CONCAT('unit:', unit_id), 256), 20);

UPDATE campaign_info
SET advertiser_id = LEFT(SHA2(CONCAT('advertiser:', advertiser_id), 256), 20),
    campaign_id = LEFT(SHA2(CONCAT('campaign:', campaign_id), 256), 20);

UPDATE advertiser_info
SET advertiser_id = LEFT(SHA2(CONCAT('advertiser:', advertiser_id), 256), 20);

SET FOREIGN_KEY_CHECKS = 1;
