USE ad_ods;

ALTER TABLE campaign_info
  ADD COLUMN `推广目标` VARCHAR(32) NOT NULL DEFAULT '电商下单推广' AFTER campaign_name,
  ADD COLUMN `广告类型` VARCHAR(32) NOT NULL DEFAULT '展示广告' AFTER `推广目标`,
  ADD COLUMN `竞价策略` VARCHAR(32) NOT NULL DEFAULT '成本优先' AFTER `广告类型`,
  ADD COLUMN `预算模式` VARCHAR(32) NOT NULL DEFAULT '不限' AFTER `竞价策略`;

UPDATE campaign_info c
JOIN advertiser_info a ON c.advertiser_id = a.advertiser_id
SET
  c.`推广目标` = CASE MOD(CRC32(c.campaign_id), 7)
    WHEN 0 THEN '应用推广'
    WHEN 1 THEN '销售线索收集'
    WHEN 2 THEN '直播推广'
    WHEN 3 THEN '品牌活动推广'
    WHEN 4 THEN '电商下单推广'
    WHEN 5 THEN '小程序推广'
    ELSE '快手号推广'
  END,
  c.`广告类型` = CASE
    WHEN MOD(CRC32(CONCAT(c.campaign_id, '_ad_type')), 3) = 0 THEN '搜索广告'
    ELSE '展示广告'
  END,
  c.`竞价策略` = CASE
    WHEN MOD(CRC32(CONCAT(c.campaign_id, '_strategy')), 3) = 0 THEN '成本优先'
    ELSE '最大转化'
  END,
  c.`预算模式` = CASE MOD(CRC32(CONCAT(c.campaign_id, '_budget')), 3)
    WHEN 0 THEN '不限'
    WHEN 1 THEN '统一预算'
    ELSE '分日预算'
  END;
