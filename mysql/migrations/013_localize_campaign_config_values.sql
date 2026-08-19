USE ad_ods;

UPDATE campaign_info
SET `推广目标` = CASE `推广目标`
  WHEN 'app_promotion' THEN '应用推广'
  WHEN 'lead_collection' THEN '销售线索收集'
  WHEN 'live_promotion' THEN '直播推广'
  WHEN 'brand_promotion' THEN '品牌活动推广'
  WHEN 'ecommerce_order' THEN '电商下单推广'
  WHEN 'mini_program' THEN '小程序推广'
  WHEN 'kuaishou_account' THEN '快手号推广'
  ELSE `推广目标`
END;

UPDATE campaign_info
SET `广告类型` = CASE `广告类型`
  WHEN 'display' THEN '展示广告'
  WHEN 'search' THEN '搜索广告'
  ELSE `广告类型`
END;

UPDATE campaign_info
SET `竞价策略` = CASE `竞价策略`
  WHEN 'cost_priority' THEN '成本优先'
  WHEN 'max_conversion' THEN '最大转化'
  ELSE `竞价策略`
END;

UPDATE campaign_info
SET `预算模式` = CASE `预算模式`
  WHEN 'unlimited' THEN '不限'
  WHEN 'unified' THEN '统一预算'
  WHEN 'daily_split' THEN '分日预算'
  ELSE `预算模式`
END;

ALTER TABLE campaign_info
  ALTER COLUMN `推广目标` SET DEFAULT '电商下单推广',
  ALTER COLUMN `广告类型` SET DEFAULT '展示广告',
  ALTER COLUMN `竞价策略` SET DEFAULT '成本优先',
  ALTER COLUMN `预算模式` SET DEFAULT '不限';
