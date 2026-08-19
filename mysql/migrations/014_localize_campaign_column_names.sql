USE ad_ods;

ALTER TABLE campaign_info
  RENAME COLUMN promotion_goal TO `推广目标`,
  RENAME COLUMN ad_type TO `广告类型`,
  RENAME COLUMN bidding_strategy TO `竞价策略`,
  RENAME COLUMN budget_mode TO `预算模式`;
