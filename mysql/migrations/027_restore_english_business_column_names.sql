USE ad_ods;

ALTER TABLE campaign_info
  RENAME COLUMN `推广目标` TO promotion_goal,
  RENAME COLUMN `广告类型` TO ad_type,
  RENAME COLUMN `竞价策略` TO bidding_strategy,
  RENAME COLUMN `预算模式` TO budget_mode;

ALTER TABLE order_detail
  RENAME COLUMN `订单金额` TO order_amount;
