USE ad_ods;

ALTER TABLE user_info
  DROP COLUMN nickname,
  ADD COLUMN user_type VARCHAR(32) NOT NULL DEFAULT 'consumer' AFTER user_id,
  ADD COLUMN register_channel VARCHAR(32) NOT NULL DEFAULT 'organic' AFTER user_type,
  ADD COLUMN region VARCHAR(64) NULL AFTER register_channel,
  ADD COLUMN membership_level VARCHAR(32) NOT NULL DEFAULT 'normal' AFTER region,
  ADD COLUMN last_active_at TIMESTAMP NULL AFTER registered_at,
  ADD INDEX idx_user_registered_at (registered_at),
  ADD INDEX idx_user_status (status);

ALTER TABLE shop_info
  ADD COLUMN shop_type VARCHAR(32) NOT NULL DEFAULT 'flagship' AFTER shop_name,
  ADD COLUMN region VARCHAR(64) NULL AFTER shop_type,
  ADD COLUMN opened_at DATE NULL AFTER business_status;

ALTER TABLE product_info
  ADD COLUMN category VARCHAR(64) NULL AFTER brand,
  ADD COLUMN `销售价格` DECIMAL(18,2) NOT NULL DEFAULT 0 AFTER category,
  ADD COLUMN `库存数量` INT NOT NULL DEFAULT 0 AFTER `销售价格`,
  ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER status,
  ADD INDEX idx_product_category (category);

INSERT INTO shop_info
  (shop_id, shop_name, shop_type, region, business_status, opened_at)
SELECT
  LEFT(SHA2(CONCAT('shop:', CONCAT('shop_', advertiser_id)), 256), 20),
  CONCAT(advertiser_name, CONVERT(UNHEX('E5AE98E696B9E69797E888B0E5BA97') USING utf8mb4)),
  CASE WHEN industry = 'ecommerce' THEN 'platform' ELSE 'flagship' END,
  home_region,
  'open',
  signup_date
FROM advertiser_info;

INSERT INTO product_info
  (product_id, shop_id, product_name, brand, category, `销售价格`,
   `库存数量`, status, created_at)
SELECT
  LEFT(SHA2(CONCAT('product:', CONCAT('product_', cr.creative_id)), 256), 20),
  LEFT(SHA2(CONCAT('shop:', CONCAT('shop_', a.advertiser_id)), 256), 20),
  CONCAT(a.advertiser_name, ' ', cr.creative_name),
  a.advertiser_name,
  a.industry,
  CAST(49 + MOD(CRC32(cr.creative_id), 195000) / 100 AS DECIMAL(18,2)),
  100 + MOD(CRC32(CONCAT(cr.creative_id, '_stock')), 4901),
  'active',
  a.signup_date
FROM creative_info cr
JOIN unit_info u ON cr.unit_id = u.unit_id
JOIN campaign_info c ON u.campaign_id = c.campaign_id
JOIN advertiser_info a ON c.advertiser_id = a.advertiser_id;

INSERT INTO user_info
  (user_id, user_type, register_channel, region, membership_level,
   status, registered_at, last_active_at)
SELECT
  u.user_id,
  'consumer',
  ELT(1 + MOD(CRC32(u.user_id), 5), 'organic', 'douyin', 'kuaishou', 'weibo', 'referral'),
  ELT(1 + MOD(CRC32(CONCAT(u.user_id, '_region')), 8),
      'Beijing', 'Shanghai', 'Guangdong', 'Zhejiang',
      'Jiangsu', 'Sichuan', 'Hubei', 'Fujian'),
  ELT(1 + MOD(CRC32(CONCAT(u.user_id, '_level')), 4),
      'normal', 'silver', 'gold', 'platinum'),
  'active',
  u.first_seen,
  u.last_seen
FROM (
  SELECT user_id, MIN(event_time) AS first_seen, MAX(event_time) AS last_seen
  FROM (
    SELECT user_id, create_time AS event_time FROM order_detail
    UNION ALL
    SELECT user_id, bill_time AS event_time FROM bill_detail
  ) activity
  GROUP BY user_id
) u;
