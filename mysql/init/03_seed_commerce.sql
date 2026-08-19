USE ad_ods;

INSERT INTO shop_info
  (shop_id, shop_name, shop_type, region, business_status, opened_at)
SELECT
  LEFT(SHA2(CONCAT('shop:', CONCAT('shop_', advertiser_id)), 256), 20),
  CONCAT(advertiser_name, CONVERT(UNHEX('E5AE98E696B9E69797E888B0E5BA97') USING utf8mb4)),
  CASE WHEN industry = 'ecommerce' THEN 'platform' ELSE 'flagship' END,
  home_region,
  'open',
  signup_date
FROM advertiser_info
ON DUPLICATE KEY UPDATE
  shop_name = VALUES(shop_name),
  shop_type = VALUES(shop_type),
  region = VALUES(region),
  business_status = VALUES(business_status);

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
JOIN advertiser_info a ON c.advertiser_id = a.advertiser_id
ON DUPLICATE KEY UPDATE
  shop_id = VALUES(shop_id),
  product_name = VALUES(product_name),
  brand = VALUES(brand),
  category = VALUES(category),
  `销售价格` = VALUES(`销售价格`),
  `库存数量` = VALUES(`库存数量`),
  status = VALUES(status);

UPDATE unit_info u
SET u.`关联商品ID` = (
  SELECT LEFT(SHA2(CONCAT('product:', CONCAT('product_', cr.creative_id)), 256), 20)
  FROM creative_info cr
  WHERE cr.unit_id = u.unit_id
  ORDER BY cr.creative_id
  LIMIT 1
);
