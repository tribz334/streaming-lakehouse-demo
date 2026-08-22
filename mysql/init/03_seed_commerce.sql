USE ad_ods;

INSERT INTO shop_info (
  shop_id, shop_name, shop_type, status, main_category_id,
  main_category_name, shop_qualification_type, credit_code,
  contact_person, contact_phone, created_at
)
SELECT
  1000 + advertiser_id,
  CONCAT(advertiser_name, '官方旗舰店'),
  0, 0, 10000 + advertiser_id,
  COALESCE(industry_l2_name, '综合'), 2,
  CONCAT('91340000', LPAD(advertiser_id, 10, '0')),
  CONCAT(advertiser_name, '运营'),
  CONCAT('138', LPAD(advertiser_id, 8, '0')),
  created_at
FROM advertiser_info
ON DUPLICATE KEY UPDATE
  shop_name=VALUES(shop_name), main_category_name=VALUES(main_category_name),
  updated_at=CURRENT_TIMESTAMP;

INSERT INTO product_info (product_id, product_name, shop_id, price, created_at)
SELECT
  100000000 + cr.creative_id,
  CONCAT(a.advertiser_name, ' ', cr.creative_name),
  1000 + a.advertiser_id,
  (4900000 + MOD(cr.creative_id, 195000000)),
  cr.created_at
FROM creative_info cr
JOIN unit_info u ON cr.unit_id = u.unit_id
JOIN campaign_info c ON u.campaign_id = c.campaign_id
JOIN advertiser_info a ON c.advertiser_id = a.advertiser_id
ON DUPLICATE KEY UPDATE
  shop_id=VALUES(shop_id), product_name=VALUES(product_name),
  price=VALUES(price), updated_at=CURRENT_TIMESTAMP;

UPDATE unit_info u
SET u.product_id = (
  SELECT 100000000 + cr.creative_id
  FROM creative_info cr
  WHERE cr.unit_id = u.unit_id
  ORDER BY cr.creative_id
  LIMIT 1
);
