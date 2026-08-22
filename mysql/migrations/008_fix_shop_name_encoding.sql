USE ad_ods;

-- Use UTF-8 bytes explicitly so Windows shell encoding cannot corrupt the suffix.
UPDATE shop_info AS s
JOIN advertiser_info AS a
  ON s.shop_id = 1000 + a.advertiser_id
SET s.shop_name = CONCAT(
  a.advertiser_name,
  CONVERT(UNHEX('E5AE98E696B9E69797E888B0E5BA97') USING utf8mb4)
);
