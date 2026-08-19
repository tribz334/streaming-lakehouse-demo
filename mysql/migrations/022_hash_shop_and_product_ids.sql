USE ad_ods;

CREATE TEMPORARY TABLE shop_id_map (
  old_id VARCHAR(32) PRIMARY KEY,
  new_id VARCHAR(32) NOT NULL UNIQUE
);

INSERT INTO shop_id_map
SELECT shop_id, LEFT(SHA2(CONCAT('shop:', shop_id), 256), 20)
FROM shop_info;

CREATE TEMPORARY TABLE product_id_map (
  old_id VARCHAR(64) PRIMARY KEY,
  new_id VARCHAR(64) NOT NULL UNIQUE
);

INSERT INTO product_id_map
SELECT product_id, LEFT(SHA2(CONCAT('product:', product_id), 256), 20)
FROM product_info;

SET FOREIGN_KEY_CHECKS = 0;

UPDATE product_info p
JOIN shop_id_map m ON p.shop_id = m.old_id
SET p.shop_id = m.new_id;

UPDATE shop_info s
JOIN shop_id_map m ON s.shop_id = m.old_id
SET s.shop_id = m.new_id;

UPDATE order_detail o
JOIN product_id_map m ON o.product_id = m.old_id
SET o.product_id = m.new_id;

UPDATE unit_info u
JOIN product_id_map m ON u.`关联商品ID` = m.old_id
SET u.`关联商品ID` = m.new_id;

UPDATE product_info p
JOIN product_id_map m ON p.product_id = m.old_id
SET p.product_id = m.new_id;

SET FOREIGN_KEY_CHECKS = 1;
