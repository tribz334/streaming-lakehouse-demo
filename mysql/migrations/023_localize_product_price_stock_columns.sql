USE ad_ods;

ALTER TABLE product_info
  RENAME COLUMN sale_price TO `销售价格`,
  RENAME COLUMN stock_quantity TO `库存数量`;
