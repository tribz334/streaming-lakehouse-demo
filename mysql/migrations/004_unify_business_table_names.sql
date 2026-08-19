USE ad_ods;

RENAME TABLE
  advertiser TO advertiser_info,
  campaign TO campaign_info,
  `unit` TO unit_info,
  creative TO creative_info,
  `order` TO order_detail,
  ad_bill TO bill_detail;

CREATE TABLE IF NOT EXISTS user_info (
  user_id VARCHAR(32) PRIMARY KEY,
  user_type VARCHAR(32) NOT NULL DEFAULT 'consumer',
  register_channel VARCHAR(32) NOT NULL DEFAULT 'organic',
  region VARCHAR(64) NULL,
  membership_level VARCHAR(32) NOT NULL DEFAULT 'normal',
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  registered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_active_at TIMESTAMP NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shop_info (
  shop_id VARCHAR(32) PRIMARY KEY,
  shop_name VARCHAR(128) NOT NULL,
  shop_type VARCHAR(32) NOT NULL DEFAULT 'flagship',
  region VARCHAR(64) NULL,
  business_status VARCHAR(32) NOT NULL DEFAULT 'open',
  opened_at DATE NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_info (
  product_id VARCHAR(64) PRIMARY KEY,
  shop_id VARCHAR(32) NULL,
  product_name VARCHAR(128) NOT NULL,
  brand VARCHAR(128) NULL,
  category VARCHAR(64) NULL,
  `销售价格` DECIMAL(18,2) NOT NULL DEFAULT 0,
  `库存数量` INT NOT NULL DEFAULT 0,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_product_shop (shop_id),
  CONSTRAINT fk_product_shop FOREIGN KEY (shop_id) REFERENCES shop_info(shop_id)
);
