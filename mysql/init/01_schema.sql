CREATE DATABASE IF NOT EXISTS ad_ods;
USE ad_ods;

CREATE TABLE IF NOT EXISTS advertiser_info (
  advertiser_id BIGINT PRIMARY KEY,
  advertiser_name VARCHAR(128) NOT NULL,
  qualification_type INT NOT NULL DEFAULT 0,
  status INT NOT NULL DEFAULT 2,
  industry_l1_id BIGINT NULL,
  industry_l1_name VARCHAR(64) NULL,
  industry_l2_id BIGINT NULL,
  industry_l2_name VARCHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS campaign_info (
  campaign_id BIGINT PRIMARY KEY,
  campaign_name VARCHAR(128) NOT NULL,
  advertiser_id BIGINT NOT NULL,
  status INT NOT NULL DEFAULT 4,
  market_goal INT NOT NULL DEFAULT 4,
  trading_mode INT NOT NULL DEFAULT 0,
  budget BIGINT NOT NULL,
  daily_budget BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_campaign_advertiser FOREIGN KEY (advertiser_id) REFERENCES advertiser_info(advertiser_id)
);

CREATE TABLE IF NOT EXISTS unit_info (
  unit_id BIGINT PRIMARY KEY,
  unit_name VARCHAR(128) NOT NULL,
  campaign_id BIGINT NOT NULL,
  status INT NOT NULL DEFAULT 0,
  is_closed INT NOT NULL DEFAULT 0,
  placement_type INT NOT NULL DEFAULT 6 COMMENT '1-search,2-splash,3-feed,4-rewarded,5-banner,6-other',
  ad_type INT NOT NULL DEFAULT 4 COMMENT '1-short_video,2-live,3-image_text,4-other',
  search_keyword JSON NULL,
  product_id BIGINT NULL,
  landing_page_url VARCHAR(512) NULL,
  audience JSON NULL,
  start_date DATETIME NOT NULL,
  end_date DATETIME NULL,
  daily_budget BIGINT NOT NULL,
  bid_type VARCHAR(32) NOT NULL,
  bid BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT chk_unit_placement_type CHECK (placement_type BETWEEN 1 AND 6),
  CONSTRAINT chk_unit_ad_type CHECK (ad_type BETWEEN 1 AND 4),
  CONSTRAINT fk_unit_campaign FOREIGN KEY (campaign_id) REFERENCES campaign_info(campaign_id)
);

CREATE TABLE IF NOT EXISTS creative_info (
  creative_id BIGINT PRIMARY KEY,
  creative_name VARCHAR(128) NOT NULL,
  unit_id BIGINT NOT NULL,
  status INT NOT NULL DEFAULT 0,
  creative_mode INT NOT NULL DEFAULT 1,
  material_mode INT NOT NULL DEFAULT 1,
  creative_title VARCHAR(255) NULL,
  creative_category VARCHAR(64) NULL,
  creative_tags JSON NULL,
  creative_text TEXT NULL,
  creative_image_urls VARCHAR(1024) NULL,
  creative_video_id BIGINT NULL,
  monitoring_url VARCHAR(1024) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_creative_unit FOREIGN KEY (unit_id) REFERENCES unit_info(unit_id)
);

CREATE TABLE IF NOT EXISTS user_info (
  uid BIGINT PRIMARY KEY,
  user_name VARCHAR(128) NOT NULL,
  gender INT NOT NULL DEFAULT 0,
  phone_hash VARCHAR(64) NULL,
  email VARCHAR(255) NULL,
  user_level INT NOT NULL DEFAULT 0,
  birthday DATE NULL,
  status INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shop_info (
  shop_id BIGINT PRIMARY KEY,
  shop_name VARCHAR(128) NOT NULL,
  shop_type INT NOT NULL DEFAULT 0,
  status INT NOT NULL DEFAULT 0,
  main_category_id BIGINT NULL,
  main_category_name VARCHAR(128) NULL,
  shop_qualification_type INT NOT NULL DEFAULT 2,
  credit_code VARCHAR(64) NULL COMMENT '统一社会信用代码，个人资质为空',
  contact_person VARCHAR(64) NULL COMMENT '联系人',
  contact_phone VARCHAR(32) NULL COMMENT '联系电话',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_info (
  product_id BIGINT PRIMARY KEY,
  product_name VARCHAR(128) NOT NULL,
  shop_id BIGINT NOT NULL,
  price BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_product_shop (shop_id),
  CONSTRAINT fk_product_shop FOREIGN KEY (shop_id) REFERENCES shop_info(shop_id)
);

CREATE TABLE IF NOT EXISTS order_detail (
  order_id BIGINT PRIMARY KEY,
  user_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  shop_id BIGINT NULL,
  product_price BIGINT NOT NULL COMMENT '商品单价，单位千分之一分',
  product_num INT NOT NULL DEFAULT 1,
  total_amount BIGINT NOT NULL COMMENT '订单金额，单位千分之一分',
  payment_method INT NULL COMMENT '1-微信，2-支付宝',
  receiver_name VARCHAR(128) NULL,
  receiver_phone VARCHAR(32) NULL,
  shipping_address VARCHAR(512) NULL,
  tracking_number VARCHAR(128) NULL,
  order_status INT NOT NULL,
  create_time TIMESTAMP NOT NULL,
  cancel_time TIMESTAMP NULL,
  pay_time TIMESTAMP NULL,
  confirm_time TIMESTAMP NULL,
  refund_time TIMESTAMP NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT chk_order_status CHECK (order_status BETWEEN 1 AND 5)
);

CREATE TABLE IF NOT EXISTS bill_detail (
  bill_id BIGINT PRIMARY KEY,
  advertiser_id BIGINT NOT NULL,
  campaign_id BIGINT NOT NULL,
  unit_id BIGINT NOT NULL,
  creative_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  slot_id BIGINT NOT NULL,
  billing_type TINYINT NOT NULL COMMENT '1-曝光，2-点击，3-转化',
  media VARCHAR(32) NOT NULL,
  commerce_channel VARCHAR(32) NOT NULL,
  cost BIGINT NOT NULL COMMENT '消耗，单位千分之一分',
  bill_time TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_bill_time (bill_time),
  INDEX idx_bill_creative (creative_id)
);
