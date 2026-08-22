-- MySQL binlog is synchronized directly into current-state DIM tables.
-- Paimon daily tags on the sinks retain the historical table snapshots.
SET 'execution.runtime-mode'='streaming';
SET 'execution.checkpointing.interval'='10s';
SET 'pipeline.name'='mysql-cdc-direct-to-dim';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'table.exec.sink.upsert-materialize'='NONE';
SET 'parallelism.default'='1';

CREATE CATALOG paimon WITH (
  'type'='paimon','metastore'='hive','uri'='thrift://hive-metastore:9083',
  'warehouse'='file:///warehouse/paimon'
);

CREATE TEMPORARY TABLE mysql_advertiser (
  advertiser_id BIGINT,advertiser_name STRING,qualification_type INT,status INT,
  industry_l1_id BIGINT,industry_l1_name STRING,industry_l2_id BIGINT,
  industry_l2_name STRING,created_at TIMESTAMP(3),updated_at TIMESTAMP(3),
  PRIMARY KEY(advertiser_id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root',
  'password'='root','database-name'='ad_ods','table-name'='advertiser_info',
  'server-id'='5611-5618','server-time-zone'='UTC','scan.startup.mode'='initial'
);

CREATE TEMPORARY TABLE mysql_campaign (
  campaign_id BIGINT,campaign_name STRING,advertiser_id BIGINT,status INT,
  market_goal INT,ad_type INT,trading_mode INT,budget BIGINT,daily_budget BIGINT,
  created_at TIMESTAMP(3),updated_at TIMESTAMP(3),
  PRIMARY KEY(campaign_id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root',
  'password'='root','database-name'='ad_ods','table-name'='campaign_info',
  'server-id'='5621-5628','server-time-zone'='UTC','scan.startup.mode'='initial'
);

CREATE TEMPORARY TABLE mysql_unit (
  unit_id BIGINT,unit_name STRING,campaign_id BIGINT,status INT,is_closed INT,
  delivery_way INT,search_keyword STRING,product_id BIGINT,landing_page_url STRING,
  audience STRING,start_date TIMESTAMP(3),end_date TIMESTAMP(3),daily_budget BIGINT,
  bid_type STRING,bid BIGINT,created_at TIMESTAMP(3),updated_at TIMESTAMP(3),
  PRIMARY KEY(unit_id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root',
  'password'='root','database-name'='ad_ods','table-name'='unit_info',
  'server-id'='5631-5638','server-time-zone'='UTC','scan.startup.mode'='initial'
);

CREATE TEMPORARY TABLE mysql_creative (
  creative_id BIGINT,creative_name STRING,unit_id BIGINT,status INT,
  creative_mode INT,material_mode INT,creative_title STRING,creative_category STRING,
  creative_tags STRING,creative_text STRING,creative_image_urls STRING,
  creative_video_id BIGINT,monitoring_url STRING,created_at TIMESTAMP(3),updated_at TIMESTAMP(3),
  PRIMARY KEY(creative_id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root',
  'password'='root','database-name'='ad_ods','table-name'='creative_info',
  'server-id'='5641-5648','server-time-zone'='UTC','scan.startup.mode'='initial'
);

CREATE TEMPORARY TABLE mysql_user (
  uid BIGINT,user_name STRING,gender INT,phone_hash STRING,email STRING,user_level INT,
  birthday DATE,status INT,created_at TIMESTAMP(3),updated_at TIMESTAMP(3),
  PRIMARY KEY(uid) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root',
  'password'='root','database-name'='ad_ods','table-name'='user_info',
  'server-id'='5651-5658','server-time-zone'='UTC','scan.startup.mode'='initial'
);

CREATE TEMPORARY TABLE mysql_shop (
  shop_id BIGINT,shop_name STRING,shop_type INT,status INT,main_category_id BIGINT,
  main_category_name STRING,shop_qualification_type INT,credit_code STRING,
  contact_person STRING,contact_phone STRING,created_at TIMESTAMP(3),updated_at TIMESTAMP(3),
  PRIMARY KEY(shop_id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root',
  'password'='root','database-name'='ad_ods','table-name'='shop_info',
  'server-id'='5661-5668','server-time-zone'='UTC','scan.startup.mode'='initial'
);

CREATE TEMPORARY TABLE mysql_product (
  product_id BIGINT,product_name STRING,shop_id BIGINT,price BIGINT,
  created_at TIMESTAMP(3),updated_at TIMESTAMP(3),
  PRIMARY KEY(product_id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root',
  'password'='root','database-name'='ad_ods','table-name'='product_info',
  'server-id'='5671-5678','server-time-zone'='UTC','scan.startup.mode'='initial'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO paimon.ad_dw.dim_advertiser_zip
  SELECT advertiser_id,advertiser_name,qualification_type,status,industry_l1_id,
    industry_l1_name,industry_l2_id,industry_l2_name,
    CAST(created_at AS STRING),CAST(updated_at AS STRING)
  FROM mysql_advertiser;

  INSERT INTO paimon.ad_dw.dim_campaign
  SELECT c.campaign_id,c.campaign_name,c.advertiser_id,a.advertiser_name,c.status,
    c.market_goal,c.ad_type,c.trading_mode,c.budget,c.daily_budget,
    CAST(c.created_at AS STRING),CAST(c.updated_at AS STRING)
  FROM mysql_campaign c LEFT JOIN mysql_advertiser a
    ON c.advertiser_id=a.advertiser_id;

  INSERT INTO paimon.ad_dw.dim_unit
  SELECT u.unit_id,u.unit_name,u.campaign_id,c.campaign_name,u.status,u.is_closed,
    u.delivery_way,JSON_QUERY(u.search_keyword,'$' RETURNING ARRAY<STRING>),
    u.product_id,u.landing_page_url,u.audience,CAST(u.start_date AS STRING),
    CAST(u.end_date AS STRING),u.daily_budget,u.bid_type,u.bid,
    CAST(u.created_at AS STRING),CAST(u.updated_at AS STRING)
  FROM mysql_unit u LEFT JOIN mysql_campaign c ON u.campaign_id=c.campaign_id;

  INSERT INTO paimon.ad_dw.dim_creative
  SELECT cr.creative_id,cr.creative_name,cr.unit_id,u.unit_name,cr.status,
    cr.creative_mode,cr.material_mode,cr.creative_title,cr.creative_category,
    JSON_QUERY(cr.creative_tags,'$' RETURNING ARRAY<STRING>),cr.creative_text,
    cr.creative_image_urls,cr.creative_video_id,cr.monitoring_url,
    CAST(cr.created_at AS STRING),CAST(cr.updated_at AS STRING)
  FROM mysql_creative cr LEFT JOIN mysql_unit u ON cr.unit_id=u.unit_id;

  INSERT INTO paimon.ad_dw.dim_user_zip
  SELECT uid,user_name,gender,phone_hash,email,user_level,CAST(birthday AS STRING),
    status,CAST(created_at AS STRING),CAST(updated_at AS STRING)
  FROM mysql_user;

  INSERT INTO paimon.ad_dw.dim_shop_zip
  SELECT shop_id,shop_name,shop_type,status,main_category_id,main_category_name,
    shop_qualification_type,credit_code,contact_person,contact_phone,
    CAST(created_at AS STRING),CAST(updated_at AS STRING)
  FROM mysql_shop;

  INSERT INTO paimon.ad_dw.dim_product_zip
  SELECT p.product_id,p.product_name,p.shop_id,s.shop_name,p.price,
    CAST(p.created_at AS STRING),CAST(p.updated_at AS STRING)
  FROM mysql_product p LEFT JOIN mysql_shop s ON p.shop_id=s.shop_id;
END;
