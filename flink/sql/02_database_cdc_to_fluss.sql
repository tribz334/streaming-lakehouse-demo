SET 'execution.runtime-mode'='streaming';
SET 'execution.checkpointing.interval'='30s';
SET 'pipeline.name'='mysql-cdc-to-fluss-ods-and-dim';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'table.exec.sink.upsert-materialize'='NONE';

CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');

CREATE TEMPORARY TABLE mysql_advertiser (
  advertiser_id BIGINT,advertiser_name STRING,qualification_type INT,status INT,
  industry_l1_id BIGINT,industry_l1_name STRING,industry_l2_id BIGINT,industry_l2_name STRING,
  created_at TIMESTAMP(3),updated_at TIMESTAMP(3),PRIMARY KEY(advertiser_id) NOT ENFORCED
) WITH ('connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root','password'='root','database-name'='ad_ods','table-name'='advertiser_info','server-id'='5411-5418','server-time-zone'='UTC','scan.startup.mode'='initial');

CREATE TEMPORARY TABLE mysql_campaign (
  campaign_id BIGINT,campaign_name STRING,advertiser_id BIGINT,status INT,market_goal INT,
  trading_mode INT,budget BIGINT,daily_budget BIGINT,created_at TIMESTAMP(3),
  updated_at TIMESTAMP(3),PRIMARY KEY(campaign_id) NOT ENFORCED
) WITH ('connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root','password'='root','database-name'='ad_ods','table-name'='campaign_info','server-id'='5421-5428','server-time-zone'='UTC','scan.startup.mode'='initial');

CREATE TEMPORARY TABLE mysql_unit (
  unit_id BIGINT,unit_name STRING,campaign_id BIGINT,status INT,is_closed INT,placement_type INT,ad_type INT,
  search_keyword STRING,product_id BIGINT,landing_page_url STRING,audience STRING,
  start_date TIMESTAMP(3),end_date TIMESTAMP(3),daily_budget BIGINT,bid_type STRING,bid BIGINT,
  created_at TIMESTAMP(3),updated_at TIMESTAMP(3),PRIMARY KEY(unit_id) NOT ENFORCED
) WITH ('connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root','password'='root','database-name'='ad_ods','table-name'='unit_info','server-id'='5431-5438','server-time-zone'='UTC','scan.startup.mode'='initial');

CREATE TEMPORARY TABLE mysql_creative (
  creative_id BIGINT,creative_name STRING,unit_id BIGINT,status INT,creative_mode INT,
  material_mode INT,creative_title STRING,creative_category STRING,creative_tags STRING,
  creative_text STRING,creative_image_urls STRING,creative_video_id BIGINT,monitoring_url STRING,
  created_at TIMESTAMP(3),updated_at TIMESTAMP(3),PRIMARY KEY(creative_id) NOT ENFORCED
) WITH ('connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root','password'='root','database-name'='ad_ods','table-name'='creative_info','server-id'='5441-5448','server-time-zone'='UTC','scan.startup.mode'='initial');

CREATE TEMPORARY TABLE mysql_user (
  uid BIGINT,user_name STRING,gender INT,phone_hash STRING,email STRING,user_level INT,
  birthday DATE,status INT,created_at TIMESTAMP(3),updated_at TIMESTAMP(3),PRIMARY KEY(uid) NOT ENFORCED
) WITH ('connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root','password'='root','database-name'='ad_ods','table-name'='user_info','server-id'='5451-5458','server-time-zone'='UTC','scan.startup.mode'='initial');

CREATE TEMPORARY TABLE mysql_shop (
  shop_id BIGINT,shop_name STRING,shop_type INT,status INT,main_category_id BIGINT,
  main_category_name STRING,shop_qualification_type INT,credit_code STRING,contact_person STRING,
  contact_phone STRING,created_at TIMESTAMP(3),updated_at TIMESTAMP(3),PRIMARY KEY(shop_id) NOT ENFORCED
) WITH ('connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root','password'='root','database-name'='ad_ods','table-name'='shop_info','server-id'='5461-5468','server-time-zone'='UTC','scan.startup.mode'='initial');

CREATE TEMPORARY TABLE mysql_product (
  product_id BIGINT,product_name STRING,shop_id BIGINT,price BIGINT,created_at TIMESTAMP(3),
  updated_at TIMESTAMP(3),PRIMARY KEY(product_id) NOT ENFORCED
) WITH ('connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root','password'='root','database-name'='ad_ods','table-name'='product_info','server-id'='5471-5478','server-time-zone'='UTC','scan.startup.mode'='initial');

CREATE TEMPORARY TABLE mysql_bill (
  bill_id BIGINT,advertiser_id BIGINT,campaign_id BIGINT,unit_id BIGINT,creative_id BIGINT,
  user_id BIGINT,slot_id BIGINT,billing_type TINYINT,cost BIGINT,bill_time TIMESTAMP(3),
  updated_at TIMESTAMP(3),PRIMARY KEY(bill_id) NOT ENFORCED
) WITH ('connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root','password'='root','database-name'='ad_ods','table-name'='bill_detail','server-id'='5481-5488','server-time-zone'='UTC','scan.startup.mode'='initial');

CREATE TEMPORARY TABLE mysql_order (
  order_id BIGINT,user_id BIGINT,product_id BIGINT,shop_id BIGINT,
  product_price DECIMAL(18,2),product_num INT,total_amount DECIMAL(18,2),payment_method INT,
  receiver_name STRING,receiver_phone STRING,shipping_address STRING,tracking_number STRING,
  order_status INT,create_time TIMESTAMP(3),cancel_time TIMESTAMP(3),pay_time TIMESTAMP(3),
  confirm_time TIMESTAMP(3),refund_time TIMESTAMP(3),updated_at TIMESTAMP(3),
  PRIMARY KEY(order_id) NOT ENFORCED
) WITH ('connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root','password'='root','database-name'='ad_ods','table-name'='order_detail','server-id'='5491-5498','server-time-zone'='UTC','scan.startup.mode'='initial');

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO fluss.ad_dw.dim_advertiser_df
  SELECT advertiser_id,advertiser_name,qualification_type,status,industry_l1_id,industry_l1_name,
    industry_l2_id,industry_l2_name,DATE_FORMAT(created_at,'yyyy-MM-dd'),'9999-12-31',
    CAST(created_at AS STRING),CAST(updated_at AS STRING) FROM mysql_advertiser;

  INSERT INTO fluss.ad_dw.dim_campaign_df
  SELECT c.campaign_id,c.campaign_name,c.advertiser_id,a.advertiser_name,c.status,c.market_goal,
    c.trading_mode,c.budget,c.daily_budget,CAST(c.created_at AS STRING),
    CAST(c.updated_at AS STRING) FROM mysql_campaign c LEFT JOIN mysql_advertiser a
    ON c.advertiser_id=a.advertiser_id;

  INSERT INTO fluss.ad_dw.dim_unit_df
  SELECT u.unit_id,u.unit_name,u.campaign_id,c.campaign_name,u.status,u.is_closed,u.placement_type,u.ad_type,
    JSON_QUERY(u.search_keyword,'$' RETURNING ARRAY<STRING>),u.product_id,u.landing_page_url,
    u.audience,CAST(u.start_date AS STRING),CAST(u.end_date AS STRING),u.daily_budget,u.bid_type,
    u.bid,CAST(u.created_at AS STRING),CAST(u.updated_at AS STRING)
  FROM mysql_unit u LEFT JOIN mysql_campaign c ON u.campaign_id=c.campaign_id;

  INSERT INTO fluss.ad_dw.dim_creative_df
  SELECT cr.creative_id,cr.creative_name,cr.unit_id,u.unit_name,cr.status,cr.creative_mode,
    cr.material_mode,cr.creative_title,cr.creative_category,
    JSON_QUERY(cr.creative_tags,'$' RETURNING ARRAY<STRING>),cr.creative_text,
    cr.creative_image_urls,cr.creative_video_id,cr.monitoring_url,
    CAST(cr.created_at AS STRING),CAST(cr.updated_at AS STRING)
  FROM mysql_creative cr LEFT JOIN mysql_unit u ON cr.unit_id=u.unit_id;

  INSERT INTO fluss.ad_dw.dim_user_df
  SELECT uid,user_name,gender,phone_hash,email,user_level,CAST(birthday AS STRING),status,
    DATE_FORMAT(created_at,'yyyy-MM-dd'),'9999-12-31',CAST(created_at AS STRING),
    CAST(updated_at AS STRING) FROM mysql_user;

  INSERT INTO fluss.ad_dw.dim_shop_df
  SELECT shop_id,shop_name,shop_type,status,main_category_id,main_category_name,
    shop_qualification_type,credit_code,contact_person,contact_phone,
    DATE_FORMAT(created_at,'yyyy-MM-dd'),'9999-12-31',CAST(created_at AS STRING),
    CAST(updated_at AS STRING) FROM mysql_shop;

  INSERT INTO fluss.ad_dw.dim_product_df
  SELECT p.product_id,p.product_name,p.shop_id,s.shop_name,p.price,
    DATE_FORMAT(p.created_at,'yyyy-MM-dd'),'9999-12-31',CAST(p.created_at AS STRING),
    CAST(p.updated_at AS STRING) FROM mysql_product p LEFT JOIN mysql_shop s ON p.shop_id=s.shop_id;

  INSERT INTO fluss.ad_dw.ods_mysql_bill_di
  SELECT bill_id,advertiser_id,campaign_id,unit_id,creative_id,user_id,slot_id,cost,
    CAST(bill_time AS STRING),CAST(updated_at AS STRING),DATE_FORMAT(bill_time,'yyyy-MM-dd')
  FROM mysql_bill;

  INSERT INTO fluss.ad_dw.ods_mysql_order_acc
  SELECT order_id,user_id,product_id,shop_id,CAST(product_price AS BIGINT),
    product_num,CAST(total_amount AS BIGINT),payment_method,receiver_name,receiver_phone,
    shipping_address,tracking_number,order_status,CAST(create_time AS STRING),
    CAST(cancel_time AS STRING),CAST(pay_time AS STRING),CAST(confirm_time AS STRING),
    CAST(refund_time AS STRING),
    CAST(updated_at AS STRING),DATE_FORMAT(create_time,'yyyy-MM-dd') FROM mysql_order;
END;
