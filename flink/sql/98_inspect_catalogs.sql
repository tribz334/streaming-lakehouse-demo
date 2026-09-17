CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');
SHOW CATALOGS;
USE CATALOG fluss;
SHOW DATABASES;
USE ad_dw;
SHOW TABLES;
DESCRIBE dws_ad_creative_10s;
DESCRIBE dwd_ad_event_di;
DESCRIBE dwd_ad_bill_di;
DESCRIBE dwd_order_acc;

CREATE CATALOG paimon WITH ('type'='paimon','metastore'='filesystem','warehouse'='file:///warehouse/paimon');
USE CATALOG paimon;
SHOW DATABASES;
USE ad_dw;
SHOW TABLES;
