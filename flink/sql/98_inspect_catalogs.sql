CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');
SHOW CATALOGS;
USE CATALOG fluss;
SHOW DATABASES;
USE ad_dw;
SHOW TABLES;
DESCRIBE ads_realtime_metric_10s;
DESCRIBE dwd_ad_order_acc;

CREATE CATALOG paimon WITH ('type'='paimon','metastore'='filesystem','warehouse'='file:///warehouse/paimon');
USE CATALOG paimon;
SHOW DATABASES;
USE ad_dw;
SHOW TABLES;
