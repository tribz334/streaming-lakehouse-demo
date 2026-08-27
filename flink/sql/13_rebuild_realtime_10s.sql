-- One-time runtime migration for Fluss hot tables and their Paimon lake copies.
-- Stop dependent jobs first. Only derived tables are removed; ODS and DIM stay intact.
CREATE CATALOG fluss WITH (
  'type'='fluss',
  'bootstrap.servers'='fluss-coordinator:9123'
);
USE CATALOG fluss;
USE ad_dw;
DROP TABLE IF EXISTS ads_realtime_metric_30s;
DROP TABLE IF EXISTS ads_realtime_metric_10s;
DROP TABLE IF EXISTS dws_ad_type_di;
DROP TABLE IF EXISTS dws_placement_di;
DROP TABLE IF EXISTS dws_advertiser_di;
DROP TABLE IF EXISTS dws_campaign_di;
DROP TABLE IF EXISTS dwd_ad_event_dirty_di;
DROP TABLE IF EXISTS dwd_ad_bill_di;
DROP TABLE IF EXISTS dwd_ad_order_acc;
DROP TABLE IF EXISTS dwd_ad_order_event_di;
DROP TABLE IF EXISTS dwd_ad_order_di;

-- Drop lake metadata after the Fluss registration is gone; otherwise an old
-- lake schema can be discovered again while the hot table is being removed.
CREATE CATALOG paimon WITH (
  'type'='paimon',
  'metastore'='filesystem',
  'warehouse'='file:///warehouse/paimon'
);
USE CATALOG paimon;
USE ad_dw;
DROP TABLE IF EXISTS ads_realtime_metric_30s;
DROP TABLE IF EXISTS ads_realtime_metric_10s;
DROP TABLE IF EXISTS dws_ad_type_di;
DROP TABLE IF EXISTS dws_placement_di;
DROP TABLE IF EXISTS dws_advertiser_di;
DROP TABLE IF EXISTS dws_campaign_di;
DROP TABLE IF EXISTS dwd_ad_event_dirty_di;
DROP TABLE IF EXISTS dwd_ad_bill_di;
DROP TABLE IF EXISTS dwd_ad_order_acc;
DROP TABLE IF EXISTS dwd_ad_order_event_di;
DROP TABLE IF EXISTS dwd_ad_order_di;
