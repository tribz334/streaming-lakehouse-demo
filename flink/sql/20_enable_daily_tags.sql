-- Non-destructive migration for already existing DWD tables. CREATE TABLE IF
-- NOT EXISTS does not update table options, so apply daily tags explicitly.
SET 'execution.runtime-mode'='batch';
CREATE CATALOG paimon WITH (
  'type'='paimon','metastore'='hive','uri'='thrift://hive-metastore:9083',
  'warehouse'='file:///warehouse/paimon'
);
USE CATALOG paimon;
USE ad_dw;

ALTER TABLE dwd_ad_action_log_inc SET (
  'tag.automatic-creation'='process-time','tag.creation-period'='daily',
  'tag.num-retained-max'='35','tag.default-time-retained'='30 d'
);
ALTER TABLE dwd_ad_bill_detail_inc SET (
  'tag.automatic-creation'='process-time','tag.creation-period'='daily',
  'tag.num-retained-max'='35','tag.default-time-retained'='30 d'
);
ALTER TABLE dwd_order_detail_acc SET (
  'tag.automatic-creation'='process-time','tag.creation-period'='daily',
  'tag.num-retained-max'='35','tag.default-time-retained'='30 d'
);
