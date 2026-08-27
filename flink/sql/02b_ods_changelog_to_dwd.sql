-- Enrich MySQL billing CDC into the daily DWD billing fact.
-- SDK log parsing, validation and dirty-data splitting are owned by
-- cn.edu.ustc.lakehouse.realtime.job.DwdLogDataStreamJob.
SET 'execution.runtime-mode'='streaming';
SET 'execution.checkpointing.interval'='30s';
SET 'pipeline.name'='fluss-bill-ods-to-dwd';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'table.exec.sink.upsert-materialize'='NONE';
CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');

INSERT INTO fluss.ad_dw.dwd_ad_bill_di
SELECT b.bill_id,b.creative_id,b.unit_id,b.campaign_id,b.slot_id,b.uid,b.advertiser_id,
  u.placement_type,u.ad_type,b.cost,u.is_closed,UNIX_TIMESTAMP(b.bill_time)*1000,
  TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(b.bill_time)*1000,3),b.dt,
  DATE_FORMAT(TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(b.bill_time)*1000,3),'HH')
FROM fluss.ad_dw.ods_mysql_bill_di /*+ OPTIONS('scan.startup.mode'='earliest') */ b
JOIN fluss.ad_dw.dim_unit_df u ON b.unit_id=u.unit_id
WHERE u.placement_type BETWEEN 1 AND 6 AND u.ad_type BETWEEN 1 AND 4;
