-- Enrich append-only ad events and MySQL billing CDC into daily DWD facts.
SET 'execution.runtime-mode'='streaming';
SET 'execution.checkpointing.interval'='30s';
SET 'pipeline.name'='fluss-ods-to-dwd-ad-event-and-bill-di';
SET 'table.local-time-zone'='Asia/Shanghai';
SET 'table.exec.sink.upsert-materialize'='NONE';
CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO fluss.ad_dw.dwd_ad_event_di
  SELECT o.msg_id,CAST(JSON_VALUE(o.common,'$.uid') AS BIGINT),
    JSON_VALUE(o.common,'$.device_id'),CAST(JSON_VALUE(o.common,'$.platform') AS INT),
    JSON_VALUE(o.common,'$.app_version'),JSON_VALUE(o.common,'$.browser_version'),
    JSON_VALUE(o.common,'$.sdk_version'),cp.advertiser_id,u.campaign_id,c.unit_id,
    CAST(JSON_VALUE(o.events,'$[0].creative_id') AS BIGINT),
    CAST(JSON_VALUE(o.events,'$[0].product_id') AS BIGINT),
    CAST(JSON_VALUE(o.events,'$[0].slot_id') AS BIGINT),
    LOWER(JSON_VALUE(o.events,'$[0].event')),JSON_VALUE(o.events,'$[0].scene'),o.ts,
    TO_TIMESTAMP_LTZ(o.ts,3),o.dt,DATE_FORMAT(TO_TIMESTAMP_LTZ(o.ts,3),'HH')
  FROM fluss.ad_dw.ods_log_di /*+ OPTIONS('scan.startup.mode'='earliest') */ o
  LEFT JOIN fluss.ad_dw.dim_creative_df c
    ON CAST(JSON_VALUE(o.events,'$[0].creative_id') AS BIGINT)=c.creative_id
  LEFT JOIN fluss.ad_dw.dim_unit_df u ON c.unit_id=u.unit_id
  LEFT JOIN fluss.ad_dw.dim_campaign_df cp ON u.campaign_id=cp.campaign_id;

  INSERT INTO fluss.ad_dw.dwd_ad_bill_di
  SELECT b.bill_id,b.creative_id,b.unit_id,b.campaign_id,b.slot_id,b.uid,b.advertiser_id,
    b.cost,u.is_closed,UNIX_TIMESTAMP(b.bill_time)*1000,
    TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(b.bill_time)*1000,3),b.dt,
    DATE_FORMAT(TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(b.bill_time)*1000,3),'HH')
  FROM fluss.ad_dw.ods_mysql_bill_di /*+ OPTIONS('scan.startup.mode'='earliest') */ b
  LEFT JOIN fluss.ad_dw.dim_unit_df u ON b.unit_id=u.unit_id;
END;
