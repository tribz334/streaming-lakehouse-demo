-- SDK logs persist in ods_log_inc and are parsed into DWD actions. Database
-- billing changes are captured directly from MySQL binlog by Flink CDC.
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'pipeline.name' = 'ods-log-and-dwd-ad-facts';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'parallelism.default' = '1';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://hive-metastore:9083',
  'warehouse' = 'file:///warehouse/paimon'
);

CREATE TEMPORARY TABLE kafka_ods_log (
  bus_id INT,
  app_id INT,
  log_id INT,
  msg_id BIGINT,
  common ROW<
    uid BIGINT,
    area STRING,
    ip STRING,
    device_id BIGINT,
    platform INT,
    app_version STRING,
    browser_version STRING,
    sdk_version STRING
  >,
  actions ARRAY<ROW<
    event_id BIGINT,
    action STRING,
    creative_id BIGINT,
    product_id BIGINT,
    slot_id BIGINT,
    media STRING,
    commerce_scene STRING,
    traffic_type STRING,
    play_during BIGINT,
    ts BIGINT
  >>,
  ts BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_log',
  'properties.bootstrap.servers' = 'kafka-node-1:9092',
  'properties.group.id' = 'ods-log-to-paimon-v1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false'
);

CREATE TEMPORARY TABLE mysql_ad_bill (
  bill_id BIGINT,advertiser_id BIGINT,campaign_id BIGINT,unit_id BIGINT,
  creative_id BIGINT,user_id BIGINT,slot_id BIGINT,billing_type TINYINT,
  media STRING,commerce_scene STRING,cost BIGINT,bill_time TIMESTAMP(3),
  updated_at TIMESTAMP(3),PRIMARY KEY(bill_id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='mysql','port'='3306','username'='root',
  'password'='root','database-name'='ad_ods','table-name'='bill_detail',
  'server-id'='5521-5528','server-time-zone'='UTC','scan.startup.mode'='initial'
);

CREATE TEMPORARY VIEW flattened_actions AS
SELECT
  r.common.uid AS uid,
  CAST(r.common.device_id AS STRING) AS device_id,
  r.common.platform AS platform,
  r.common.app_version AS app_vc,
  r.common.browser_version AS browser_vc,
  r.common.sdk_version AS sdk_vc,
  a.creative_id,
  a.slot_id,
  CASE a.action
    WHEN 'send' THEN 'delivery'
    WHEN 'show' THEN 'impression'
    WHEN 'convert' THEN 'conversion'
    ELSE a.action
  END AS action_type,
  a.play_during,
  a.ts
FROM paimon.ad_dw.ods_log_inc r
CROSS JOIN UNNEST(r.actions) AS a (
  event_id, action, creative_id, product_id, slot_id, media,
  commerce_scene, traffic_type, play_during, ts
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO paimon.ad_dw.ods_log_inc
  SELECT msg_id, bus_id, app_id, log_id, common, actions, ts,
    DATE_FORMAT(TO_TIMESTAMP_LTZ(ts, 3), 'yyyy-MM-dd')
  FROM kafka_ods_log;

  INSERT INTO paimon.ad_dw.dwd_ad_action_log_inc
  SELECT uid, device_id, platform, app_vc, browser_vc, sdk_vc,
    creative_id, slot_id, action_type, play_during, ts,
    DATE_FORMAT(TO_TIMESTAMP_LTZ(ts, 3), 'yyyy-MM-dd'),
    DATE_FORMAT(TO_TIMESTAMP_LTZ(ts, 3), 'HH')
  FROM flattened_actions
  UNION ALL
  SELECT uid, device_id, platform, app_vc, browser_vc, sdk_vc,
    creative_id, slot_id, 'delivery', CAST(0 AS BIGINT), ts,
    DATE_FORMAT(TO_TIMESTAMP_LTZ(ts, 3), 'yyyy-MM-dd'),
    DATE_FORMAT(TO_TIMESTAMP_LTZ(ts, 3), 'HH')
  FROM flattened_actions
  WHERE action_type = 'impression' AND COALESCE(sdk_vc, 'legacy') <> '3.3.0';

  INSERT INTO paimon.ad_dw.dwd_ad_bill_detail_inc
  SELECT creative_id,unit_id,campaign_id,slot_id,user_id,cost,billing_type,
    UNIX_TIMESTAMP(DATE_FORMAT(bill_time,'yyyy-MM-dd HH:mm:ss'))*1000,
    DATE_FORMAT(bill_time,'yyyy-MM-dd'),DATE_FORMAT(bill_time,'HH')
  FROM mysql_ad_bill;
END;
