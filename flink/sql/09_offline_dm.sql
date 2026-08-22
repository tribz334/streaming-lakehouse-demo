SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.dm_creative;
TRUNCATE TABLE paimon.ad_dw.dm_unit;
TRUNCATE TABLE paimon.ad_dw.dm_campaign;
TRUNCATE TABLE paimon.ad_dw.dm_advertiser;

CREATE TEMPORARY VIEW all_metric_daily AS
SELECT 'creative' AS entity_type, creative_id AS entity_id,
  delivery_cnt, impression_cnt, click_cnt, conversion_cnt, cost,
  order_pay_cnt, order_pay_gmv, order_refund_cnt, order_refund_gmv,
  order_valid_cnt, order_valid_gmv, dt
FROM paimon.ad_dw.dws_creative
UNION ALL
SELECT 'unit', unit_id, delivery_cnt, impression_cnt, click_cnt, conversion_cnt, cost,
  order_pay_cnt, order_pay_gmv, order_refund_cnt, order_refund_gmv,
  order_valid_cnt, order_valid_gmv, dt
FROM paimon.ad_dw.dws_unit
UNION ALL
SELECT 'campaign', campaign_id, delivery_cnt, impression_cnt, click_cnt, conversion_cnt, cost,
  order_pay_cnt, order_pay_gmv, order_refund_cnt, order_refund_gmv,
  order_valid_cnt, order_valid_gmv, dt
FROM paimon.ad_dw.dws_campaign
UNION ALL
SELECT 'advertiser', advertiser_id, delivery_cnt, impression_cnt, click_cnt, conversion_cnt, cost,
  order_pay_cnt, order_pay_gmv, order_refund_cnt, order_refund_gmv,
  order_valid_cnt, order_valid_gmv, dt
FROM paimon.ad_dw.dws_advertiser;

CREATE TEMPORARY VIEW all_metric_rolling AS
SELECT d.entity_type, d.entity_id,
  SUM(CASE WHEN h.dt=d.dt THEN h.delivery_cnt ELSE 0 END) AS delivery_cnt_1d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '6' DAY THEN h.delivery_cnt ELSE 0 END) AS delivery_cnt_7d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '29' DAY THEN h.delivery_cnt ELSE 0 END) AS delivery_cnt_30d,
  SUM(h.delivery_cnt) AS delivery_cnt_acc,
  SUM(CASE WHEN h.dt=d.dt THEN h.impression_cnt ELSE 0 END) AS impression_cnt_1d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '6' DAY THEN h.impression_cnt ELSE 0 END) AS impression_cnt_7d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '29' DAY THEN h.impression_cnt ELSE 0 END) AS impression_cnt_30d,
  SUM(h.impression_cnt) AS impression_cnt_acc,
  SUM(CASE WHEN h.dt=d.dt THEN h.click_cnt ELSE 0 END) AS click_cnt_1d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '6' DAY THEN h.click_cnt ELSE 0 END) AS click_cnt_7d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '29' DAY THEN h.click_cnt ELSE 0 END) AS click_cnt_30d,
  SUM(h.click_cnt) AS click_cnt_acc,
  SUM(CASE WHEN h.dt=d.dt THEN h.conversion_cnt ELSE 0 END) AS conversion_cnt_1d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '6' DAY THEN h.conversion_cnt ELSE 0 END) AS conversion_cnt_7d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '29' DAY THEN h.conversion_cnt ELSE 0 END) AS conversion_cnt_30d,
  SUM(h.conversion_cnt) AS conversion_cnt_acc,
  MIN(CASE WHEN h.cost>0 THEN h.dt END) AS cost_first_date,
  MAX(CASE WHEN h.cost>0 THEN h.dt END) AS cost_last_date,
  SUM(CASE WHEN h.dt=d.dt THEN h.cost ELSE 0 END) AS cost_1d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '6' DAY THEN h.cost ELSE 0 END) AS cost_7d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '29' DAY THEN h.cost ELSE 0 END) AS cost_30d,
  SUM(h.cost) AS cost_acc,
  MIN(CASE WHEN h.order_pay_cnt>0 THEN h.dt END) AS order_pay_first_date,
  MAX(CASE WHEN h.order_pay_cnt>0 THEN h.dt END) AS order_pay_last_date,
  SUM(CASE WHEN h.dt=d.dt THEN h.order_pay_cnt ELSE 0 END) AS order_pay_cnt_1d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '6' DAY THEN h.order_pay_cnt ELSE 0 END) AS order_pay_cnt_7d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '29' DAY THEN h.order_pay_cnt ELSE 0 END) AS order_pay_cnt_30d,
  SUM(h.order_pay_cnt) AS order_pay_cnt_acc,
  SUM(CASE WHEN h.dt=d.dt THEN h.order_pay_gmv ELSE 0 END) AS order_pay_gmv_1d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '6' DAY THEN h.order_pay_gmv ELSE 0 END) AS order_pay_gmv_7d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '29' DAY THEN h.order_pay_gmv ELSE 0 END) AS order_pay_gmv_30d,
  SUM(h.order_pay_gmv) AS order_pay_gmv_acc,
  MIN(CASE WHEN h.order_refund_cnt>0 THEN h.dt END) AS order_refund_first_date,
  MAX(CASE WHEN h.order_refund_cnt>0 THEN h.dt END) AS order_refund_last_date,
  SUM(CASE WHEN h.dt=d.dt THEN h.order_refund_cnt ELSE 0 END) AS order_refund_cnt_1d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '6' DAY THEN h.order_refund_cnt ELSE 0 END) AS order_refund_cnt_7d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '29' DAY THEN h.order_refund_cnt ELSE 0 END) AS order_refund_cnt_30d,
  SUM(h.order_refund_cnt) AS order_refund_cnt_acc,
  SUM(CASE WHEN h.dt=d.dt THEN h.order_refund_gmv ELSE 0 END) AS order_refund_gmv_1d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '6' DAY THEN h.order_refund_gmv ELSE 0 END) AS order_refund_gmv_7d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '29' DAY THEN h.order_refund_gmv ELSE 0 END) AS order_refund_gmv_30d,
  SUM(h.order_refund_gmv) AS order_refund_gmv_acc,
  MIN(CASE WHEN h.order_valid_cnt>0 THEN h.dt END) AS order_valid_first_date,
  MAX(CASE WHEN h.order_valid_cnt>0 THEN h.dt END) AS order_valid_last_date,
  SUM(CASE WHEN h.dt=d.dt THEN h.order_valid_cnt ELSE 0 END) AS order_valid_cnt_1d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '6' DAY THEN h.order_valid_cnt ELSE 0 END) AS order_valid_cnt_7d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '29' DAY THEN h.order_valid_cnt ELSE 0 END) AS order_valid_cnt_30d,
  SUM(h.order_valid_cnt) AS order_valid_cnt_acc,
  SUM(CASE WHEN h.dt=d.dt THEN h.order_valid_gmv ELSE 0 END) AS order_valid_gmv_1d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '6' DAY THEN h.order_valid_gmv ELSE 0 END) AS order_valid_gmv_7d,
  SUM(CASE WHEN CAST(h.dt AS DATE)>=CAST(d.dt AS DATE)-INTERVAL '29' DAY THEN h.order_valid_gmv ELSE 0 END) AS order_valid_gmv_30d,
  SUM(h.order_valid_gmv) AS order_valid_gmv_acc,
  d.dt
FROM all_metric_daily d
JOIN all_metric_daily h
  ON d.entity_type=h.entity_type AND d.entity_id=h.entity_id
 AND CAST(h.dt AS DATE)<=CAST(d.dt AS DATE)
GROUP BY d.entity_type, d.entity_id, d.dt;

-- The four inserts share the same ordered metric contract; only the key changes.
INSERT INTO paimon.ad_dw.dm_creative SELECT entity_id,
  delivery_cnt_1d,delivery_cnt_7d,delivery_cnt_30d,delivery_cnt_acc,
  impression_cnt_1d,impression_cnt_7d,impression_cnt_30d,impression_cnt_acc,
  click_cnt_1d,click_cnt_7d,click_cnt_30d,click_cnt_acc,
  conversion_cnt_1d,conversion_cnt_7d,conversion_cnt_30d,conversion_cnt_acc,
  cost_first_date,cost_last_date,cost_1d,cost_7d,cost_30d,cost_acc,
  order_pay_first_date,order_pay_last_date,order_pay_cnt_1d,order_pay_cnt_7d,order_pay_cnt_30d,order_pay_cnt_acc,
  order_pay_gmv_1d,order_pay_gmv_7d,order_pay_gmv_30d,order_pay_gmv_acc,
  order_refund_first_date,order_refund_last_date,order_refund_cnt_1d,order_refund_cnt_7d,order_refund_cnt_30d,order_refund_cnt_acc,
  order_refund_gmv_1d,order_refund_gmv_7d,order_refund_gmv_30d,order_refund_gmv_acc,
  order_valid_first_date,order_valid_last_date,order_valid_cnt_1d,order_valid_cnt_7d,order_valid_cnt_30d,order_valid_cnt_acc,
  order_valid_gmv_1d,order_valid_gmv_7d,order_valid_gmv_30d,order_valid_gmv_acc,dt
FROM all_metric_rolling WHERE entity_type='creative';

INSERT INTO paimon.ad_dw.dm_unit SELECT entity_id,
  delivery_cnt_1d,delivery_cnt_7d,delivery_cnt_30d,delivery_cnt_acc,
  impression_cnt_1d,impression_cnt_7d,impression_cnt_30d,impression_cnt_acc,
  click_cnt_1d,click_cnt_7d,click_cnt_30d,click_cnt_acc,
  conversion_cnt_1d,conversion_cnt_7d,conversion_cnt_30d,conversion_cnt_acc,
  cost_first_date,cost_last_date,cost_1d,cost_7d,cost_30d,cost_acc,
  order_pay_first_date,order_pay_last_date,order_pay_cnt_1d,order_pay_cnt_7d,order_pay_cnt_30d,order_pay_cnt_acc,
  order_pay_gmv_1d,order_pay_gmv_7d,order_pay_gmv_30d,order_pay_gmv_acc,
  order_refund_first_date,order_refund_last_date,order_refund_cnt_1d,order_refund_cnt_7d,order_refund_cnt_30d,order_refund_cnt_acc,
  order_refund_gmv_1d,order_refund_gmv_7d,order_refund_gmv_30d,order_refund_gmv_acc,
  order_valid_first_date,order_valid_last_date,order_valid_cnt_1d,order_valid_cnt_7d,order_valid_cnt_30d,order_valid_cnt_acc,
  order_valid_gmv_1d,order_valid_gmv_7d,order_valid_gmv_30d,order_valid_gmv_acc,dt
FROM all_metric_rolling WHERE entity_type='unit';

INSERT INTO paimon.ad_dw.dm_campaign SELECT entity_id,
  delivery_cnt_1d,delivery_cnt_7d,delivery_cnt_30d,delivery_cnt_acc,
  impression_cnt_1d,impression_cnt_7d,impression_cnt_30d,impression_cnt_acc,
  click_cnt_1d,click_cnt_7d,click_cnt_30d,click_cnt_acc,
  conversion_cnt_1d,conversion_cnt_7d,conversion_cnt_30d,conversion_cnt_acc,
  cost_first_date,cost_last_date,cost_1d,cost_7d,cost_30d,cost_acc,
  order_pay_first_date,order_pay_last_date,order_pay_cnt_1d,order_pay_cnt_7d,order_pay_cnt_30d,order_pay_cnt_acc,
  order_pay_gmv_1d,order_pay_gmv_7d,order_pay_gmv_30d,order_pay_gmv_acc,
  order_refund_first_date,order_refund_last_date,order_refund_cnt_1d,order_refund_cnt_7d,order_refund_cnt_30d,order_refund_cnt_acc,
  order_refund_gmv_1d,order_refund_gmv_7d,order_refund_gmv_30d,order_refund_gmv_acc,
  order_valid_first_date,order_valid_last_date,order_valid_cnt_1d,order_valid_cnt_7d,order_valid_cnt_30d,order_valid_cnt_acc,
  order_valid_gmv_1d,order_valid_gmv_7d,order_valid_gmv_30d,order_valid_gmv_acc,dt
FROM all_metric_rolling WHERE entity_type='campaign';

INSERT INTO paimon.ad_dw.dm_advertiser SELECT entity_id,
  delivery_cnt_1d,delivery_cnt_7d,delivery_cnt_30d,delivery_cnt_acc,
  impression_cnt_1d,impression_cnt_7d,impression_cnt_30d,impression_cnt_acc,
  click_cnt_1d,click_cnt_7d,click_cnt_30d,click_cnt_acc,
  conversion_cnt_1d,conversion_cnt_7d,conversion_cnt_30d,conversion_cnt_acc,
  cost_first_date,cost_last_date,cost_1d,cost_7d,cost_30d,cost_acc,
  order_pay_first_date,order_pay_last_date,order_pay_cnt_1d,order_pay_cnt_7d,order_pay_cnt_30d,order_pay_cnt_acc,
  order_pay_gmv_1d,order_pay_gmv_7d,order_pay_gmv_30d,order_pay_gmv_acc,
  order_refund_first_date,order_refund_last_date,order_refund_cnt_1d,order_refund_cnt_7d,order_refund_cnt_30d,order_refund_cnt_acc,
  order_refund_gmv_1d,order_refund_gmv_7d,order_refund_gmv_30d,order_refund_gmv_acc,
  order_valid_first_date,order_valid_last_date,order_valid_cnt_1d,order_valid_cnt_7d,order_valid_cnt_30d,order_valid_cnt_acc,
  order_valid_gmv_1d,order_valid_gmv_7d,order_valid_gmv_30d,order_valid_gmv_acc,dt
FROM all_metric_rolling WHERE entity_type='advertiser';
