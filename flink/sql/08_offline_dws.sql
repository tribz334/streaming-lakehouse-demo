SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.dws_creative;
TRUNCATE TABLE paimon.ad_dw.dws_unit;
TRUNCATE TABLE paimon.ad_dw.dws_campaign;
TRUNCATE TABLE paimon.ad_dw.dws_advertiser;

INSERT INTO paimon.ad_dw.dws_creative
WITH event_daily AS (
  SELECT
    dt,
    creative_id,
    SUM(CASE WHEN action_type='delivery' THEN 1 ELSE 0 END) AS delivery_cnt,
    SUM(CASE WHEN action_type='impression' THEN 1 ELSE 0 END) AS impression_cnt,
    SUM(CASE WHEN action_type='click' THEN 1 ELSE 0 END) AS click_cnt,
    SUM(CASE WHEN action_type='conversion' THEN 1 ELSE 0 END) AS conversion_cnt
  FROM paimon.ad_dw.dwd_ad_action_log_inc
  WHERE creative_id IS NOT NULL
  GROUP BY dt, creative_id
), bill_daily AS (
  SELECT dt, creative_id, SUM(cost) AS cost
  FROM paimon.ad_dw.dwd_ad_bill_detail_inc
  GROUP BY dt, creative_id
), order_changes AS (
  SELECT creative_id, SUBSTRING(payment_time, 1, 10) AS metric_date,
    1 AS order_pay_cnt, total_amount AS order_pay_gmv,
    0 AS order_refund_cnt, CAST(0 AS BIGINT) AS order_refund_gmv,
    CASE WHEN order_status IN (3,4,7) THEN 1 ELSE 0 END AS order_valid_cnt,
    CASE WHEN order_status IN (3,4,7) THEN total_amount ELSE CAST(0 AS BIGINT) END AS order_valid_gmv
  FROM paimon.ad_dw.dwd_order_detail_acc
  WHERE creative_id IS NOT NULL AND payment_time IS NOT NULL
  UNION ALL
  SELECT creative_id, SUBSTRING(refund_finish_time, 1, 10),
    0, CAST(0 AS BIGINT), 1, total_amount, 0, CAST(0 AS BIGINT)
  FROM paimon.ad_dw.dwd_order_detail_acc
  WHERE creative_id IS NOT NULL AND refund_finish_time IS NOT NULL
), order_daily AS (
  SELECT metric_date AS dt, creative_id,
    SUM(order_pay_cnt) AS order_pay_cnt, SUM(order_pay_gmv) AS order_pay_gmv,
    SUM(order_refund_cnt) AS order_refund_cnt, SUM(order_refund_gmv) AS order_refund_gmv,
    SUM(order_valid_cnt) AS order_valid_cnt, SUM(order_valid_gmv) AS order_valid_gmv
  FROM order_changes
  GROUP BY metric_date, creative_id
)
SELECT
  COALESCE(e.creative_id, b.creative_id, o.creative_id),
  COALESCE(e.delivery_cnt, 0), COALESCE(e.impression_cnt, 0),
  COALESCE(e.click_cnt, 0), COALESCE(e.conversion_cnt, 0),
  COALESCE(b.cost, 0),
  COALESCE(o.order_pay_cnt, 0), COALESCE(o.order_pay_gmv, 0),
  COALESCE(o.order_refund_cnt, 0), COALESCE(o.order_refund_gmv, 0),
  COALESCE(o.order_valid_cnt, 0), COALESCE(o.order_valid_gmv, 0),
  COALESCE(e.dt, b.dt, o.dt)
FROM event_daily e
FULL OUTER JOIN bill_daily b
  ON e.dt = b.dt AND e.creative_id = b.creative_id
FULL OUTER JOIN order_daily o
  ON COALESCE(e.dt, b.dt) = o.dt
 AND COALESCE(e.creative_id, b.creative_id) = o.creative_id;

INSERT INTO paimon.ad_dw.dws_unit
SELECT u.unit_id,
  SUM(d.delivery_cnt), SUM(d.impression_cnt), SUM(d.click_cnt), SUM(d.conversion_cnt), SUM(d.cost),
  SUM(d.order_pay_cnt), SUM(d.order_pay_gmv), SUM(d.order_refund_cnt), SUM(d.order_refund_gmv),
  SUM(d.order_valid_cnt), SUM(d.order_valid_gmv), d.dt
FROM paimon.ad_dw.dws_creative d
JOIN paimon.ad_dw.dim_creative c ON d.creative_id=c.creative_id
JOIN paimon.ad_dw.dim_unit u ON c.unit_id=u.unit_id
GROUP BY u.unit_id, d.dt;

INSERT INTO paimon.ad_dw.dws_campaign
SELECT u.campaign_id,
  SUM(d.delivery_cnt), SUM(d.impression_cnt), SUM(d.click_cnt), SUM(d.conversion_cnt), SUM(d.cost),
  SUM(d.order_pay_cnt), SUM(d.order_pay_gmv), SUM(d.order_refund_cnt), SUM(d.order_refund_gmv),
  SUM(d.order_valid_cnt), SUM(d.order_valid_gmv), d.dt
FROM paimon.ad_dw.dws_unit d
JOIN paimon.ad_dw.dim_unit u ON d.unit_id=u.unit_id
GROUP BY u.campaign_id, d.dt;

INSERT INTO paimon.ad_dw.dws_advertiser
SELECT c.advertiser_id,
  SUM(d.delivery_cnt), SUM(d.impression_cnt), SUM(d.click_cnt), SUM(d.conversion_cnt), SUM(d.cost),
  SUM(d.order_pay_cnt), SUM(d.order_pay_gmv), SUM(d.order_refund_cnt), SUM(d.order_refund_gmv),
  SUM(d.order_valid_cnt), SUM(d.order_valid_gmv), d.dt
FROM paimon.ad_dw.dws_campaign d
JOIN paimon.ad_dw.dim_campaign c ON d.campaign_id=c.campaign_id
GROUP BY c.advertiser_id, d.dt;
