USE ad_ods;

UPDATE order_detail
SET order_id = SUBSTRING(order_id, 5)
WHERE order_id LIKE 'ord\_%';
