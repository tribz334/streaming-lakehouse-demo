USE ad_ods;

UPDATE bill_detail
SET bill_id = SUBSTRING(bill_id, 6)
WHERE bill_id LIKE 'bill\_%';
