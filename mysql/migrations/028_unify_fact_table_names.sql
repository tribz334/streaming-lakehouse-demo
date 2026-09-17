USE ad_ods;

DELIMITER //
CREATE PROCEDURE migrate_fact_table_names_028()
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='ad_ods' AND table_name='order_detail'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='ad_ods' AND table_name='order_info'
  ) THEN
    RENAME TABLE order_detail TO order_info;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='ad_ods' AND table_name='bill_detail'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='ad_ods' AND table_name='bill_info'
  ) THEN
    RENAME TABLE bill_detail TO bill_info;
  END IF;
END//
DELIMITER ;

CALL migrate_fact_table_names_028();
DROP PROCEDURE migrate_fact_table_names_028;
