USE ad_ods;

-- Convert legacy zero-padded numeric IDs to stable anonymous hashes.
UPDATE user_info
SET user_id = LEFT(SHA2(CONCAT('user:', user_id), 256), 20)
WHERE user_id REGEXP '^[0-9]+$';

UPDATE order_detail
SET user_id = LEFT(SHA2(CONCAT('user:', user_id), 256), 20)
WHERE user_id REGEXP '^[0-9]+$';

UPDATE bill_detail
SET user_id = LEFT(SHA2(CONCAT('user:', user_id), 256), 20)
WHERE user_id REGEXP '^[0-9]+$';
