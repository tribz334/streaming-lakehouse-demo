USE ad_ods;

-- Migration runners execute every file on every invocation. Keep a durable
-- marker because the legacy 1/2/3 placement mapping is a permutation and
-- cannot be identified safely from the values alone after it is converted.
CREATE TABLE IF NOT EXISTS demo_schema_migrations (
  version VARCHAR(64) PRIMARY KEY,
  applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

SET @remap_placement_039 = (
  SELECT CASE
    WHEN EXISTS (
      SELECT 1 FROM demo_schema_migrations
      WHERE version='039_placement_type_paper_mapping'
    ) THEN 0
    WHEN EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='ad_ods'
        AND table_name='unit_info'
        AND column_name='placement_type'
        AND column_comment LIKE '%1-feed,2-search,3-splash%'
    ) THEN 0
    ELSE 1
  END
);

-- Legacy: 1-search, 2-splash, 3-feed.
-- Thesis: 1-feed, 2-search, 3-splash. Values 4/5/6 are unchanged.
UPDATE unit_info
SET placement_type = CASE placement_type
  WHEN 1 THEN 2
  WHEN 2 THEN 3
  WHEN 3 THEN 1
  ELSE placement_type
END
WHERE @remap_placement_039=1 AND placement_type BETWEEN 1 AND 3;

-- Record the value permutation immediately after it succeeds. If a later DDL
-- or seed statement fails, retrying 039 must not apply the permutation twice.
INSERT IGNORE INTO demo_schema_migrations(version)
VALUES ('039_placement_type_paper_mapping');

ALTER TABLE unit_info
  MODIFY COLUMN placement_type INT NOT NULL DEFAULT 6
  COMMENT '1-feed,2-search,3-splash,4-rewarded,5-banner,6-other';

ALTER TABLE unit_info
  MODIFY COLUMN is_closed INT NOT NULL DEFAULT 0
  COMMENT '1 means this unit contributes to closed-loop Cost';

ALTER TABLE order_info
  MODIFY COLUMN refund_time TIMESTAMP NULL
  COMMENT 'refund completion time; populated with order_status=5';

-- The SDK simulator uses the stable namespace 10000001..10012000. Seed the
-- whole range so CDC/order generators always have a sufficiently large user
-- dimension and uid-based attribution can be inspected end to end.
DROP TEMPORARY TABLE IF EXISTS demo_seed_digit_039;
CREATE TEMPORARY TABLE demo_seed_digit_039 (
  digit TINYINT PRIMARY KEY
);
INSERT INTO demo_seed_digit_039(digit)
VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

-- MySQL does not allow the same TEMPORARY table to be reopened through
-- multiple aliases in one statement. Materialize four small copies so the
-- deterministic number generator below works on every supported MySQL 8.x.
CREATE TEMPORARY TABLE demo_seed_digit_039_2 AS SELECT digit FROM demo_seed_digit_039;
CREATE TEMPORARY TABLE demo_seed_digit_039_3 AS SELECT digit FROM demo_seed_digit_039;
CREATE TEMPORARY TABLE demo_seed_digit_039_4 AS SELECT digit FROM demo_seed_digit_039;
CREATE TEMPORARY TABLE demo_seed_digit_039_5 AS SELECT digit FROM demo_seed_digit_039;

INSERT IGNORE INTO user_info (
  uid,user_name,gender,phone_hash,email,user_level,birthday,status,created_at
)
SELECT
  10000000+n.seq,
  CONCAT('demo_user_',LPAD(n.seq,5,'0')),
  MOD(n.seq,3),
  SHA2(CONCAT('demo-phone-',n.seq),256),
  CONCAT('demo_user_',LPAD(n.seq,5,'0'),'@example.test'),
  MOD(n.seq,6),
  DATE_ADD('1980-01-01',INTERVAL MOD(n.seq,10000) DAY),
  0,
  TIMESTAMP('2026-01-01 00:00:00')
FROM (
  SELECT 1+ones.digit+10*tens.digit+100*hundreds.digit
    +1000*thousands.digit+10000*ten_thousands.digit AS seq
  FROM demo_seed_digit_039 ones
  CROSS JOIN demo_seed_digit_039_2 tens
  CROSS JOIN demo_seed_digit_039_3 hundreds
  CROSS JOIN demo_seed_digit_039_4 thousands
  CROSS JOIN demo_seed_digit_039_5 ten_thousands
) n
WHERE n.seq BETWEEN 1 AND 12000;

INSERT IGNORE INTO user_info (
  uid,user_name,gender,phone_hash,email,user_level,birthday,status,created_at
) VALUES
  (90000001,'fraud_demo_1',0,SHA2('fraud-phone-1',256),'fraud_demo_1@example.test',0,'1990-01-01',0,'2026-01-01 00:00:00'),
  (90000002,'fraud_demo_2',1,SHA2('fraud-phone-2',256),'fraud_demo_2@example.test',0,'1990-01-02',0,'2026-01-01 00:00:00'),
  (90000003,'fraud_demo_3',2,SHA2('fraud-phone-3',256),'fraud_demo_3@example.test',0,'1990-01-03',0,'2026-01-01 00:00:00');

DROP TEMPORARY TABLE demo_seed_digit_039_5;
DROP TEMPORARY TABLE demo_seed_digit_039_4;
DROP TEMPORARY TABLE demo_seed_digit_039_3;
DROP TEMPORARY TABLE demo_seed_digit_039_2;
DROP TEMPORARY TABLE demo_seed_digit_039;
