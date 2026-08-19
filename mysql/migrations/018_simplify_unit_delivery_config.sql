USE ad_ods;

ALTER TABLE unit_info
  ADD COLUMN `推广落地页网址` VARCHAR(255) NULL AFTER `投放位置`,
  RENAME COLUMN `定向设置` TO `目标人群`,
  ADD COLUMN `单日预算JSON` JSON NULL AFTER `单日预算`;

UPDATE unit_info
SET
  `投放位置` = CASE
    WHEN `投放位置` IN ('快手联盟', '联盟') THEN '联盟'
    WHEN MOD(CRC32(unit_id), 2) = 0 THEN '主站'
    ELSE '联盟'
  END,
  `推广落地页网址` = CONCAT(
    'https://landing.example.com/',
    CASE `推广落地页`
      WHEN '建站落地页' THEN 'site/'
      WHEN '程序化落地页' THEN 'programmatic/'
      ELSE 'custom/'
    END,
    COALESCE(`落地页ID`, unit_id)
  ),
  `单日预算JSON` = CASE `单日预算模式`
    WHEN '统一预算' THEN JSON_OBJECT('每日', COALESCE(`单日预算`, 0))
    WHEN '分日预算' THEN JSON_OBJECT(
      '周一', ROUND(COALESCE(`单日预算`, 0) * 0.85, 2),
      '周二', ROUND(COALESCE(`单日预算`, 0) * 0.90, 2),
      '周三', ROUND(COALESCE(`单日预算`, 0) * 0.95, 2),
      '周四', ROUND(COALESCE(`单日预算`, 0) * 1.00, 2),
      '周五', ROUND(COALESCE(`单日预算`, 0) * 1.10, 2),
      '周六', ROUND(COALESCE(`单日预算`, 0) * 1.25, 2),
      '周日', ROUND(COALESCE(`单日预算`, 0) * 1.15, 2)
    )
    ELSE NULL
  END;

ALTER TABLE unit_info
  MODIFY COLUMN `推广落地页网址` VARCHAR(255) NOT NULL,
  DROP COLUMN `推广落地页`,
  DROP COLUMN `落地页ID`,
  DROP COLUMN `投放类型`,
  DROP COLUMN `定向方式`,
  DROP COLUMN `定向模板ID`,
  DROP COLUMN `单日预算`,
  RENAME COLUMN `单日预算JSON` TO `单日预算`;
