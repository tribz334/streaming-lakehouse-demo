USE ad_ods;

ALTER TABLE unit_info
  RENAME COLUMN unit_name TO `广告组名称`,
  RENAME COLUMN bid_type TO `出价方式`,
  RENAME COLUMN bid_amount TO `出价金额`,
  ADD COLUMN `投放位置` VARCHAR(32) NOT NULL DEFAULT '智能优选' AFTER `广告组名称`,
  ADD COLUMN `推广落地页` VARCHAR(32) NOT NULL DEFAULT '程序化落地页' AFTER `投放位置`,
  ADD COLUMN `落地页ID` VARCHAR(64) NULL AFTER `推广落地页`,
  ADD COLUMN `投放类型` VARCHAR(32) NOT NULL DEFAULT '单品投放' AFTER `落地页ID`,
  ADD COLUMN `关联商品ID` VARCHAR(64) NULL AFTER `投放类型`,
  ADD COLUMN `定向方式` VARCHAR(32) NOT NULL DEFAULT '系统默认定向' AFTER `关联商品ID`,
  ADD COLUMN `定向模板ID` VARCHAR(64) NULL AFTER `定向方式`,
  ADD COLUMN `定向设置` JSON NULL AFTER `定向模板ID`,
  ADD COLUMN `投放日期类型` VARCHAR(32) NOT NULL DEFAULT '长期投放' AFTER `定向设置`,
  ADD COLUMN `开始日期` DATE NULL AFTER `投放日期类型`,
  ADD COLUMN `结束日期` DATE NULL AFTER `开始日期`,
  ADD COLUMN `单日预算模式` VARCHAR(32) NOT NULL DEFAULT '不限' AFTER `结束日期`,
  ADD COLUMN `单日预算` DECIMAL(18,2) NULL AFTER `单日预算模式`,
  ADD COLUMN `转化目标` VARCHAR(32) NOT NULL DEFAULT '下单' AFTER `出价金额`;

UPDATE unit_info u
JOIN campaign_info c ON u.campaign_id = c.campaign_id
SET
  u.`投放位置` = ELT(1 + MOD(CRC32(u.unit_id), 3), '智能优选', '快手主站', '快手联盟'),
  u.`推广落地页` = ELT(1 + MOD(CRC32(CONCAT(u.unit_id, '_page')), 3), '建站落地页', '程序化落地页', '自定义'),
  u.`落地页ID` = CONCAT('landing_', RIGHT(u.unit_id, 12)),
  u.`投放类型` = IF(MOD(CRC32(CONCAT(u.unit_id, '_product')), 3) = 0, '动态商品卡投放', '单品投放'),
  u.`关联商品ID` = (SELECT 100000000 + cr.creative_id FROM creative_info cr WHERE cr.unit_id = u.unit_id ORDER BY cr.creative_id LIMIT 1),
  u.`定向方式` = ELT(1 + MOD(CRC32(CONCAT(u.unit_id, '_target')), 3), '系统默认定向', '智能定向', '自定义人群'),
  u.`定向模板ID` = IF(MOD(CRC32(CONCAT(u.unit_id, '_target')), 3) = 2, CONCAT('target_', RIGHT(u.unit_id, 12)), NULL),
  u.`定向设置` = JSON_OBJECT('地区', ELT(1 + MOD(CRC32(CONCAT(u.unit_id, '_region')), 5), '全国', '华东', '华南', '华北', '西南'), '年龄', ELT(1 + MOD(CRC32(CONCAT(u.unit_id, '_age')), 4), '不限', '18-24', '25-34', '35-44'), '性别', ELT(1 + MOD(CRC32(CONCAT(u.unit_id, '_gender')), 3), '不限', '男', '女'), '设备品牌', '不限', '设备价格', '不限'),
  u.`投放日期类型` = IF(MOD(CRC32(CONCAT(u.unit_id, '_date')), 4) = 0, '指定投放周期', '长期投放'),
  u.`开始日期` = DATE(c.updated_at),
  u.`结束日期` = IF(MOD(CRC32(CONCAT(u.unit_id, '_date')), 4) = 0, DATE_ADD(DATE(c.updated_at), INTERVAL 90 DAY), NULL),
  u.`单日预算模式` = ELT(1 + MOD(CRC32(CONCAT(u.unit_id, '_daily_budget')), 3), '不限', '统一预算', '分日预算'),
  u.`单日预算` = IF(MOD(CRC32(CONCAT(u.unit_id, '_daily_budget')), 3) = 0, NULL, ROUND(c.budget / 30, 2)),
  u.`出价方式` = IF(u.`出价方式` = 'CPM', 'oCPM', u.`出价方式`),
  u.`转化目标` = ELT(1 + MOD(CRC32(CONCAT(u.unit_id, '_conversion')), 4), '下单', '支付', '表单提交', '应用激活');

ALTER TABLE unit_info MODIFY COLUMN `开始日期` DATE NOT NULL;
