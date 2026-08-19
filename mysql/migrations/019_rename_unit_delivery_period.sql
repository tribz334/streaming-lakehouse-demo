USE ad_ods;

UPDATE unit_info
SET `投放日期类型` = '指定投放周期'
WHERE `投放日期类型` = '设置开始和结束日期';
