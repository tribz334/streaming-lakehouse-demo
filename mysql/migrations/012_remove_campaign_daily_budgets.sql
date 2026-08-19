USE ad_ods;

ALTER TABLE campaign_info
  DROP COLUMN monday_budget,
  DROP COLUMN tuesday_budget,
  DROP COLUMN wednesday_budget,
  DROP COLUMN thursday_budget,
  DROP COLUMN friday_budget,
  DROP COLUMN saturday_budget,
  DROP COLUMN sunday_budget;
