USE ad_ods;

-- Marketing objective is redundant with the more specific promotion goal.
ALTER TABLE campaign_info DROP COLUMN objective;
