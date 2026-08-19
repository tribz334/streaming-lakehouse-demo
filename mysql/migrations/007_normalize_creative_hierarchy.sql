USE ad_ods;

-- campaign_id is derivable through creative_info.unit_id -> unit_info.campaign_id.
ALTER TABLE creative_info
  DROP FOREIGN KEY fk_creative_campaign,
  DROP COLUMN campaign_id;
