USE ad_ods;

-- User IDs are identifiers, so they do not repeat the table/entity name.
UPDATE user_info
SET user_id = CASE
  WHEN user_id LIKE 'order\_user\_%' THEN SUBSTRING(user_id, 12)
  WHEN user_id LIKE 'user\_%' THEN SUBSTRING(user_id, 6)
  ELSE user_id
END
WHERE user_id LIKE 'order\_user\_%' OR user_id LIKE 'user\_%';

UPDATE order_detail
SET user_id = CASE
  WHEN user_id LIKE 'order\_user\_%' THEN SUBSTRING(user_id, 12)
  WHEN user_id LIKE 'user\_%' THEN SUBSTRING(user_id, 6)
  ELSE user_id
END
WHERE user_id LIKE 'order\_user\_%' OR user_id LIKE 'user\_%';

UPDATE bill_detail
SET user_id = CASE
  WHEN user_id LIKE 'order\_user\_%' THEN SUBSTRING(user_id, 12)
  WHEN user_id LIKE 'user\_%' THEN SUBSTRING(user_id, 6)
  ELSE user_id
END
WHERE user_id LIKE 'order\_user\_%' OR user_id LIKE 'user\_%';
