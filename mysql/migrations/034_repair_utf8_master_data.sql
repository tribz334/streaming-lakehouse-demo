USE ad_ods;
SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- Repair derived Chinese fields after the UTF-8 seed has restored all names.
UPDATE advertiser_info
SET industry_l1_name='广告行业';

UPDATE unit_info
SET search_keyword=JSON_ARRAY(unit_name);

UPDATE creative_info
SET creative_title=creative_name,
    creative_text=CONCAT(creative_name, '，了解更多产品信息。');

UPDATE shop_info s
JOIN advertiser_info a ON s.shop_id=1000+a.advertiser_id
SET s.shop_name=CONCAT(a.advertiser_name, '官方旗舰店'),
    s.contact_person=CONCAT(a.advertiser_name, '运营');

UPDATE product_info p
JOIN creative_info cr ON p.product_id=100000000+cr.creative_id
JOIN unit_info u ON cr.unit_id=u.unit_id
JOIN campaign_info c ON u.campaign_id=c.campaign_id
JOIN advertiser_info a ON c.advertiser_id=a.advertiser_id
SET p.product_name=CONCAT(a.advertiser_name, ' ', cr.creative_name);
