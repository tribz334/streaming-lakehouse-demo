USE ad_ods;

INSERT INTO advertiser(advertiser_id, advertiser_name, industry, tier, home_region, signup_date) VALUES
('adv_001','Advertiser 01','ecommerce','KA','Zhejiang','2026-04-01'),
('adv_002','Advertiser 02','game','SMB','Beijing','2026-04-02'),
('adv_003','Advertiser 03','education','Growth','Guangdong','2026-04-03'),
('adv_004','Advertiser 04','local_service','KA','Sichuan','2026-04-04'),
('adv_005','Advertiser 05','beauty','SMB','Hubei','2026-04-05'),
('adv_006','Advertiser 06','ecommerce','Growth','Jiangsu','2026-04-06'),
('adv_007','Advertiser 07','game','KA','Hebei','2026-04-07'),
('adv_008','Advertiser 08','education','SMB','Fujian','2026-04-08'),
('adv_009','Advertiser 09','local_service','Growth','Chongqing','2026-04-09'),
('adv_010','Advertiser 10','beauty','KA','Henan','2026-04-10'),
('adv_011','Advertiser 11','ecommerce','SMB','Shanghai','2026-04-11'),
('adv_012','Advertiser 12','game','Growth','Tianjin','2026-04-12')
ON DUPLICATE KEY UPDATE
  advertiser_name=VALUES(advertiser_name),
  industry=VALUES(industry),
  tier=VALUES(tier),
  home_region=VALUES(home_region),
  signup_date=VALUES(signup_date);

INSERT INTO campaign(campaign_id, advertiser_id, campaign_name, objective, budget, status) VALUES
('cmp_001_1','adv_001','Campaign 01-1','ROI',50000,'running'),
('cmp_001_2','adv_001','Campaign 01-2','GMV',30000,'running'),
('cmp_002_1','adv_002','Campaign 02-1','CTR',26000,'running'),
('cmp_002_2','adv_002','Campaign 02-2','Retention',22000,'running'),
('cmp_003_1','adv_003','Campaign 03-1','ROI',42000,'running'),
('cmp_004_1','adv_004','Campaign 04-1','GMV',28000,'running'),
('cmp_005_1','adv_005','Campaign 05-1','CTR',24000,'running'),
('cmp_006_1','adv_006','Campaign 06-1','ROI',45000,'running'),
('cmp_007_1','adv_007','Campaign 07-1','GMV',51000,'running'),
('cmp_008_1','adv_008','Campaign 08-1','CTR',17000,'running'),
('cmp_009_1','adv_009','Campaign 09-1','Retention',33000,'running'),
('cmp_010_1','adv_010','Campaign 10-1','ROI',70000,'running'),
('cmp_011_1','adv_011','Campaign 11-1','GMV',21000,'running'),
('cmp_012_1','adv_012','Campaign 12-1','CTR',19000,'running')
ON DUPLICATE KEY UPDATE campaign_name=VALUES(campaign_name);

INSERT INTO `unit`(unit_id, campaign_id, unit_name, bid_type, bid_amount, status) VALUES
('unit_crt_001_1_1','cmp_001_1','Unit 01-1-1','CPC',2.5000,'running'),
('unit_crt_001_2_1','cmp_001_2','Unit 01-2-1','OCPC',3.2000,'running'),
('unit_crt_002_1_1','cmp_002_1','Unit 02-1-1','CPC',1.8000,'running'),
('unit_crt_002_2_1','cmp_002_2','Unit 02-2-1','CPM',18.0000,'running'),
('unit_crt_003_1_1','cmp_003_1','Unit 03-1-1','OCPC',2.9000,'running'),
('unit_crt_004_1_1','cmp_004_1','Unit 04-1-1','CPC',2.1000,'running'),
('unit_crt_005_1_1','cmp_005_1','Unit 05-1-1','CPM',16.0000,'running'),
('unit_crt_006_1_1','cmp_006_1','Unit 06-1-1','OCPC',3.5000,'running'),
('unit_crt_007_1_1','cmp_007_1','Unit 07-1-1','CPC',2.7000,'running'),
('unit_crt_008_1_1','cmp_008_1','Unit 08-1-1','CPM',15.0000,'running'),
('unit_crt_009_1_1','cmp_009_1','Unit 09-1-1','OCPC',3.1000,'running'),
('unit_crt_010_1_1','cmp_010_1','Unit 10-1-1','CPC',3.8000,'running'),
('unit_crt_011_1_1','cmp_011_1','Unit 11-1-1','OCPC',2.6000,'running'),
('unit_crt_012_1_1','cmp_012_1','Unit 12-1-1','CPM',17.0000,'running')
ON DUPLICATE KEY UPDATE unit_name=VALUES(unit_name), bid_amount=VALUES(bid_amount);

INSERT INTO creative(creative_id, campaign_id, unit_id, creative_name, format) VALUES
('crt_001_1_1','cmp_001_1','unit_crt_001_1_1','Creative 01-1-1','short_video'),
('crt_001_2_1','cmp_001_2','unit_crt_001_2_1','Creative 01-2-1','feed_card'),
('crt_002_1_1','cmp_002_1','unit_crt_002_1_1','Creative 02-1-1','short_video'),
('crt_002_2_1','cmp_002_2','unit_crt_002_2_1','Creative 02-2-1','live_room'),
('crt_003_1_1','cmp_003_1','unit_crt_003_1_1','Creative 03-1-1','feed_card'),
('crt_004_1_1','cmp_004_1','unit_crt_004_1_1','Creative 04-1-1','short_video'),
('crt_005_1_1','cmp_005_1','unit_crt_005_1_1','Creative 05-1-1','live_room'),
('crt_006_1_1','cmp_006_1','unit_crt_006_1_1','Creative 06-1-1','feed_card'),
('crt_007_1_1','cmp_007_1','unit_crt_007_1_1','Creative 07-1-1','short_video'),
('crt_008_1_1','cmp_008_1','unit_crt_008_1_1','Creative 08-1-1','feed_card'),
('crt_009_1_1','cmp_009_1','unit_crt_009_1_1','Creative 09-1-1','live_room'),
('crt_010_1_1','cmp_010_1','unit_crt_010_1_1','Creative 10-1-1','short_video'),
('crt_011_1_1','cmp_011_1','unit_crt_011_1_1','Creative 11-1-1','feed_card'),
('crt_012_1_1','cmp_012_1','unit_crt_012_1_1','Creative 12-1-1','live_room')
ON DUPLICATE KEY UPDATE creative_name=VALUES(creative_name);
