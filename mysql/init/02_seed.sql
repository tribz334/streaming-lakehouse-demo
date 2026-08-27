USE ad_ods;

CREATE TEMPORARY TABLE advertiser_seed (
  advertiser_id BIGINT, advertiser_name VARCHAR(128), industry VARCHAR(64),
  tier VARCHAR(32), home_region VARCHAR(64), signup_date DATE
);
INSERT INTO advertiser_seed VALUES
(3,'UNIQLO','apparel','KA','Tokyo','2026-04-03'),
(4,'蕉内','apparel','KA','Guangdong','2026-04-05'),
(2,'Zara','apparel','Growth','Madrid','2026-04-23'),
(1,'UR','apparel','Growth','Guangdong','2026-04-24'),
(6,'比亚迪','automotive','KA','Guangdong','2026-04-15'),
(5,'Tesla','automotive','KA','Texas','2026-04-16'),
(7,'蔚来','automotive','KA','Shanghai','2026-05-08'),
(8,'鸿蒙智行','automotive','Growth','Beijing','2026-05-10'),
(15,'Coca-Cola','beverage','KA','Georgia','2026-04-04'),
(12,'L''Oreal','beauty','KA','Paris','2026-04-10'),
(10,'SK-II','beauty','KA','Tokyo','2026-05-03'),
(13,'Shiseido','beauty','KA','Tokyo','2026-05-04'),
(9,'Lancôme','beauty','KA','Paris','2026-05-05'),
(14,'Estée Lauder','beauty','KA','New York','2026-05-06'),
(11,'YSL Beauty','beauty','KA','Paris','2026-05-07'),
(17,'瑞幸咖啡','coffee','SMB','Fujian','2026-04-12'),
(16,'库迪咖啡','coffee','Growth','Beijing','2026-05-14'),
(20,'小米','consumer_electronics','Growth','Beijing','2026-04-11'),
(18,'Apple','consumer_electronics','KA','California','2026-04-13'),
(19,'华为','consumer_electronics','KA','Guangdong','2026-04-14'),
(22,'天猫','ecommerce','KA','Zhejiang','2026-04-17'),
(21,'京东','ecommerce','KA','Beijing','2026-04-18'),
(26,'腾讯游戏','game','KA','Guangdong','2026-04-08'),
(25,'网易游戏','game','Growth','Zhejiang','2026-04-09'),
(24,'米哈游','game','Growth','Shanghai','2026-04-21'),
(27,'美团','local_service','Growth','Beijing','2026-04-19'),
(29,'Gucci','luxury','KA','Florence','2026-04-28'),
(31,'Louis Vuitton','luxury','KA','Paris','2026-04-29'),
(28,'Dior','luxury','KA','Paris','2026-05-01'),
(30,'Hermes','luxury','KA','Paris','2026-05-02'),
(33,'Nike','sportswear','Growth','Oregon','2026-04-06'),
(32,'Adidas','sportswear','Growth','Bavaria','2026-04-07'),
(36,'OpenAI','technology','KA','California','2026-04-01'),
(35,'NVIDIA','technology','KA','California','2026-04-25'),
(34,'Anthropic','technology','KA','California','2026-04-26'),
(37,'携程','travel','Growth','Shanghai','2026-04-20'),
(38,'去哪儿','travel','Growth','Beijing','2026-05-09'),
(23,'拼多多','ecommerce','KA','Shanghai','2026-05-11'),
(39,'理想','automotive','KA','Beijing','2026-05-15')
;
INSERT INTO advertiser_info (
  advertiser_id, advertiser_name, qualification_type, status,
  industry_l1_id, industry_l1_name, industry_l2_id, industry_l2_name,
  created_at
)
SELECT advertiser_id, advertiser_name, 0, 2,
  1, '广告行业', 100 + advertiser_id, industry, signup_date
FROM advertiser_seed
ON DUPLICATE KEY UPDATE
  advertiser_name=VALUES(advertiser_name),
  industry_l2_name=VALUES(industry_l2_name),
  updated_at=CURRENT_TIMESTAMP;
DROP TEMPORARY TABLE advertiser_seed;

CREATE TEMPORARY TABLE campaign_seed (
  campaign_id BIGINT, advertiser_id BIGINT, campaign_name VARCHAR(128),
  budget_yuan BIGINT, status_name VARCHAR(32)
);
INSERT INTO campaign_seed VALUES
(3601,36,'ChatGPT Plus 增长计划',50000,'running'),
(3602,36,'OpenAI API 企业版',30000,'running'),
(301,3,'UNIQLO 夏季系列',42000,'running'),
(1501,15,'可口可乐夏日营销',28000,'running'),
(401,4,'蕉内舒适系列',24000,'running'),
(3301,33,'耐克跑步系列',45000,'running'),
(3201,32,'阿迪达斯经典系列',51000,'running'),
(2601,26,'腾讯游戏新品上线',17000,'running'),
(2501,25,'网易游戏赛季通行证',33000,'running'),
(1201,12,'欧莱雅美妆节',70000,'running'),
(2001,20,'小米智慧生活',21000,'running'),
(1701,17,'瑞幸夏日咖啡',19000,'running'),
(1601,16,'库迪咖啡夏日特饮',26000,'running'),
(1801,18,'Apple 开学季',68000,'running'),
(1901,19,'华为旗舰发布',72000,'running'),
(601,6,'比亚迪新能源轿车',64000,'running'),
(501,5,'特斯拉 Model Y 推广',66000,'running'),
(701,7,'蔚来 ET5 上市',68000,'running'),
(801,8,'鸿蒙智行智能出行',42000,'running'),
(502,5,'特斯拉超充网络',59000,'running'),
(602,6,'比亚迪混动升级',62000,'running'),
(702,7,'蔚来换电权益',64000,'running'),
(2201,22,'天猫 618 大促',85000,'running'),
(2101,21,'京东超级品牌日',78000,'running'),
(2701,27,'美团本地优惠',46000,'running'),
(3701,37,'携程夏日出行',52000,'running'),
(3801,38,'去哪儿夏季旅行优惠',36000,'running'),
(2401,24,'原神新版本',58000,'running'),
(201,2,'Zara 夏季上新',38000,'running'),
(101,1,'UR 新季上新',35000,'running'),
(3501,35,'NVIDIA RTX AI PC 上市',82000,'running'),
(3401,34,'Claude Code 团队版',76000,'running'),
(2301,23,'拼多多大促',88000,'running'),
(2901,29,'Gucci 新季营销',88000,'running'),
(3101,31,'Louis Vuitton 旅行皮具系列',98000,'running'),
(2801,28,'Dior 美妆经典系列',84000,'running'),
(3001,30,'Hermes 经典皮具',110000,'running'),
(1001,10,'SK-II 神仙水系列',62000,'running'),
(1301,13,'资生堂肌肤科技',58000,'running'),
(901,9,'兰蔻进阶护肤',69000,'running'),
(1401,14,'雅诗兰黛持妆粉底',73000,'running'),
(1101,11,'YSL Beauty 口红系列',54000,'running'),
(3603,36,'OpenAI 团队版扩展',42000,'running'),
(3604,36,'ChatGPT 订阅增长',36000,'running'),
(3605,36,'OpenAI 代码助手升级',48000,'running'),
(1202,12,'欧莱雅护肤节',55000,'running'),
(1203,12,'欧莱雅彩妆上新',47000,'running'),
(1204,12,'欧莱雅夏季焕新',59000,'running'),
(3702,37,'携程暑期出境游',64000,'running'),
(3703,37,'携程酒店会员日',51000,'running'),
(3704,37,'携程中秋出行',67000,'running'),
(2302,23,'拼多多百亿补贴',92000,'running'),
(2303,23,'拼多多年中大促',87000,'running'),
(2304,23,'拼多多秋季大促',93000,'running'),
(3901,39,'理想家庭出行',76000,'running'),
(802,8,'鸿蒙智行智驾焕新',53000,'running'),
(3902,39,'理想 L 系列上新',82000,'running'),
(803,8,'鸿蒙智行秋季出行',56000,'running'),
(3903,39,'理想秋季长途',84000,'running')
;
INSERT INTO campaign_info (
  campaign_id, campaign_name, advertiser_id, status, market_goal,
  trading_mode, budget, daily_budget, created_at
)
SELECT campaign_id, campaign_name, advertiser_id, 4, 4, 0,
  budget_yuan * 100000, GREATEST(100000, budget_yuan * 10000),
  TIMESTAMP('2026-05-01 00:00:00')
FROM campaign_seed
ON DUPLICATE KEY UPDATE
  campaign_name=VALUES(campaign_name), budget=VALUES(budget),
  daily_budget=VALUES(daily_budget), updated_at=CURRENT_TIMESTAMP;
DROP TEMPORARY TABLE campaign_seed;

CREATE TEMPORARY TABLE unit_seed (
  unit_id BIGINT, campaign_id BIGINT, unit_name VARCHAR(128),
  bid_type VARCHAR(32), bid_yuan DECIMAL(18,4), status_name VARCHAR(32)
);
INSERT INTO unit_seed VALUES
(360101,3601,'ChatGPT 增长单元','CPC',2.5000,'running'),
(360201,3602,'OpenAI API 单元','OCPC',3.2000,'running'),
(30101,301,'优衣库夏季单元','OCPC',2.9000,'running'),
(150101,1501,'可口可乐夏日单元','CPC',2.1000,'running'),
(40101,401,'蕉内舒适单元','CPM',16.0000,'running'),
(330101,3301,'耐克跑步单元','OCPC',3.5000,'running'),
(320101,3201,'阿迪达斯经典单元','CPC',2.7000,'running'),
(260101,2601,'腾讯游戏上线单元','CPM',15.0000,'running'),
(250101,2501,'网易游戏通行证单元','OCPC',3.1000,'running'),
(120101,1201,'欧莱雅美妆单元','CPC',3.8000,'running'),
(200101,2001,'小米生活单元','OCPC',2.6000,'running'),
(170101,1701,'瑞幸咖啡单元','CPM',17.0000,'running'),
(160101,1601,'库迪咖啡单元','OCPC',2.8000,'running'),
(180101,1801,'Apple 开学单元','OCPC',4.2000,'running'),
(190101,1901,'华为发布单元','OCPC',4.0000,'running'),
(60101,601,'比亚迪单元','CPC',5.2000,'running'),
(50101,501,'特斯拉单元','CPC',5.6000,'running'),
(70101,701,'蔚来单元','OCPC',5.9000,'running'),
(80101,801,'鸿蒙智行单元','CPC',3.9000,'running'),
(50201,502,'特斯拉超充单元','OCPC',5.8000,'running'),
(50202,502,'特斯拉长续航单元','CPC',5.4000,'running'),
(60201,602,'比亚迪混动单元','OCPC',5.1000,'running'),
(60202,602,'比亚迪升级单元','CPC',4.9000,'running'),
(70201,702,'蔚来换电单元','OCPC',6.1000,'running'),
(70202,702,'蔚来权益单元','CPC',5.7000,'running'),
(220101,2201,'天猫单元','OCPC',3.6000,'running'),
(210101,2101,'京东单元','OCPC',3.5000,'running'),
(270101,2701,'美团单元','CPC',2.4000,'running'),
(370101,3701,'携程单元','OCPC',3.8000,'running'),
(380101,3801,'去哪儿单元','OCPC',3.2000,'running'),
(240101,2401,'原神单元','CPM',22.0000,'running'),
(20101,201,'Zara 单元','OCPC',3.4000,'running'),
(10101,101,'UR 单元','CPC',3.0000,'running'),
(350101,3501,'NVIDIA 单元','OCPC',6.5000,'running'),
(340101,3401,'Claude Code 单元','OCPC',6.2000,'running'),
(230101,2301,'拼多多单元','OCPC',4.1000,'running'),
(290101,2901,'Gucci 单元','CPC',7.2000,'running'),
(310101,3101,'Louis Vuitton 单元','OCPC',8.4000,'running'),
(280101,2801,'Dior 单元','OCPC',6.6000,'running'),
(300101,3001,'Hermes 单元','OCPC',9.5000,'running'),
(100101,1001,'SK-II 单元','OCPC',5.8000,'running'),
(130101,1301,'资生堂单元','OCPC',5.6000,'running'),
(90101,901,'兰蔻单元','OCPC',6.1000,'running'),
(140101,1401,'雅诗兰黛单元','CPC',6.4000,'running'),
(110101,1101,'YSL Beauty 单元','CPM',5.2000,'running'),
(360301,3603,'OpenAI 团队版信息流','OCPC',6.8000,'running'),
(360302,3603,'OpenAI 团队版视频','CPM',7.2000,'running'),
(360401,3604,'ChatGPT 订阅转化','CPC',5.9000,'running'),
(360402,3604,'ChatGPT 会员召回','OCPC',5.4000,'running'),
(360501,3605,'OpenAI 代码助手单元','OCPC',6.9000,'running'),
(360502,3605,'OpenAI 代码助手补充单元','CPC',6.3000,'running'),
(120201,1202,'欧莱雅精华单元','OCPC',6.1000,'running'),
(120202,1202,'欧莱雅面霜单元','CPC',5.6000,'running'),
(120301,1203,'欧莱雅彩妆单元','CPM',6.3000,'running'),
(120302,1203,'欧莱雅唇釉单元','OCPC',5.8000,'running'),
(120401,1204,'欧莱雅焕新单元','OCPC',6.5000,'running'),
(120402,1204,'欧莱雅夏季单元','CPC',5.9000,'running'),
(370201,3702,'携程机票单元','OCPC',4.9000,'running'),
(370202,3702,'携程出境游单元','CPC',5.2000,'running'),
(370301,3703,'携程会员日单元','CPM',4.7000,'running'),
(370302,3703,'携程酒店单元','OCPC',5.0000,'running'),
(370401,3704,'携程中秋单元','OCPC',5.1000,'running'),
(370402,3704,'携程团圆出行单元','CPC',4.8000,'running'),
(230201,2302,'拼多多补贴单元','OCPC',4.3000,'running'),
(230202,2302,'拼多多爆款单元','CPC',4.6000,'running'),
(230301,2303,'拼多多年中大促单元','CPM',4.9000,'running'),
(230302,2303,'拼多多拉新单元','OCPC',4.2000,'running'),
(230401,2304,'拼多多秋季单元','OCPC',4.7000,'running'),
(230402,2304,'拼多多用户召回单元','CPC',4.4000,'running'),
(390101,3901,'理想家庭出行单元','OCPC',5.7000,'running'),
(80201,802,'鸿蒙智行智驾单元','OCPC',4.1000,'running'),
(390201,3902,'理想上新单元','CPC',5.3000,'running'),
(80301,803,'鸿蒙智行秋季单元','OCPC',4.3000,'running'),
(80302,803,'鸿蒙智行出行单元','CPC',4.0000,'running'),
(390301,3903,'理想秋季单元','OCPC',5.6000,'running'),
(390302,3903,'理想长途单元','CPC',5.2000,'running')
;
INSERT INTO unit_info (
  unit_id, unit_name, campaign_id, status, is_closed, placement_type, ad_type,
  search_keyword, product_id, landing_page_url, audience, start_date,
  end_date, daily_budget, bid_type, bid, created_at
)
SELECT unit_id, unit_name, campaign_id, 0,
  CASE WHEN MOD(unit_id, 4)=0 THEN 1 ELSE 0 END,
  MOD(unit_id, 6) + 1,
  MOD(unit_id, 4) + 1,
  JSON_ARRAY(unit_name), NULL,
  CONCAT('https://landing.ustc.example/unit/', unit_id),
  JSON_OBJECT('region', 'all', 'age', JSON_ARRAY('18-24','25-34')),
  TIMESTAMP('2026-06-01 00:00:00'), NULL,
  500000000, bid_type, CAST(ROUND(bid_yuan * 100000) AS SIGNED),
  TIMESTAMP('2026-05-15 00:00:00')
FROM unit_seed
ON DUPLICATE KEY UPDATE
  unit_name=VALUES(unit_name), bid_type=VALUES(bid_type), bid=VALUES(bid),
  placement_type=VALUES(placement_type), ad_type=VALUES(ad_type),
  updated_at=CURRENT_TIMESTAMP;
DROP TEMPORARY TABLE unit_seed;

INSERT INTO creative_info(creative_id, unit_id, creative_name, creative_tags) VALUES
(36010101,360101,'ChatGPT Plus 增长创意',JSON_ARRAY('feed_card')),
(36020101,360201,'OpenAI API 企业方案',JSON_ARRAY('feed_card')),
(3010101,30101,'优衣库夏季穿搭',JSON_ARRAY('feed_card')),
(15010101,150101,'可口可乐夏日清爽',JSON_ARRAY('feed_card')),
(4010101,40101,'蕉内舒适系列',JSON_ARRAY('feed_card')),
(33010101,330101,'耐克跑步系列',JSON_ARRAY('feed_card')),
(32010101,320101,'阿迪达斯经典款',JSON_ARRAY('feed_card')),
(26010101,260101,'腾讯游戏新作',JSON_ARRAY('feed_card')),
(25010101,250101,'网易游戏赛季通行证',JSON_ARRAY('feed_card')),
(12010101,120101,'欧莱雅美妆节',JSON_ARRAY('feed_card')),
(20010101,200101,'小米智慧生活',JSON_ARRAY('feed_card')),
(17010101,170101,'瑞幸夏日咖啡',JSON_ARRAY('feed_card')),
(16010101,160101,'库迪咖啡冰咖故事',JSON_ARRAY('feed_card')),
(18010101,180101,'Apple 开学季',JSON_ARRAY('feed_card')),
(19010101,190101,'华为旗舰发布',JSON_ARRAY('feed_card')),
(6010101,60101,'比亚迪试驾',JSON_ARRAY('feed_card')),
(5010101,50101,'特斯拉 Model Y 视频',JSON_ARRAY('feed_card')),
(7010101,70101,'蔚来 ET5 上市视频',JSON_ARRAY('feed_card')),
(8010101,80101,'鸿蒙智行智能出行故事',JSON_ARRAY('feed_card')),
(5020101,50201,'特斯拉超充网络介绍',JSON_ARRAY('feed_card')),
(5020201,50202,'特斯拉长续航体验',JSON_ARRAY('feed_card')),
(6020101,60201,'比亚迪混动升级介绍',JSON_ARRAY('feed_card')),
(6020201,60202,'比亚迪升级权益',JSON_ARRAY('feed_card')),
(7020101,70201,'蔚来换电服务介绍',JSON_ARRAY('feed_card')),
(7020201,70202,'蔚来权益视频',JSON_ARRAY('feed_card')),
(22010101,220101,'天猫 618 直播间',JSON_ARRAY('feed_card')),
(21010101,210101,'京东品牌日信息流',JSON_ARRAY('feed_card')),
(27010101,270101,'美团周边优惠',JSON_ARRAY('feed_card')),
(37010101,370101,'携程夏日出行视频',JSON_ARRAY('feed_card')),
(38010101,380101,'去哪儿旅行优惠',JSON_ARRAY('feed_card')),
(24010101,240101,'原神版本预告',JSON_ARRAY('feed_card')),
(2010101,20101,'Zara 夏季新品图册',JSON_ARRAY('feed_card')),
(1010101,10101,'UR 街头风穿搭',JSON_ARRAY('feed_card')),
(35010101,350101,'NVIDIA AI PC 预告',JSON_ARRAY('feed_card')),
(34010101,340101,'Claude Code 团队演示',JSON_ARRAY('feed_card')),
(23010101,230101,'拼多多大促故事',JSON_ARRAY('feed_card')),
(29010101,290101,'Gucci 秀场精选',JSON_ARRAY('feed_card')),
(31010101,310101,'Louis Vuitton 旅行故事',JSON_ARRAY('feed_card')),
(28010101,280101,'Dior 美妆经典展示',JSON_ARRAY('feed_card')),
(30010101,300101,'Hermes 工艺纪录片',JSON_ARRAY('feed_card')),
(10010101,100101,'SK-II 神仙水揭秘',JSON_ARRAY('feed_card')),
(13010101,130101,'资生堂焕亮护理',JSON_ARRAY('feed_card')),
(9010101,90101,'兰蔻精华故事',JSON_ARRAY('feed_card')),
(14010101,140101,'雅诗兰黛持妆演示',JSON_ARRAY('feed_card')),
(11010101,110101,'YSL Beauty 口红发布',JSON_ARRAY('feed_card')),
(36030101,360301,'OpenAI 团队版介绍',JSON_ARRAY('feed_card')),
(36030201,360302,'OpenAI 团队版演示',JSON_ARRAY('feed_card')),
(36030102,360301,'OpenAI 团队版补充图文',JSON_ARRAY('feed_card')),
(36040101,360401,'ChatGPT 订阅转化页',JSON_ARRAY('feed_card')),
(36040201,360402,'ChatGPT 会员召回视频',JSON_ARRAY('feed_card')),
(36040102,360401,'ChatGPT 订阅补充页',JSON_ARRAY('feed_card')),
(36050101,360501,'OpenAI 代码助手介绍',JSON_ARRAY('feed_card')),
(36050201,360502,'OpenAI 代码助手补充',JSON_ARRAY('feed_card')),
(12020101,120201,'欧莱雅精华讲解',JSON_ARRAY('feed_card')),
(12020201,120202,'欧莱雅面霜短视频',JSON_ARRAY('feed_card')),
(12020102,120201,'欧莱雅精华补充素材',JSON_ARRAY('feed_card')),
(12030101,120301,'欧莱雅彩妆上新图文',JSON_ARRAY('feed_card')),
(12030201,120302,'欧莱雅唇釉视频',JSON_ARRAY('feed_card')),
(12040101,120401,'欧莱雅焕新讲解',JSON_ARRAY('feed_card')),
(12040201,120402,'欧莱雅夏季短视频',JSON_ARRAY('feed_card')),
(37020101,370201,'携程机票优惠',JSON_ARRAY('feed_card')),
(37020201,370202,'携程出境游预热视频',JSON_ARRAY('feed_card')),
(37020102,370201,'携程机票补充卡片',JSON_ARRAY('feed_card')),
(37030101,370301,'携程会员日权益',JSON_ARRAY('feed_card')),
(37030201,370302,'携程酒店推广视频',JSON_ARRAY('feed_card')),
(37040101,370401,'携程中秋出行页',JSON_ARRAY('feed_card')),
(37040201,370402,'携程团圆出行视频',JSON_ARRAY('feed_card')),
(23020101,230201,'拼多多补贴页',JSON_ARRAY('feed_card')),
(23020201,230202,'拼多多爆款短视频',JSON_ARRAY('feed_card')),
(23020102,230201,'拼多多补贴补充页',JSON_ARRAY('feed_card')),
(23030101,230301,'拼多多年中大促页',JSON_ARRAY('feed_card')),
(23030201,230302,'拼多多拉新视频',JSON_ARRAY('feed_card')),
(23040101,230401,'拼多多秋季大促页',JSON_ARRAY('feed_card')),
(23040201,230402,'拼多多召回视频',JSON_ARRAY('feed_card')),
(39010101,390101,'理想家庭出行故事',JSON_ARRAY('feed_card')),
(8020101,80201,'鸿蒙智行智驾宣传',JSON_ARRAY('feed_card')),
(8020102,80201,'鸿蒙智行智驾补充',JSON_ARRAY('feed_card')),
(39020101,390201,'理想 L 系列讲解',JSON_ARRAY('feed_card')),
(39020102,390201,'理想 L 系列补充',JSON_ARRAY('feed_card')),
(8030101,80301,'鸿蒙智行秋季出行',JSON_ARRAY('feed_card')),
(8030201,80302,'鸿蒙智行出行补充',JSON_ARRAY('feed_card')),
(39030101,390301,'理想秋季单元讲解',JSON_ARRAY('feed_card')),
(39030201,390302,'理想长途出行补充',JSON_ARRAY('feed_card'))
ON DUPLICATE KEY UPDATE creative_name=VALUES(creative_name), creative_tags=VALUES(creative_tags);

UPDATE creative_info
SET creative_tags = JSON_ARRAY(CASE
  WHEN creative_name LIKE '%直播%' THEN 'live_room'
  WHEN creative_name REGEXP '视频|预告|纪录片|故事|演示' THEN 'short_video'
  ELSE 'feed_card'
END);

UPDATE creative_info
SET
  status = 0,
  creative_mode = CASE WHEN MOD(creative_id, 3) = 0 THEN 2 ELSE 1 END,
  material_mode = CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(creative_tags, '$[0]')) = 'feed_card'
                       AND MOD(creative_id, 5) = 1 THEN 2 ELSE 1 END,
  creative_title = creative_name,
  creative_category = CASE
    WHEN creative_name REGEXP 'API|ChatGPT|OpenAI|Claude' THEN 'lead_generation'
    WHEN creative_name REGEXP '直播|促销|优惠|大促' THEN 'sales_promotion'
    ELSE 'brand_awareness'
  END,
  creative_text = CONCAT(creative_name, '，了解更多产品信息。'),
  creative_tags = JSON_ARRAY(
    JSON_UNQUOTE(JSON_EXTRACT(creative_tags, '$[0]')),
    CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(creative_tags, '$[0]')) = 'feed_card'
              AND MOD(creative_id, 5) = 1 THEN 'image' ELSE 'video' END
  ),
  creative_image_urls = CASE
    WHEN JSON_UNQUOTE(JSON_EXTRACT(creative_tags, '$[0]')) = 'feed_card'
         AND MOD(creative_id, 5) = 1
    THEN CONCAT('https://cdn.ustc.example/creative/', creative_id, '.jpg')
    ELSE NULL
  END,
  creative_video_id = CASE
    WHEN JSON_UNQUOTE(JSON_EXTRACT(creative_tags, '$[0]')) = 'feed_card'
         AND MOD(creative_id, 5) = 1 THEN NULL
    ELSE creative_id * 10 + 1
  END,
  monitoring_url = CONCAT('https://monitor.ustc.example/creative/', creative_id);


