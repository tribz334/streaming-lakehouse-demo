# 广告湖仓表、产出逻辑与上下游说明（入门版）

## 1. 先理解：一条数据如何走完整条链路

可以把数仓想象成一家餐厅：

- Generator 是顾客提交的原始订单。
- Kafka 是传送带，负责缓存和传递消息。
- Flink CDC 将 MySQL 主数据持续同步为 Paimon 当前态 DIM；Paimon daily tag 保留最近 30 天的可查询快照。
- ODS 是原料仓，尽量保留原始数据。
- DIM 是字典，例如 `adv_001` 到底是哪家广告主。
- DWD 是清洗、补全后的明细菜品，一行仍代表一个业务事件。
- DWM 是把常用字段拼成宽表，减少下游反复关联。
- DWS 是按广告主、计划、创意等主题汇总后的指标表。
- DM 是面向归因、反作弊等特定算法场景的中间模型。
- ADS 是直接给报表和业务使用的结果表。
- StarRocks 是查询加速层，Superset 从 StarRocks 读取并画图。

```text
实时热路径：Kafka ods_log（创意/商品/广告位与事件上下文）-> 广播关联 Creative/Campaign DIM 补齐层级 ID -> dwd_ad_action_log；广告点击与 MySQL `order_detail` 商品订单进入 DwdOrderDetail -> dwd_order_detail；MySQL `bill_detail` 进入 DwdAdBill；三类事实随后汇合 -> DWS 10 秒聚合 -> StarRocks -> Superset
离线湖仓：MySQL -> Flink CDC -> Paimon DIM/DWD；Paimon 快照 -> DWS/DM/ADS -> StarRocks
```

## 2. 表名后缀是什么意思

| 后缀 | 含义 | 本项目中的理解 |
|---|---|---|
| `_di` | Daily Increment，按天增量 | 通常有日期字段或日期分区，保存事件/结果明细 |
| `_df` | Daily Full，按天全量/快照 | 维表快照或按天完整主题结果 |
| `_10s` | 10 秒窗口 | 每 10 秒聚合一次，用于实时指标 |

注意：后缀表达的是建模意图，不会自动产生数据；是否真的产出，要看是否有 `INSERT INTO` 作业。

## 3. 两条计算链路

### 3.1 实时链路

实时任务长期运行，新消息到达后由一个 Java Flink 作业直接处理：

```text
produce_events.py
  -> 广告行为 -> Kafka: ods_log ------------------┐
  -> 商品订单 -> MySQL: order_detail -> CDC --------├-> DwsAdMetric
  -> 广告计费 -> MySQL: bill_detail -> CDC ---------┘  DwdAdBill、10 秒聚合
  -> StarRocks realtime_ad_attribution_metrics_10s
```

入口脚本是 `scripts/windows/submit-streaming-jobs.ps1`。Kafka 不承载订单或计费事件；商品订单和广告账单由 Flink MySQL CDC 直接读取。SDK 行为只上报 `creative_id` 等事件字段，DWD 通过 DIM CDC 广播状态补齐广告主、计划和单元 ID。点击与订单按 `user_id + product_id` 分组进入 `AttributionProcessFunction`；没有命中点击的订单成为自然订单。名称、行业等展示属性不复制进每条 DWD，而在 ADS/查询层关联。

每天 00:05，DolphinScheduler 的 `ad_realtime_daily_rollover` 工作流先把所有已结束日期的最终累计结果封存到 StarRocks `realtime_ad_metrics_daily`，封存成功后删除实时主表中今天以前的 10 秒窗口，再校验 `v_realtime_ad_metrics_today` 已切换到新业务日。它负责日界线、历史封存和实时明细清理；当天零点到当前时刻的连续累加仍由常驻 Flink 作业完成。

### 3.2 离线链路

离线任务读取某一时刻的 Paimon 快照，批量清空并重算结果。DolphinScheduler 的设计调度时间是每天 `02:00`：

```text
ODS 快照检查 + DIM 刷新
  -> DWS 主题汇总（直接读取统一 DWD）
  -> DM 归因/反作弊特征
  -> ADS 留存/归因/反作弊/创意报表
```

调度定义在 `dolphinscheduler/workflows/ad-lakehouse-demo.yaml`。本地批处理入口是 `scripts/windows/run-ads-batches.ps1`。

## 4. 数据源与 ODS

### `ods_log_inc`：埋点日志原始层

| 项目 | 说明 |
|---|---|
| 一行代表 | 一次曝光、点击或转化广告行为；订单不在此表 |
| 上游 | 历史湖仓装载结果 |
| 当前用途 | 离线快照、历史重算与兼容性验证 |
| 更新方式 | 不属于当前实时热路径 |

Generator 先从 MySQL 读取广告主、计划、单元、创意组合，然后按媒体、行业、时段等权重生成事件。事件类型概率在 `generator/produce_events.py:157`，历史日期在 `:297`，历史回灌在 `:324`，持续实时生成在 `:415`。当前 Compose 配置的业务日期为 2026-06-01 至 2026-07-17。

### MySQL 业务源表

| MySQL 表 | 含义 | 湖仓下游 |
|---|---|---|
| `advertiser_info` | 广告主档案 | Flink CDC -> `dim_advertiser_zip` |
| `campaign_info` | 广告计划 | Flink CDC -> `dim_campaign` |
| `unit_info` | 广告单元 | Flink CDC -> `dim_unit` |
| `creative_info` | 广告创意 | Flink CDC -> `dim_creative` |
| `user_info` | C 端用户 | Flink CDC -> `dim_user_zip` |
| `shop_info` | 店铺档案 | Flink CDC -> `dim_shop_zip` |
| `product_info` | 商品档案 | Flink CDC -> `dim_product_zip` |
| `order_detail` | 商品订单生命周期 | Flink CDC -> `dwd_order_detail_acc` |
| `bill_detail` | 广告计费记录 | Flink CDC -> `dwd_ad_bill_detail_inc` |

表结构在 `mysql/init/01_schema.sql`，演示主数据在 `mysql/init/02_seed.sql`。

## 5. DIM：维度表

维表回答的是“这个 ID 是谁、叫什么、属于什么分类”，通常不直接计算曝光、点击、GMV。

| 表 | 一行粒度 | 上游 | 下游 | 当前状态 |
|---|---|---|---|---|
| `dim_advertiser_zip` | 一个广告主当前版本 | MySQL `advertiser_info` CDC | ADS 创意宽表 | 主键 `advertiser_id`，daily tag 留历史 |
| `dim_campaign` | 一个广告计划当前版本 | MySQL `campaign_info` CDC | DWS/ADS | 主键 `campaign_id`，daily tag 留历史 |
| `dim_unit` | 一个广告单元当前版本 | MySQL `unit_info` CDC | DWS/ADS、商品匹配 | 主键 `unit_id`，daily tag 留历史 |
| `dim_creative` | 一个广告创意当前版本 | MySQL `creative_info` CDC | DWS/ADS | 主键 `creative_id`，daily tag 留历史 |
| `dim_user_zip` | 一个用户当前版本 | MySQL `user_info` CDC | 用户画像 | 主键 `uid`，daily tag 留历史 |
| `dim_customer` | 一个客户 | 尚无源表 | 论文模型预留 | 空表 |
| `dim_shop_zip` | 一个店铺当前版本 | MySQL `shop_info` CDC | 商品、订单分析 | 主键 `shop_id`，daily tag 留历史 |
| `dim_product_zip` | 一个商品当前版本 | MySQL `product_info` CDC | 商品、订单分析 | 主键 `product_id`，daily tag 留历史 |
| `dim_slot` | 一个广告位 | 尚无独立源表 | 论文媒体广告位扩展预留 | 空表 |

七张业务维表均由 `05_mysql_cdc_direct_to_dim.sql` 持续维护当前值；daily tag 用于按日回看，不再运行独立 ODS/DIM 日批刷新。

## 6. DWD：明细事实层

### `dwd_ad_events_di`：统一事件主干表

| 项目 | 说明 |
|---|---|
| 一行代表 | 一个清洗并补充维度后的广告事件 |
| 上游 | `ods_log_inc` 拆分后的 DWD 动作 + 广告层级 DIM |
| 下游 | 离线 DWS、DM 和 ADS |
| 更新方式 | 不属于当前实时热路径 |

实时 ODS 保留 SDK 的 `common/actions` 原始嵌套结构；每个 action 只携带 `creative_id`、`product_id`、广告位 `slot_id` 和事件上下文。Flink 将 actions 拆成逐条 DWD 事实，再通过广播 DIM 补齐 `unit_id / campaign_id / advertiser_id`。名称、行业和等级等展示属性不写入实时 DWD。

### `dwd_order_detail_acc`：订单累积快照事实表

一行代表一个商品订单的完整当前生命周期。`shop_id` 由商品所属店铺得到；`creative_id / unit_id / slot_id` 来自实时 LastClick 结果，自然订单均为 NULL。`product_price`、`product_num`、`total_amount` 分别表示下单时商品单价、数量和总价（元），并满足 `total_amount = product_price * product_num`。表中还包含创建、取消、支付、确认收货、退款发起、退款完成和交易完成时间。终止时间按取消、退款完成、交易完成的顺序取第一个非空值；三者都为空时 `dt=9999-12-31`。Paimon `bucket=-1` 使 `order_id` 在所有分区全局唯一，订单闭环后从哨兵分区迁移到真实终止日分区。`update_at` 记录该 DWD 行最后一次生命周期或归因更新时间。

## 7. DWS：主题汇总层

实时 10 秒指标不再物化为 Paimon DWS 表，而是在 `DwsAdMetric` 中完成窗口聚合后直接写入 StarRocks Primary Key 表。

### 四张离线主题表

| 表 | 一行粒度 | 主要指标 | 典型下游 |
|---|---|---|---|
| `dws_creative` | 日期 + 创意 | 当日下发、曝光、点击、转化、消耗和订单指标 | `dm_creative` |
| `dws_unit` | 日期 + 单元 | 按单元汇总的当日指标 | `dm_unit` |
| `dws_campaign` | 日期 + 计划 | 按计划汇总的当日指标 | `dm_campaign` |
| `dws_advertiser` | 日期 + 广告主 | 按广告主汇总的当日指标 | `dm_advertiser` |

四张离线 DWS 统一由 `08_offline_dws.sql` 产出。`dws_creative` 对广告动作、广告账单和订单生命周期分别按日聚合，再通过广告层级维度向上汇总到单元、计划和广告主。DWS 只保存可累加的每日事实，滚动窗口在 DM 计算。

## 8. DM：专题模型层

### `dm_creative`

它从 `dws_creative` 生成创意主题宽表，一行代表“统计日期 + 创意”。计算以当天 `dim_creative` 全量分区为主表左连接，因此没有行为的创意也会得到零指标。表中维护发送、曝光、点击、转化、互动、播放、消耗和订单指标的最近 1 天、7 天、30 天及累计值，并保存消耗、创建订单、支付订单和退款订单的首次/末次发生日期；尚未采集的互动与播放窗口字段保留为 NULL。实现采用可任意重跑的历史重算，结果与“昨日 DM + 今日 DWS - 7/30 天前 DWS”的论文递推算法一致，但首次初始化和历史补数不依赖前一日 DM 分区。`ads_creative_offline_di` 从该表读取 1 日指标并在关联维度后重新计算 CTR、CVR、CPC、CPA 和 ROI。

### `dm_antifraud_feature`

该表仅保留表结构，当前离线 DWS/DM 主链路不再产出用户点击窗口中间表。

## 9. ADS：报表应用层

| 表 | 上游 | 产出逻辑 | 对应业务/指标 | 类型 |
|---|---|---|---|---|
| `ads_advertiser_retention_di` | `dws_advertiser` 日汇总 | 比较同一广告主在 cohort 日及 +1/+7/+15/+30 日是否仍有消费 | 广告主留存人数、留存率 | 离线 |
| `ads_order_attribution_detail_di` | 订单累积事实 + 点击事实 + 广告层级 DIM | 同用户同商品、下单前 30 天内最后一次点击；按 30 分钟、1/3/7/30 日或自然订单互斥分类 | 订单级间接归因下钻 | 离线 |
| `ads_attribution_summary_di` | `ads_order_attribution_detail_di` | 按日期、广告主、计划、归因周期聚合订单数与 GMV | 归因分析看板 | 离线 |
| `ads_creative_offline_di` | `dm_creative` + 四张广告层级当前态 DIM | 读取创意 DM 的 1 日可加指标，补齐名称/行业/出价，并重算 CTR、CVR、CPC、CPA、ROI | Superset 创意多维离线看板 | 离线 |

关键代码：

| 结果 | SQL 文件 |
|---|---|
| 留存 | `flink/sql/10_ads_retention.sql` |
| 订单间接归因 | `flink/sql/11_ads_order_attribution.sql` |
| 创意离线指标 | `flink/sql/13_ads_creative_offline.sql` |

## 10. 指标公式（最常用）

| 指标 | 公式 | 小白解释 |
|---|---|---|
| CTR | 点击数 / 曝光数 | 看过广告的人中有多少点击 |
| CVR | 转化数 / 点击数 | 点击的人中有多少完成转化 |
| CPC | 消耗 / 点击数 | 平均买到一次点击花多少钱 |
| CPA | 消耗 / 转化数 | 平均获得一次转化花多少钱 |
| eCPM | 消耗 × 1000 / 曝光数 | 每千次曝光的成本 |
| ROI/ROAS | GMV / 消耗 | 每花 1 元广告费带来多少 GMV |
| 留存率 | 未来仍活跃的广告主数 / cohort 广告主数 | 某天活跃的广告主后来还有多少回来 |
| 归因订单率 | 有广告点击归因的订单 / 全部订单 | 多少订单可归功于广告触点 |

计算比例时不能直接 `AVG(ctr)`，应使用 `SUM(clicks)/SUM(impressions)` 重算，否则不同流量规模的行会被错误地等权平均。Superset 数据集也按这个方式配置。

## 11. 查询服务层：Paimon、StarRocks、Superset 的关系

Paimon 是离线湖仓存储，StarRocks 是实时与离线结果的统一查询服务。实时核心指标由 Java Flink 作业合并 Kafka 广告流和 MySQL CDC 订单流后计算，并通过 JDBC 写入 StarRocks；离线 ADS 由同步脚本生成快照。Superset 查询 StarRocks 视图。

```text
Kafka ods_log + MySQL order_detail/bill_detail CDC -> DwsAdMetric -> StarRocks 实时 Primary Key 表
Paimon ADS -> scripts/windows/sync-starrocks-olap.ps1 -> StarRocks 快照
  -> StarRocks ad_ads 视图
  -> superset/bootstrap_datasets.py 中定义指标口径
  -> Superset 图表与仪表盘
```

所以修改某个图表时要先判断：是 Generator 原始分布问题、Flink SQL 口径问题、StarRocks 同步问题，还是 Superset 展示配置问题。

## 12. 当前表状态总结

| 状态 | 表 |
|---|---|
| 实时持续更新 | StarRocks `realtime_ad_attribution_metrics_10s` |
| ODS 埋点原文 | `ods_log_inc` |
| 离线批量重算 | DWD 五张专表、DWM、六张离线 DWS、DM、各 ADS 表 |
| CDC 当前态维表（daily tag 留历史） | `dim_advertiser_zip`、`dim_campaign`、`dim_unit`、`dim_creative`、`dim_user_zip`、`dim_shop_zip`、`dim_product_zip` |
| 只有表结构、尚无数据源 | `dim_customer`、`dim_slot` |

最后记住一个判断方法：先找表的 `CREATE TABLE` 理解字段，再全局搜索 `INSERT INTO 表名` 找产出逻辑，再看 `FROM/JOIN` 确认上游，最后搜索谁在 `FROM 表名` 确认下游。只有建表、没有 INSERT 的表，就是模型预留而不是正在生产的数据表。
