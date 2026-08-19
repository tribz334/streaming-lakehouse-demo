# 广告湖仓表、产出逻辑与上下游说明（入门版）

## 1. 先理解：一条数据如何走完整条链路

可以把数仓想象成一家餐厅：

- Generator 是顾客提交的原始订单。
- Kafka 是传送带，负责缓存和传递消息。
- Flink CDC 把 MySQL 主数据持续同步成 Paimon DIM 表，事实链路只保存维度外键。
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
离线湖仓：MySQL -> Flink CDC -> Paimon DIM/ODS；Paimon 快照 -> DWS/DM/ADS -> StarRocks
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

### `ods_ad_events_di`：广告事件原始层

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
| `advertiser` | 广告主档案 | `dim_advertiser_df` |
| `campaign` | 广告计划 | `dim_campaign_df` |
| `unit` | 广告单元 | `dim_unit_df` |
| `creative` | 广告创意 | `dim_creative_df` |
| `order` | 跨渠道商品订单生命周期 | `ods_order`、实时订单归因 |
| `ad_bill` | 广告计费记录 | `ods_ad_bill`、实时消耗汇总 |

表结构在 `mysql/init/01_schema.sql`，演示主数据在 `mysql/init/02_seed.sql`。

## 5. DIM：维度表

维表回答的是“这个 ID 是谁、叫什么、属于什么分类”，通常不直接计算曝光、点击、GMV。

| 表 | 一行粒度 | 上游 | 下游 | 当前状态 |
|---|---|---|---|---|
| `dim_advertiser_df` | 一个广告主 | MySQL `advertiser_info` | DWD 补广告主名称、行业、等级；ADS 创意宽表 | 正常产出，约 39 行 |
| `dim_campaign_df` | 一个广告计划 | MySQL `campaign_info` | DWD 补计划名称；ADS 创意宽表 | 正常产出，约 59 行 |
| `dim_unit_df` | 一个广告单元 | MySQL `unit_info` | ADS 创意宽表补出价方式和金额 | 正常产出，约 76 行 |
| `dim_creative_df` | 一个创意 | MySQL `creative_info` | DWD 补创意名称；ADS 创意宽表 | 正常产出，约 83 行 |
| `dim_customer_df` | 一个客户 | 尚无源表 | 论文模型预留 | 空表 |
| `dim_shop_df` | 一个店铺 | 尚无源表 | 论文电商扩展预留 | 空表 |
| `dim_product_df` | 一个商品 | 尚无源表 | 论文电商扩展预留 | 空表 |
| `dim_slot_df` | 一个广告位 | 尚无独立源表 | 论文媒体广告位扩展预留 | 空表 |
| `dim_user_df` | 一个用户 | 尚无独立源表 | 论文用户画像扩展预留 | 空表 |

前四张表由 `flink-cdc/mysql-to-paimon.yaml` 在完成 MySQL 全量快照后持续消费 binlog 更新；`flink/sql/07_offline_dim_snapshot.sql` 仅保留为迁移说明，不再执行 JDBC 刷新。后五张只在 `flink/sql/01_model_tables.sql` 建了表，没有任何 `INSERT INTO`，所以“看得见表”但“没有产出逻辑”。

## 6. DWD：明细事实层

### `dwd_ad_events_di`：统一事件主干表

| 项目 | 说明 |
|---|---|
| 一行代表 | 一个清洗并补充维度后的广告事件 |
| 上游 | `ods_ad_events_di` + `dim_advertiser_df` + `dim_campaign_df` + `dim_creative_df` |
| 下游 | 离线 DWS、DM 和 ADS |
| 更新方式 | 不属于当前实时热路径 |

实时 ODS 保留 SDK 的 `common/page/actions` 原始嵌套结构；每个 action 只携带 `creative_id`、`product_id`、广告位 `slot_id` 和事件上下文。Flink 将 actions 拆成逐条 DWD 事实，再通过广播 DIM 补齐 `unit_id / campaign_id / advertiser_id`。名称、行业和等级等展示属性不写入实时 DWD。

### `dwd_order_lifecycle_df`：订单状态表

一行代表一个商品订单当前生命周期，来自 MySQL `order_detail`，只保存用户、商品、金额和创建/支付/退款/完成时间，不保存广告层级。CDC Pipeline 将其同步到 Paimon `ods_order`；实时作业消费同一张表的 binlog，并通过 `user_id + product_id` 与 Kafka 点击完成广告归因。

## 7. DWS：主题汇总层

实时 10 秒指标不再物化为 Paimon DWS 表，而是在 `DwsAdMetric` 中完成窗口聚合后直接写入 StarRocks Primary Key 表。

### 三张离线主题表

| 表 | 一行粒度 | 主要指标 | 典型下游 |
|---|---|---|---|
| `dws_creative_df` | 日期 + 创意 | 曝光、点击、转化、订单、成本、GMV、CTR、CVR、ROI | `ads_creative_offline_di` |
| `dws_attribution_candidate_df` | 订单 + 候选点击 | 订单前 30 天候选点击、触点顺序、间隔分钟 | `dm_attribution_touchpoint_df` |
| `dws_user_click_window_df` | 单次点击 + 用户窗口 | 1 小时/1 天点击数、曝光数、点击间隔、CTR 偏离 | `dm_antifraud_feature_df` |

三张离线 DWS 统一由 `08_offline_dws.sql` 产出。归因和反作弊分别使用独立主题表，避免将不同粒度的数据塞入一张通用 DWS。

## 8. DM：专题模型层

### `dm_attribution_touchpoint_df`

它从 DWS 候选触点中选出离订单最近的一次点击。没有候选点击的订单会保留为 `organic`，因此自然订单不会丢失。

- 上游：`dws_attribution_candidate_df`。
- 下游：`ads_order_attribution_detail_di` 和归因分析。
- 代码：`09_offline_dm.sql`。
- 指标：归因权重、归因 GMV、归因转化数、触点序号。

关联窗口和结果字段 `lookback_days` 现在统一为 30 天。

### `dm_antifraud_feature_df`

它从 `dws_user_click_window_df` 读取 1 小时/1 天点击数、CTR 偏离和点击间隔，输出 `HIGH_CLICK_BURST`、`ABNORMAL_CTR`、`HIGH_DAILY_CLICKS` 或 `NORMAL`，并生成 0.1-0.95 的风险分。

## 9. ADS：报表应用层

| 表 | 上游 | 产出逻辑 | 对应业务/指标 | 类型 |
|---|---|---|---|---|
| `ads_advertiser_retention_di` | DWD 付费活跃事件 | 比较同一广告主在 cohort 日及 +1/+7/+15/+30 日是否仍有消费 | 广告主留存人数、留存率 | 离线 |
| `ads_order_attribution_detail_di` | 归因 DM + 候选 DWS | 只选末次点击或自然订单，划分六个归因窗口 | 每单归因明细、归因时延、自然/直接/间接 | 离线 |
| `ads_attribution_summary_di` | 订单归因明细 | 按日期、广告主、计划、归因时间段汇总 | 各归因窗口订单占比和 GMV 占比 | 离线 |
| `ads_fraud_signal_di` | 反作弊 DM + 用户窗口 DWS | 按 1 分钟汇总 DM 命中的风险点击 | 可疑点击、可疑消耗、风险分 | 离线规则计算 |
| `ads_creative_offline_di` | DWS 创意 + 四张 DIM | 拼接名称/行业/出价，并重算 CTR、CVR、CPC、CPA、ROI | Superset 创意多维离线看板 | 离线 |

关键代码：

| 结果 | SQL 文件 |
|---|---|
| 留存 | `flink/sql/10_ads_retention.sql` |
| 归因明细和汇总 | `flink/sql/11_ads_attribution.sql` |
| 反作弊信号 | `flink/sql/12_ads_fraud.sql` |
| 创意离线指标 | `flink/sql/13_ads_creative_offline.sql` |

归因六类由订单与最后一次点击的分钟差决定：无点击是自然订单；不超过 30 分钟是直接归因；随后依次进入 1 日、3 日、7 日、30 日间接归因。Generator 在 `produce_events.py:65-69` 按目标比例生成这些订单旅程，在 `:226-256` 人为构造不同时间差，所以数据库业务时间可以覆盖完整 30 天窗口。

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
| 离线兼容快照 | `ods_ad_events_di`、`dwd_ad_events_di` |
| 离线批量重算 | DWD 五张专表、DWM、六张离线 DWS、DM、各 ADS 表 |
| 正常维表快照 | `dim_advertiser_df`、`dim_campaign_df`、`dim_unit_df`、`dim_creative_df` |
| 只有表结构、尚无数据源 | `dim_customer_df`、`dim_shop_df`、`dim_product_df`、`dim_slot_df`、`dim_user_df` |

最后记住一个判断方法：先找表的 `CREATE TABLE` 理解字段，再全局搜索 `INSERT INTO 表名` 找产出逻辑，再看 `FROM/JOIN` 确认上游，最后搜索谁在 `FROM 表名` 确认下游。只有建表、没有 INSERT 的表，就是模型预留而不是正在生产的数据表。
