# 广告双分类数据字典

`placement_type` 与 `ad_type` 均归属于 Unit，权威来源是 `dim_unit_df`；`slot_id` 仅表示 SDK 上报的具体广告槽位。

| 字段 | 枚举 | 含义 |
| --- | --- | --- |
| `placement_type` | 1 / 2 / 3 / 4 / 5 / 6 | feed / search / splash / rewarded / banner / other |
| `ad_type` | 1 / 2 / 3 / 4 | short_video / live / image_text / other |

传递链路：SDK 只上报 `creative_id` / `product_id` / `slot_id` / `event` / `ts`，ODS 忠实保留原始 JSON。`dwd_ad_event_di` 只固化标准事实字段；实时和离线汇总通过 `creative_id → dim_creative_df → unit_id → dim_unit_df` 获取分类。无法补维或枚举越界的事件仅写入作业日志，不再落成 DWD 业务表。

`placement_type=1/2/3` 分别为 feed/search/splash，这一顺序同时用于 DWS 宽表、ADS 列和 Dashboard 标题，不允许在可视化层重新解释。

## Cost 与金额口径

- Cost 来自 MySQL CDC 直接写入的账单事实。闭环 Cost 在聚合前通过 `dwd_ad_bill_di.unit_id → dim_unit.is_closed` 判断。
- Cost、GMV 及各分类 GMV 在 Fluss/Paimon 中用 `BIGINT` 保存“千分之一分”，即 1 元 = 100000 存储单位。StarRocks 的实时、离线和订单归因经营服务视图以元输出。

DWS 粒度：

- `dws_unit_di`：`dt + unit_id`，携带已在 DWD 固化的两个分类属性，不展开宽指标。
- `dws_creative_di`：`dt + creative_id`，只保留总体基础指标。
- `dws_campaign_di` 与 `dws_advertiser_di`：保留总体基础指标，并按固定枚举展开订单数、退款数、GMV 和退款 GMV。

最终 DWS 只有 advertiser / campaign / unit / creative 四张主题表，DM 也只有同四个粒度，不存在独立的 placement/ad_type DWS 或 DM。实时 ADS 在 10 秒窗口中聚合 DWD 已固化的分类，离线 ADS 从 `dws_advertiser_di` 读取分类 GMV；CTR/CVR/ROAS 只在 ADS/StarRocks View 按基础指标计算。

## Dashboard 展示约定

- 实时 Dashboard 只查询 StarRocks 最新 10 秒窗口；离线 Dashboard 按 `dt` 查询日粒度结果。
- 两者都展示 Cost、闭环 Cost、广告 GMV、ROAS，以及 short_video/live/image_text 和 feed/search/splash/rewarded/banner 的 GMV 分类卡；离线另展示 CTR、CVR、Cost 日趋势和 GMV 日趋势。
- `other_ad_type_*` 与 `other_placement_*` 存于明细/汇总层，以保持分类加和闭合且便于数据质量审计，但不展示于 Dashboard。
