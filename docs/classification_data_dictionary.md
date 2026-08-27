# 广告双分类数据字典

`placement_type` 与 `ad_type` 均归属于 Unit，权威来源是 `dim_unit_df`；`slot_id` 仅表示 SDK 上报的具体广告槽位。

| 字段 | 枚举 | 含义 |
| --- | --- | --- |
| `placement_type` | 1 / 2 / 3 / 4 / 5 / 6 | search / splash / feed / rewarded / banner / other |
| `ad_type` | 1 / 2 / 3 / 4 | short_video / live / image_text / other |

传递链路：SDK 只上报 `creative_id` / `product_id` / `slot_id` / `event` / `ts`，ODS 忠实保留原始 JSON。DWD 通过 `creative_id → dim_creative_df → unit_id → dim_unit_df` 一次性补齐并固化两个分类。无法补维或枚举越界的事件写入 `dwd_ad_event_dirty_di`。

DWS 粒度：

- `dws_unit_di`：`dt + unit_id`，携带已在 DWD 固化的两个分类属性，不展开宽指标。
- `dws_creative_di`：`dt + creative_id`，只保留总体基础指标。
- `dws_campaign_di` 与 `dws_advertiser_di`：保留总体基础指标，并按固定枚举展开订单数、退款数、GMV 和退款 GMV。

最终 DWS 只有 advertiser / campaign / unit / creative 四张主题表，不存在独立的 placement/ad_type DWS 或 DM。实时与离线 ADS 均从 `dws_advertiser_di` 读取分类 GMV；CTR/CVR/ROAS 只在 ADS/StarRocks View 按基础指标计算。
