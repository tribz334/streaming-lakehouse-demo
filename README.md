# Flink + Fluss + Paimon + StarRocks 广告湖仓

本项目按自定义广告数仓模型运行：SDK 日志直接追加到 Fluss 实时 ODS；Flink CDC 只读取 MySQL 业务表。Fluss 承载 ODS/DIM/DWD 和 30 秒 DWS 热数据，Tiering Service 自动落盘到 Paimon，StarRocks 服务离线 ADS 查询。

## 数据链路

```text
SDK 埋点 JSON ── Fluss Client ──► Fluss ods_log_di（实时 ODS）
                                      │ Flink JSON 解析
MySQL 广告、用户、商品、账单、订单表    │
        │ Flink CDC（initial + binlog）▼
        └──────────────────────► Fluss DIM / DWD ── 6 小时直接归因 ──► 30 秒 DWS
        │
        │ Fluss Tiering Service（freshness=30s）
        ▼
Paimon ODS / DIM / DWD / 30 秒 DWS ── 单日离线任务 ──► 1d DWS ──► 7d/30d/累计 DM
                                                        └─► 平台 ADS ──► StarRocks
```

主要表：

- ODS：仅保留 `ods_log_di`、`ods_mysql_bill_di`、`ods_mysql_order_acc`；日志直写 Fluss，账单和订单由 MySQL CDC 写入。
- DIM：MySQL CDC 直接同步 `dim_advertiser_df`、`dim_campaign_df`、`dim_unit_df`、`dim_creative_df`、`dim_user_df`、`dim_product_df`、`dim_shop_df`。
- Fluss ODS/DIM 均由 Tiering Service 沉淀为同名 Paimon 表；日志的 `common`、`events` 内部字段留到 DWD 解析。
- DWD：`dwd_ad_event_di`、`dwd_ad_bill_di`、`dwd_ad_order_acc`、`dwd_ad_order_di`。
- 直接归因：广告点击与订单按 `uid + product_id` 匹配，取下单前 6 小时内的 Last Click。
- DWS：只保留 `dws_advertiser_di`、`dws_unit_di`、`dws_creative_di` 三张日主题表；每张表仅保存自身 ID、名称和基础可加指标。
- DM：`dm_advertiser_df`、`dm_unit_df`、`dm_creative_df` 保存每日全量主题快照，指标统一使用 `_1d`、`_7d`、`_30d`、`_acc` 后缀。
- ADS：实时 `ads_realtime_metric_30s`；离线经营大盘 `ads_offline_metric_di`；支付订单归因明细 `ads_order_attribution_di`；广告主留存 `ads_advertiser_retention_di`。归因桶为 `DIRECT/7D/30D/LONG_TERM/ORGANIC`。

表后缀统一为：`_di` = Daily Increment，`_df` = Daily Full，`_acc` = Accumulating Snapshot。

DWD 保留 `dt` 和 `hour` 字段，但仅按 `dt` 做物理分区。这样小时过滤能力不变，同时避免历史快照按 `dt × hour × bucket` 创建数千分区。

## 启动和日批

```powershell
Copy-Item .env.example .env
./scripts/windows/start-stack.ps1 -WithBi
./scripts/windows/run-daily-batch.ps1
./scripts/windows/run-daily-batch.ps1 -BizDate 2026-08-21
```

日批默认处理昨天；指定日期可幂等补跑。

## DBeaver 连接

使用 MySQL 驱动连接 StarRocks：Host `127.0.0.1`，Port `19030`，User `root`，Password 留空，Database `ad_ads`。

刷新连接后可见：

- `ad_ads`：通过 StarRocks 的 `paimon_catalog` 提供实时与离线 ADS 视图。
- `paimon_catalog`：永久外部 Catalog，可展开查看 Tiering 和离线 Paimon 表。

DBeaver 若仍缓存旧表，右键连接执行 **Invalidate/Reconnect**，再刷新 `Catalogs`。

## 服务地址

- Flink UI：`http://127.0.0.1:18082`
- Fluss bootstrap：`127.0.0.1:19123`
- StarRocks FE：`http://127.0.0.1:18030`
- StarRocks SQL / DBeaver：`127.0.0.1:19030`
- Superset：`http://127.0.0.1:18088`，账号 `admin/admin`

## 作业与验证

| 文件 | 职责 |
| --- | --- |
| `flink/sql/00_bootstrap.sql` | 创建自定义 Fluss 热表和 Paimon DWS/DM/ADS 表 |
| `flink/sql/02_database_cdc_to_fluss.sql` | 业务库 CDC 写 DIM、账单和订单 DWD |
| `flink/sql/02b_ods_changelog_to_dwd.sql` | 直接消费 Fluss ODS JSON 并生成广告行为 DWD |
| `flink/sql/02c_direct_order_attribution.sql` | 按 `uid + product_id` 做 6 小时 Last Click 归因 |
| `flink/sql/02d_attributed_order_to_event.sql` | 将广告订单拆成 PAY/REFUND 事件 |
| `flink/sql/03_realtime_order_dws.sql` | 从广告订单 PAY/REFUND 事件生成 30 秒 GMV 窗口 |
| `flink/sql/10_daily_offline.sql` | 单业务日重算 DWS、1/7/30/累计 DM 并发布 |
| `flink/sql/11_initialize_dm.sql` | 首次初始化三张 DM 每日全量主题快照 |
| `starrocks/publish_daily.sql` | 将平台 ADS 幂等发布到 StarRocks |

```powershell
docker compose exec -T flink-jobmanager flink list -r
docker compose exec -T mysql mysql --protocol=TCP --host=starrocks --port=9030 --user=root -e "SHOW TABLES FROM ad_ads; SHOW TABLES FROM paimon_catalog.ad_dw"
```

旧 Kafka 总线、Java DataStream/JDBC sink、Hive Metastore、重复 ADS 链路和旧简化表模型均已删除。`90_*` 与 `91_*` 是已执行的一次性迁移记录，不应加入日常启动流程。

## 双入口流量生成

SDK 埋点和 MySQL CDC 业务变化相互独立：

```bash
# SDK JSON -> Fluss ods_log_di
python generator/produce_events.py --rate 800

# order / bill / DIM -> MySQL binlog -> Flink CDC
python generator/produce_mysql_changes.py --rate 30 --duration 1800
```

容器方式启动持续 MySQL CDC 流量：

```bash
docker compose --profile mysql-cdc-generator up -d mysql-change-generator
```

`produce_mysql_changes.py` 默认按 Bill 60%、订单 38%、DIM 2% 产生随机抖动流量；订单只执行合法生命周期迁移，并通过 `pay_time`、`refund_time` 从 NULL 变为非 NULL 触发下游 PAY/REFUND 识别。
