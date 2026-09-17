# Flink + Fluss + Paimon + StarRocks 广告湖仓

本项目按自定义广告数仓模型运行：SDK 日志直接追加到 Fluss 实时 ODS；Flink CDC 只读取 MySQL 业务表。Fluss 承载 ODS/DIM/DWD 和 10 秒 ADS 热数据，Tiering Service 自动落盘到 Paimon，StarRocks 服务实时与离线 ADS 查询。

## 数据链路

```text
SDK 埋点 JSON ── Fluss Client ──► Fluss ods_log_di（实时 ODS）
                                      │ Flink DataStream JSON 解析
MySQL 广告、用户、商品、账单、订单表    │
        │ Flink CDC（initial + binlog）▼
        └──────────────────────► Fluss DIM / DWD ── 6 小时 LastClick ──► 10 秒 ADS
        │
        │ Fluss Tiering Service（实时 ADS freshness=5s，其余=30s）
        ▼
Paimon ODS / DIM / DWD / 10 秒 ADS ── 单日离线任务 ──► 1d DWS ──► 7d/30d/累计 DM
                                                        └─► 平台 ADS ──► StarRocks
```

主要表：

- ODS：仅保留 `ods_log`和 `ods_mysql_order_info`；日志直写 Fluss，订单由 MySQL CDC 写入。
- DIM：MySQL CDC 直接同步 `dim_advertiser`、`dim_campaign`、`dim_unit`、`dim_creative`、`dim_user`、`dim_product`、`dim_shop`。
- Fluss ODS/DIM 均由 Tiering Service 沉淀为同名 Paimon 表；日志的 `common`、`events` 内部字段留到 DWD 解析。
- DWD：保留 `dwd_ad_event_di`、`dwd_ad_bill_di`、`dwd_ad_order_acc` 三张核心事实表；`bill_info` 由 Flink CDC 直接写入 `dwd_ad_bill_di`。
- 直接归因：广告点击与订单按 `uid + product_id` 匹配，取下单前 6 小时内的 LastClick；该口径与 Fluss 行为事实表的 6 小时热日志保留周期配套。
- 分类：Unit 同时拥有 `placement_type`（1=feed、2=search、3=splash、4=rewarded、5=banner、6=other）与 `ad_type`（1=short_video、2=live、3=image_text、4=other）。SDK 和三张核心 DWD 不固化分类字段，汇总任务按 `creative_id/unit_id` 关联 Fluss DIM 获取。
- 闭环 Cost：账单事实只保存 `cost`；实时与离线汇总在聚合前按 `unit_id` 关联 `dim_unit.is_closed`，值为 1 时计入闭环 Cost。
- DWS：只保留 advertiser / campaign / unit / creative 四张表。Unit 携带两个分类属性；Campaign 与 Advertiser 保存分类订单/GMV 宽指标。
- DM：保留 advertiser / campaign / unit / creative 每日全量主题快照，指标统一使用 `_1d`、`_7d`、`_30d`、`_lifetime` 后缀。
- ADS：实时 `ads_realtime_metric_10s`；离线经营大盘 `ads_offline_metric_di`；支付订单归因明细 `ads_order_attribution_di`；广告主留存 `ads_advertiser_retention_di`。离线归因桶为 `DIRECT/1D/7D/30D/ORGANIC`。

金额在 Fluss/Paimon 中使用 `BIGINT` 存储，单位为“千分之一分”，即 1 元 = 100000 存储单位。StarRocks 的实时、离线和订单归因经营服务视图统一换算为元；Dashboard 消费这些服务视图，不再二次换算。`v_dws_*`/`v_dm_*` 是便于钻取的原始镜像视图，其金额仍保留存储单位。

表后缀统一为：`_di` = Daily Increment，`_df` = Daily Full；指标的全生命周期累计统一使用 `_lifetime`。

双分类枚举、归属层级与各 DWS 粒度见 `docs/classification_data_dictionary.md`。

DWD 保留 `dt` 和 `hour` 字段，但仅按 `dt` 做物理分区。这样小时过滤能力不变，同时避免历史快照按 `dt × hour × bucket` 创建数千分区。

## 启动和日批

```powershell
Copy-Item .env.example .env
./scripts/windows/start-stack.ps1 -WithBi
./scripts/windows/materialize-demo-history.ps1 -Days 14
./scripts/windows/verify-stack.ps1 -WithBi
```

`event-generator` 默认生成 35 天可重现、且 Cost、转化与归因分布合理的历史数据。`materialize-demo-history.ps1` 等待该历史数据到达 DWS 后，默认物化最近 14 天的离线 ADS/DM；`verify-stack.ps1` 校验 7 个常驻 Flink 作业、MySQL 事实、StarRocks 指标，`-WithBi` 还会校验 Superset 健康状态。

若已有本地 demo 数据的运行时表结构与当前代码不兼容，在确认这些演示数据可重建后执行：

```powershell
./scripts/windows/start-stack.ps1 -WithBi -RebuildRuntimeSchema
./scripts/windows/materialize-demo-history.ps1 -Days 14
./scripts/windows/verify-stack.ps1 -WithBi
```

`-RebuildRuntimeSchema` 会停止流作业，重建 Fluss/Paimon 运行时表，然后由生成器和日批重建演示数据。这是本地 demo 的破坏性迁移开关，不是生产环境的在线 schema migration 方案。

日常单日日批或补跑：

```powershell
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

## Dashboard 口径

- 实时 Dashboard 只读 StarRocks 的最新 10 秒窗口，展示 Cost、闭环 Cost、广告 GMV、ROAS，以及所有非 `other` 的 `ad_type`/`placement_type` GMV 分类卡。
- 离线 Dashboard 展示同一组经营与分类指标，并增加 CTR、CVR、Cost 日趋势和广告 GMV 日趋势。
- `other_ad_type_*` 和 `other_placement_*` 仍在数仓中保留，用于口径闭合与质量审计，但不挂载到 Dashboard。Dashboard 中的金额均为元。

## 作业与验证

| 文件 | 职责 |
| --- | --- |
| `flink/sql/00_bootstrap.sql` | 创建自定义 Fluss 热表和 Paimon DWS/DM/ADS 表 |
| `flink/sql/02_database_cdc_to_fluss.sql` | 业务库 CDC 写 DIM、账单和订单 DWD |
| `flink-java/.../DwdLogDataStreamJob.java` | 从 Fluss ODS 读取 JSON，经 DataStream 校验、分流并生成广告行为 DWD |
| `flink-java/.../DwdOrderAttributionJob.java` | 完成首次支付识别、6 小时 LastClick 归因并维护单行累计订单；PAY/REFUND 由其 Fluss PK changelog 识别 |
| `flink-java/.../DwsAdCreativeJob.java` | 合并三类 Creative 指标字段并生成可配置的 10 秒滚动窗口汇总 |
| `flink/sql/02b_ods_changelog_to_dwd.sql` | 账单 ODS 补维并写入账单 DWD |
| `flink/sql/03_realtime_order_dws.sql` | 生成 advertiser / campaign / unit / creative 四张日主题表 |
| `flink/sql/10_daily_offline.sql` | 发布单业务日 ADS，并以前一日 DM、当日 DWS 和滑出窗口 DWS 滚动计算 1d/7d/30d/lifetime DM |
| `flink/sql/11_initialize_dm.sql` | 从 DWS 历史首次初始化四张 DM 每日全量主题快照 |
| `flink/sql/12_migrate_classification_metrics.sql` | 停流后一次性重建双分类主题表和 ADS/DM 表 |
| `starrocks/init_starrocks.sql` | 创建 DWS、DM、ADS 的 StarRocks 查询视图 |

```powershell
docker compose exec -T flink-jobmanager flink list -r
docker compose exec -T mysql mysql --protocol=TCP --host=starrocks --port=9030 --user=root -e "SHOW TABLES FROM ad_ads; SHOW TABLES FROM paimon_catalog.ad_dw"
```

旧 Kafka 总线、Kafka Connector、Hive Metastore、重复 SQL 实时链路和 JDBC sink 均已删除。DataStream 作业通过 Fluss Table API 读写，不需要 Kafka。`90_*` 与 `91_*` 是已执行的一次性迁移记录，不应加入日常启动流程。

## 双入口流量生成

物理入口保持独立：SDK 埋点进 Fluss，账单/订单/维度变化进 MySQL CDC。默认 `event-generator` 会在两个入口上生成同一用户旅程的 35 天历史数据和持续实时数据；下列命令可用于独立压测：

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

## 演示与生产边界

当前 Docker Compose 是论文可复现的本地单节点演示：单 Fluss Coordinator/Tablet、单 Flink JobManager/TaskManager、单 StarRocks FE/BE，使用本地持久化卷、demo 凭据和可再生历史数据。它用于验证论文的数据流、分层、口径和可视化，不代表生产容灾能力。

论文中的生产目标架构应将 Fluss/Flink/StarRocks 按容量与故障域多副本部署，使用对象存储、持久化 checkpoint/savepoint、密钥管理、资源隔离、编排、监控告警与备份恢复。`-RebuildRuntimeSchema` 和 demo 数据重生均不属于该生产目标。
