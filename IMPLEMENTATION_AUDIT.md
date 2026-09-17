# Implementation audit

| Requirement | Implementation | Evidence |
| --- | --- | --- |
| 日志直写 ODS | SDK JSON 顶层拆为元数据、`common`、`events` 后直接追加到 Fluss `ods_log_di`，不经过 MySQL；校验后续接入 | `generator/produce_events.py`, `flink/sql/00_bootstrap.sql` |
| Fluss 秒级热层 | ODS、7 张宽维、行为/账单/订单 DWD；按天物理分区并保留小时字段 | `flink/sql/00_bootstrap.sql` |
| Paimon 落盘 | Fluss 热表开启 lake；10 秒实时 ADS 的 Tiering freshness 为 5 秒，其余为 30 秒 | `flink/sql/00_bootstrap.sql`, `scripts/*/submit-streaming-jobs.*` |
| 实时经营指标 | 三类 Creative 指标字段进入可配置的 10 秒事件时间滚动窗口；账单通过 `dim_creative.is_closed` 计算闭环 Cost | `flink-java/.../DwsAdCreativeJob.java` |
| 实时广告 GMV | 订单事实作业统一完成支付识别、6 小时 `uid + product_id` LastClick 归因和 PAY/REFUND 抽取 | `flink-java/.../DwdOrderAttributionJob.java` |
| 一天一个离线单位 | 单日覆盖 advertiser/campaign/unit/creative 四张 DWS，并滚动生成同四个粒度的 1/7/30/累计 DM 快照；订单归因桶为 `DIRECT/1D/7D/30D/ORGANIC` | `flink/sql/10_daily_offline.sql`, `scripts/*/run-daily-batch.*` |
| 金额单位 | 湖仓 `BIGINT` 以千分之一分存储（1 元 = 100000）；StarRocks 经营服务视图以元输出 | `mysql/init/01_schema.sql`, `starrocks/init_starrocks.sql` |
| Dashboard | 实时只读最新窗口；实时/离线均展示 Cost、闭环 Cost、广告 GMV、ROAS 及非 `other` 分类卡，离线另有 CTR/CVR 和 Cost/GMV 日趋势 | `superset/bootstrap_dashboard.py`, `superset/bootstrap_datasets.py`, `starrocks/init_starrocks.sql` |
| 可重现历史数据 | 默认生成 35 天合理的 Cost、转化和归因分布，再物化离线日分区 | `generator/produce_events.py`, `scripts/windows/materialize-demo-history.ps1` |
| DBeaver 可见 | StarRocks `ad_ads` 服务视图以及 `paimon_catalog.ad_dw` 外部湖表 | `starrocks/init_starrocks.sql` |
| 删除冗余 | 维度 CDC 直接写 DIM，仅日志、账单、订单保留 ODS；旧 Kafka、HMS 和重复 SQL 实时链路已移除 | repository tree |

当前常驻链路为 Tiering、业务库 CDC、账单补维 SQL、日志 DataStream、统一订单事实 DataStream、日主题 SQL 和 10 秒实时 ADS DataStream。

本地启动闭环为 `start-stack.ps1 -WithBi` → `materialize-demo-history.ps1` → `verify-stack.ps1 -WithBi`。对旧 demo 运行时元数据，`start-stack.ps1 -RebuildRuntimeSchema` 会停流并重建 Fluss/Paimon 表；它仅适用于已确认可重生的演示数据。

当前 Docker Compose 是单 Coordinator、单 Tablet、单 Flink TaskManager、单 StarRocks FE/BE 的论文本地复现拓扑，只用于验证数据流、口径和展示。论文的生产目标仍需多副本、对象存储、持久化 checkpoint/savepoint、凭据管理、资源隔离、编排、监控告警和备份恢复，不采用 demo 的破坏性重建开关。
