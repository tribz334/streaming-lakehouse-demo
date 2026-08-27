# Implementation audit

| Requirement | Implementation | Evidence |
| --- | --- | --- |
| 日志直写 ODS | SDK JSON 顶层拆为元数据、`common`、`events` 后直接追加到 Fluss `ods_log_di`，不经过 MySQL；校验后续接入 | `generator/produce_events.py`, `flink/sql/00_bootstrap.sql` |
| Fluss 秒级热层 | ODS、7 张宽维、行为/账单/订单 DWD；按天物理分区并保留小时字段 | `flink/sql/00_bootstrap.sql` |
| Paimon 落盘 | 所有 Fluss 热表开启 lake，Tiering freshness 30 秒 | `flink/sql/00_bootstrap.sql`, `scripts/*/submit-streaming-jobs.*` |
| 实时广告 GMV | 6 小时 `uid + product_id` LastClick 归因；行为、账单、订单增量进入可配置的 10 秒事件时间滚动窗口并写 Fluss | `flink-java/.../DwdOrderAttributionJob.java`、`flink-java/.../RealtimeAdMetricJob.java` |
| 一天一个离线单位 | 单日覆盖 advertiser/unit/creative 三张 DWS，并滚动生成 1/7/30/累计 DM 快照 | `flink/sql/10_daily_offline.sql`, `scripts/*/run-daily-batch.*` |
| DBeaver 可见 | StarRocks `ad_ads` 物理表/视图以及 `paimon_catalog.ad_dw` 外部湖表 | `starrocks/init_starrocks.sql` |
| 删除冗余 | 维度 CDC 直接写 DIM，仅日志、账单、订单保留 ODS；旧 Kafka、HMS 和重复 SQL 实时链路已移除 | repository tree |

当前常驻链路为 Tiering、业务库 CDC、账单补维 SQL、日志 DataStream、6 小时 LastClick DataStream、归因订单事件 SQL、日主题 SQL和 10 秒实时 ADS DataStream。

当前 Docker Compose 是单 Coordinator、单 Tablet、单 Flink TaskManager、单 StarRocks FE/BE 的本地演示拓扑。生产部署仍需多副本、对象存储、独立 checkpoint、凭据管理、资源隔离和告警。
