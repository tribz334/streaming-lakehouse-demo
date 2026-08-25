# Implementation audit

| Requirement | Implementation | Evidence |
| --- | --- | --- |
| 日志直写 ODS | SDK JSON 顶层拆为元数据、`common`、`events` 后直接追加到 Fluss `ods_log_di`，不经过 MySQL；校验后续接入 | `generator/produce_events.py`, `flink/sql/00_bootstrap.sql` |
| Fluss 秒级热层 | ODS、7 张宽维、行为/账单/订单 DWD；按天物理分区并保留小时字段 | `flink/sql/00_bootstrap.sql` |
| Paimon 落盘 | 所有 Fluss 热表开启 lake，Tiering freshness 30 秒 | `flink/sql/00_bootstrap.sql`, `scripts/*/submit-streaming-jobs.*` |
| 实时广告 GMV | 6 小时 `uid + product_id` Last Click 直接归因，30 秒窗口写 Fluss | `flink/sql/02c_direct_order_attribution.sql`、`flink/sql/03_realtime_order_dws.sql` |
| 一天一个离线单位 | 单日覆盖 advertiser/unit/creative 三张 DWS，并滚动生成 1/7/30/累计 DM 快照 | `flink/sql/10_daily_offline.sql`, `scripts/*/run-daily-batch.*` |
| DBeaver 可见 | StarRocks `ad_ads` 物理表/视图以及 `paimon_catalog.ad_dw` 外部湖表 | `starrocks/init_starrocks.sql` |
| 删除冗余 | 维度 CDC 直接写 DIM，仅日志、账单、订单保留 ODS；旧 Kafka、Java、HMS 和重复链路已移除 | repository tree |

历史验证（2026-08-22）基于旧日志 CDC 链路；改为 Fluss 直写后的运行验证需重新执行。当前目标常驻作业为 Tiering、业务库 CDC、ODS-to-DWD 和实时 ADS 共 4 个。

当前 Docker Compose 是单 Coordinator、单 Tablet、单 Flink TaskManager、单 StarRocks FE/BE 的本地演示拓扑。生产部署仍需多副本、对象存储、独立 checkpoint、凭据管理、资源隔离和告警。
