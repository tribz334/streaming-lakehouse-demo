# 论文功能实现验收表

验收依据：论文第 3 章需求分析、第 4 章系统架构设计、第 5 章系统实现与测试。

## 已实现并可运行

| 论文能力 | 当前实现 | 证据 |
| --- | --- | --- |
| Kafka 埋点事件接入 | 事件生成器持续写入 `ods_log` | `generator/produce_events.py`、Kafka 容器 |
| MySQL 业务数据源 | 广告主、计划、创意、订单数据 | `mysql/init`、MySQL 容器 |
| Flink CDC 业务库同步 | Flink CDC 3.6 完成全量快照并持续消费 binlog，维护广告主、计划、单元、创意和订单 Paimon 表 | `flink-cdc/mysql-to-paimon.yaml`、`submit-cdc-pipeline.ps1` |
| Paimon 湖仓分层 | 保存 ODS/DWD/DIM 及离线 DWS/DM/ADS，支持批量回溯与主题加工 | `00_catalogs_and_tables.sql`、`01_model_tables.sql` |
| 单机精简拓扑 | 1 Kafka Broker、1 Flink TM、1 采集实例，可选 1 StarRocks FE + 1 BE | `docker-compose.yml` |
| Flink 流批处理 | 一个 Java 流任务处理实时短窗口，SQL 批任务刷新离线 DWS/DM/ADS | `flink-java`、`scripts/windows/*.ps1`、`scripts/linux/*.sh` |
| 订单生命周期 | Paimon partial-update 主键表 | `dwd_order_lifecycle_df` |
| 核心广告指标 | Java Flink 作业完成10秒窗口聚合并直接写入保留连续窗口的StarRocks主键表 | `flink-java`、`realtime_ad_metrics_10s` |
| 离线核心指标大盘 | 创意粒度离线 ADS、最新完整分区 KPI、近两周趋势、多维筛选和创意下钻 | `13_ads_creative_offline.sql`、`bootstrap_offline_dashboard.py` |
| 广告主留存 | 次日、7 日、15 日、30 日留存口径 | `10_ads_retention.sql` |
| Schema Registry | Apicurio JSON Schema 注册与查询 | `register-schemas.ps1` |
| StarRocks OLAP | 实时指标由 Flink JDBC Sink 直接 UPSERT；离线 ADS 使用快照 | `StarRocksMetricSink.java`、`init_starrocks.sql`、`sync-starrocks-olap.ps1` |
| Superset 接入 | 注册实时、离线、留存、归因和反作弊数据集并自动生成专题看板 | `superset/bootstrap_datasets.py`、`superset/bootstrap_*dashboard.py` |
| Prometheus | Flink 指标采集和 targets API | `prometheus/prometheus.yml` |
| Hive Metastore | Paimon Flink SQL Catalog 与 Flink CDC Sink 共享 HMS 元数据后端 | `docker-compose.yml`、`00_catalogs_and_tables.sql`、`mysql-to-paimon.yaml` |
| 元数据与血缘导出 | DataHub 风格 JSON 和 MCP-style JSONL | `datahub/metadata`、`datahub/mcp` |
| 本地工作流 | 批刷新、同步、治理、验证和运行历史 | `scripts/windows/init-flink-ddl.ps1`、`run-ads-batches.ps1`、`sync-starrocks-olap.ps1` |
| 归因增强 | 30 天 LastClick；30 分钟直归、1/3/7/30 日间归、自然订单互斥分桶；订单级下钻 | `11_ads_attribution.sql` |
| 反作弊增强 | 点击突增、异常 CTR、集中用户规则 | `12_ads_fraud.sql` |

## 部分实现或采用本地替代

| 论文能力 | 当前差异 |
| --- | --- |
| HDFS + Hive Metastore | Hive Metastore 已接入 Paimon 主链路；数据文件仍使用 Docker volume 文件系统，不是 HDFS。 |
| StarRocks External Catalog 直读 Paimon | Catalog 兼容性仍受版本限制；实时服务使用 Flink JDBC 直写，离线 ADS 使用内部快照。 |
| Superset BI 应用 | 已生成实时、离线、留存、广告归因和广告反作弊五类独立看板；订阅告警和面向终端用户的独立 BI 门户尚未实现。 |
| DolphinScheduler | 提供 YAML 模板、本地 runner、运行历史和 HTML 看板；不是真实 DolphinScheduler 服务。 |
| DataHub | 提供资产、血缘、术语离线导出；不是真实 DataHub UI 和自动字段级血缘采集。 |
| Grafana / Loki | Grafana、Loki 与 Alloy 已接入；Alloy 自动发现本项目 Docker 容器并集中采集日志，Grafana Loki 数据源已实测健康。 |

## 物理环境限制

- 单机三逻辑节点不能证明跨物理机网络开销、机架故障隔离或论文中的硬件吞吐结果。
- YARN real-time/offline/routine/ad-hoc 队列仍以 Compose profile 和作业类型模拟，未部署真实 YARN Capacity Scheduler。
- Flink、Kafka、HDFS、StarRocks 的高可用、多副本和节点故障自动恢复演练。
- DataHub 字段级血缘、负责人维护和影响分析 UI。
- Kerberos、Ranger、字段级访问控制、敏感字段脱敏。
- DolphinScheduler 失败重试、依赖调度和告警通知实测。
- Grafana 告警和 node_exporter 主机监控实测。
- 论文描述的亿级数据、1/2/4 并行度吞吐、端到端时延、查询响应和五轮平均性能测试。
- 完整的 Superset 仪表板、图表和报表订阅。

## 结论

当前项目是可运行的单机 Streaming Lakehouse 论文演示版，并增加了归因和反作弊能力；它不是论文中三节点生产级系统的 100% 等价实现。核心业务数据链路可演示，生产级分布式、治理、安全、运维和性能实验仍需继续建设。
