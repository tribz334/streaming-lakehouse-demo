# 论文功能实现验收表

验收依据：论文第 3 章需求分析、第 4 章系统架构设计、第 5 章系统实现与测试。

## 已实现并可运行

| 论文能力 | 当前实现 | 证据 |
| --- | --- | --- |
| Kafka 埋点事件接入 | 事件生成器持续写入 `ods_log` | `generator/produce_events.py`、Kafka 容器 |
| MySQL 业务数据源 | 广告主、计划、创意、订单数据 | `mysql/init`、MySQL 容器 |
| Paimon 湖仓分层 | ODS、DIM、DWD、DWS、ADS 表 | `flink/sql/00_catalogs_and_tables.sql` |
| Flink 流批处理 | ODS/DWD 长期流任务，DWS/ADS 有界批刷新 | `scripts/windows/*.ps1`、`scripts/linux/*.sh` |
| 订单生命周期 | Paimon partial-update 主键表 | `dwd_order_lifecycle_df` |
| 核心广告指标 | 消耗、GMV、曝光、点击、转化、CTR、CVR、ROI | `dws_ad_metric_10s`、StarRocks 视图 |
| 广告主留存 | 次日、7 日、15 日、30 日留存口径 | `04_ads_retention_batch.sql` |
| Schema Registry | Apicurio JSON Schema 注册与查询 | `register-schemas.ps1` |
| StarRocks OLAP | DWS/ADS 同步到内部快照表并提供统一视图 | `sync-starrocks-olap.ps1` |
| Superset 接入 | 注册实时指标、留存、归因、反作弊四个数据集 | `superset/bootstrap_datasets.py` |
| Prometheus | Flink 指标采集和 targets API | `prometheus/prometheus.yml` |
| 元数据与血缘导出 | DataHub 风格 JSON 和 MCP-style JSONL | `datahub/metadata`、`datahub/mcp` |
| 本地工作流 | 批刷新、同步、治理、验证和运行历史 | `scripts/windows/run-demo-workflow.ps1` |
| 归因增强 | 7 日最后点击归因 | `05_ads_attribution_batch.sql` |
| 反作弊增强 | 点击突增、异常 CTR、集中用户规则 | `06_ads_fraud_batch.sql` |

## 部分实现或采用本地替代

| 论文能力 | 当前差异 |
| --- | --- |
| Flink CDC 3.x 整库同步 | MySQL 已开启 binlog，也保留 CDC YAML；可运行主链路使用 JDBC 维表装载，未完成严格全量快照转增量 CDC 验证。 |
| HDFS + Hive Metastore | Hive Metastore 已运行；Paimon 数据使用 Docker volume 文件系统，不是三节点 HDFS。 |
| StarRocks External Catalog 直读 Paimon | Catalog 可创建，但当前 StarRocks 3.1 与 Paimon 1.1.1 snapshot 元数据不兼容，因此采用内部快照同步。 |
| 10 秒实时 DWS 大盘 | 有 10 秒窗口指标，但本机为稳定运行将 DWS 改为批量刷新；不是论文描述的持续 10 秒可见链路。 |
| Superset BI 应用 | 四个数据集已注册，可进入 Explore；完整广告大盘、渠道分析图表、订阅和多维下钻产品页尚未制作。 |
| DolphinScheduler | 提供 YAML 模板、本地 runner、运行历史和 HTML 看板；不是真实 DolphinScheduler 服务。 |
| DataHub | 提供资产、血缘、术语离线导出；不是真实 DataHub UI 和自动字段级血缘采集。 |
| Grafana / Loki | 配置文件已保留，本地运维 HTML 看板可用；镜像未成功拉取，真实 Grafana/Loki 未运行。 |

## 尚未实现

- 三节点分布式集群与 YARN real-time/offline/routine/ad-hoc 队列隔离。
- Flink、Kafka、HDFS、StarRocks 的高可用、多副本和节点故障自动恢复演练。
- DataHub 字段级血缘、数据质量规则体系、负责人维护和影响分析 UI。
- Kerberos、Ranger、字段级访问控制、敏感字段脱敏。
- DolphinScheduler 失败重试、依赖调度和告警通知实测。
- Grafana 告警、Loki 日志检索和 node_exporter 主机监控实测。
- 论文描述的亿级数据、1/2/4 并行度吞吐、端到端时延、查询响应和五轮平均性能测试。
- 完整的 Superset 仪表板、图表和报表订阅。

## 结论

当前项目是可运行的单机 Streaming Lakehouse 论文演示版，并增加了归因和反作弊能力；它不是论文中三节点生产级系统的 100% 等价实现。核心业务数据链路可演示，生产级分布式、治理、安全、运维和性能实验仍需继续建设。
