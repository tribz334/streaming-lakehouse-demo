# Real Streaming Lakehouse Stack

这个目录是论文系统的主要实现，使用 Docker Compose 组织真实技术栈组件。

> 当前实现包含论文一致的数据模型与单机三逻辑节点拓扑。三逻辑节点用于复现组件部署、分区副本、并行计算和故障演练，不等价于三台物理主机的性能数据。逐项边界见 `IMPLEMENTATION_AUDIT.md`。

## 技术栈映射

| 论文模块 | 本目录实现 |
| --- | --- |
| 业务库 | MySQL 8.4，开启 binlog/ROW/GTID |
| 埋点日志 | `event-generator-node-1` 写入 Kafka `ods_log` |
| Kafka 消息总线 | Apache Kafka 3.9.1 KRaft 单节点 |
| Flink CDC | MySQL binlog 已开启，CDC YAML 配置保留；当前可运行链路用 Flink JDBC 4.0 装载维表 |
| Paimon 湖仓 | Paimon Flink 2.0 connector，warehouse: `/warehouse/paimon` |
| Flink 流批一体 | Flink 2.0.2 JobManager/TaskManager |
| 流批不对称数仓 | 共享 ODS/DWD/DIM；实时止于 DWS；离线增加 DWM、DM、ADS |
| 论文数据字典 | `01_model_tables.sql` 定义当前保留的 DWS/DM 核心表 |
| 订单生命周期 | Paimon `partial-update` 主键表 |
| OLAP 服务 | StarRocks FE/BE；实时指标由 Kafka Routine Load 持续写 Primary Key 表，离线 ADS 使用内部快照 |
| BI 应用 | Superset 3.0.0，自动注册 StarRocks 数据库与四个 dataset |
| Schema Registry | Apicurio Registry 3.2.5，已注册 Kafka `ods_log-value` JSON schema |
| 运维观测 | Prometheus、Grafana、Loki、Alloy 已验证；容器日志支持按服务、节点和角色集中检索；`ops-dashboard/index.html` 本地看板已生成 |
| 元数据服务 | Hive Metastore 4.0.1，Derby demo 存储，宿主机映射端口 `19083` |
| 数据治理 | `scripts/windows/export-governance-metadata.ps1` 导出 DataHub 风格 URN、血缘、术语 JSON；`export-datahub-mcp.ps1` 导出 MCP-style JSONL |
| 调度编排 | `scripts/linux/*.sh` 容器/Linux 任务入口；`dolphinscheduler/workflows/ad-lakehouse-demo.yaml` 保存 DAG 模板 |

说明：论文写的是 Flink 2.0 + Paimon 1.0。Maven 当前可用的 Flink 2.0 专用 Paimon bridge 从 1.1.x 起提供，因此默认使用 `paimon-flink-2.0:1.1.1`。如果严格使用 Paimon 1.0，可把 Flink 降到 1.20 并改用 `paimon-flink-1.20`。

## 脚本分层

```text
scripts/
├─ windows/  # Windows 宿主机的 PowerShell 启动、运维和数据同步脚本
└─ linux/    # Linux 宿主机或调度 worker 使用的 Bash 任务入口
```

Windows 用户从 `scripts/windows/start-multi-node.ps1` 启动。Linux 任务使用 `init-flink-ddl.sh`、`submit-streaming-jobs.sh`、`run-ads-batches.sh`、`sync-starrocks.sh` 和 `verify-stack.sh`。其中 StarRocks 同步暂时通过跨平台 PowerShell Core 复用已验证的解析实现。

## 启动步骤

先启动 Docker Desktop，然后在本目录运行多节点脚本：

```powershell
./scripts/windows/start-multi-node.ps1
```

该模式通过 `docker-compose.three-node.yml` 在同一台宿主机上启动三套逻辑节点：

```text
node-1: MySQL + Kafka-node-1 + Flink JM/TM + event-generator-node-1
node-2: Kafka-2 + Flink TM-2 + event-generator-2
node-3: Kafka-3 + Flink TM-3 + event-generator-3
```

`ods_log` 固定为 6 分区、3 副本、最小同步副本 2。需要同时启动三节点
StarRocks BE 和完整监控栈时使用：

```powershell
./scripts/windows/start-multi-node.ps1 -WithOlap -WithOps
```

验证多节点状态：

```powershell
./scripts/windows/verify-multi-node.ps1
```

该实验环境用于验证多节点拓扑、任务注册、并行计算 slot、数据采集节点协同和流式链路可运行性。由于所有容器仍部署在同一台物理主机上，它属于单机多 Docker 的逻辑多节点环境，不用于证明跨物理机网络开销或真实多机容灾能力。

也可以分步骤运行：

```powershell
./scripts/windows/download-flink-jars.ps1
docker compose -f docker-compose.yml -f docker-compose.three-node.yml --profile core --profile multi-node up -d --build
./scripts/windows/init-flink-ddl.ps1
./scripts/windows/submit-streaming-jobs.ps1
```

等 Kafka 事件流和 Flink 作业运行一会儿后，执行：

```powershell
./scripts/windows/run-ads-batches.ps1
./scripts/windows/verify-multi-node.ps1
```

OLAP / BI 层：

```powershell
docker compose --profile olap up -d starrocks starrocks-be-node-1
./scripts/windows/init-starrocks.ps1
./scripts/windows/sync-starrocks-olap.ps1
```

只刷新 Superset 使用的留存数据时，可执行：

```powershell
./scripts/windows/sync-starrocks-olap.ps1 -Dataset Retention
```

访问入口：

- Flink UI: http://127.0.0.1:8082
- StarRocks FE: http://127.0.0.1:8030
- Superset: http://127.0.0.1:8088
- Apicurio Registry: http://127.0.0.1:8081/apis/registry/v3/system/info
- Prometheus: http://127.0.0.1:19090
- Hive Metastore Thrift: 127.0.0.1:19083
- Local Ops Dashboard: `ops-dashboard/index.html`
- Local Scheduler Dashboard: `dolphinscheduler/dashboard/index.html`
- DolphinScheduler: http://127.0.0.1:12345/dolphinscheduler/ui/ (`admin` / `dolphinscheduler123`)
- Grafana: http://127.0.0.1:13000
- Grafana Alloy: http://127.0.0.1:12346

启动集中日志采集：

```powershell
docker compose --profile ops up -d prometheus loki alloy grafana
```

打开 Grafana 的 `Explore -> Loki`，可直接使用以下 LogQL：

```logql
{service="flink-jobmanager"}
{service="event-generator-node-1"} |= "ERROR"
{service=~"flink-.*"} |~ "(?i)exception|error|failed"
{node="node-2"}
```

Alloy 通过 Docker API 自动采集当前 Compose 项目的容器标准输出，并写入 `service`、`service_name`、`container`、`node`、`role`、`environment` 和 `platform` 标签。首次启动后可在 Grafana 的 Label browser 中选择 `service` 查看已经入库的服务。

Superset 默认账号：

```text
admin / admin
```

## 数据链路

1. MySQL 初始化广告主、计划、创意、订单表。
2. 事件生成器持续写 Kafka `ods_log`，订单事件同时写 MySQL `ad_order`，保留 CDC 输入条件。
3. Flink SQL 读取 Kafka 和 MySQL JDBC 维表，长期写入共享 Paimon ODS/DIM/DWD；严格 CDC 版本见 `flink-cdc/mysql-to-paimon.yaml`。
4. 实时链路持续产出 `dws_ad_metric_stream_10s`；`05_realtime_starrocks_relay.sql` 将其主键 changelog 写入 Kafka，StarRocks Routine Load 在 5 秒批次内持续 Upsert 到实时服务表。离线链路继续物化主题 DWS、DM 和 ADS。
5. Flink batch SQL 计算 ADS：
   - `ads_advertiser_retention_di`：广告主留存。
   - `ads_order_attribution_detail_di`：订单级 30 天 LastClick 明细，互斥区分 30 分钟直接归因、1/3/7/30 日间接归因和自然订单。
   - `ads_attribution_summary_di`：按日期、广告主、活动和归因窗口汇总订单量、GMV 与点击消耗。
   - `ads_creative_offline_di`：创意粒度离线 BI 数据集，聚合 DWS 事实并补齐广告主、计划、单元和创意维度。
   - `ads_fraud_signal_di`：demo 流量规模下的高点击、异常 CTR、集中用户点击规则信号。
6. Superset 连接 StarRocks，自动注册业务 dataset，并分别生成实时核心指标、离线核心指标、留存、广告归因和广告反作弊看板。广告归因看板独立展示 30 分钟、1/3/7/30 日与自然订单的占比、趋势和订单级下钻；广告反作弊看板独立展示可疑用户、点击、消耗、风险评分与广告主级下钻。离线核心指标看板默认查看近 14 天，支持日期、广告主、行业、投放目标和创意形式筛选，以及创意明细下钻。
8. `export-governance-metadata.ps1` 导出 DataHub 风格离线元数据，覆盖 Kafka、Paimon、StarRocks 资产和核心血缘；`export-datahub-mcp.ps1` 额外导出 `datahub/mcp/metadata_change_proposals.jsonl`。
9. `register-schemas.ps1` 向 Apicurio 注册 `ad-demo/ods_log-value` JSON schema。
10. `generate-ops-dashboard.ps1` 汇总 Flink、Prometheus、StarRocks、治理元数据、调度状态和运行时 fallback，生成本地 HTML 运维看板。
11. `bootstrap-dolphinscheduler.ps1` 通过 DolphinScheduler OpenAPI 注册离线与实时两条业务 DAG；实时 DAG 在指标 DWS 后额外启动 StarRocks relay，并校验五个常驻实时作业。

## 实时指标服务

实时查询只读 `ad_ads.v_realtime_ad_metrics`，底表是 StarRocks Primary Key 表。Flink 的 upsert-kafka 保留 Paimon 主键更新，Routine Load 按相同复合主键幂等写入，并以 `updated_at` 阻止旧消息覆盖新结果。当前时效上限由 10 秒事件时间窗口、5 秒 watermark 和 Routine Load 最多 5 秒批次共同决定，不依赖人工执行批同步。

## 常用命令

```powershell
docker compose --profile core ps
docker compose --profile core logs -f flink-jobmanager
docker compose --profile core logs -f event-generator-node-1
docker compose --profile olap exec -T starrocks bash -lc "mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW CATALOGS;'"
./scripts/windows/run-ads-batches.ps1
./scripts/windows/sync-starrocks-olap.ps1
./scripts/windows/register-schemas.ps1
./scripts/windows/export-governance-metadata.ps1
./scripts/windows/export-datahub-mcp.ps1
./scripts/windows/generate-ops-dashboard.ps1
./scripts/windows/generate-scheduler-dashboard.ps1
docker compose -f docker-compose.yml -f docker-compose.three-node.yml --profile core --profile multi-node down --remove-orphans
```
