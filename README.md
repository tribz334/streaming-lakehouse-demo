# Real Streaming Lakehouse Stack

这个目录是论文系统的主要实现，使用 Docker Compose 组织真实技术栈组件。

> 当前实现定位为单机可运行演示版，不是论文三节点生产级集群的 100% 等价复刻。逐项差距见 `IMPLEMENTATION_AUDIT.md`。

## 技术栈映射

| 论文模块 | 本目录实现 |
| --- | --- |
| 业务库 | MySQL 8.4，开启 binlog/ROW/GTID |
| 埋点日志 | `event-generator` 写入 Kafka `ods_log` |
| Kafka 消息总线 | Apache Kafka 3.9.1 KRaft 单节点 |
| Flink CDC | MySQL binlog 已开启，CDC YAML 配置保留；当前可运行链路用 Flink JDBC 4.0 装载维表 |
| Paimon 湖仓 | Paimon Flink 2.0 connector，warehouse: `/warehouse/paimon` |
| Flink 流批一体 | Flink 2.0.2 JobManager/TaskManager |
| ODS/DIM/DWD/DWS/ADS | `flink/sql/*.sql` 分层建表与作业 |
| 订单生命周期 | Paimon `partial-update` 主键表 |
| OLAP 服务 | StarRocks FE/BE 单节点，Paimon external catalog + 内部 OLAP 快照视图 |
| BI 应用 | Superset 3.0.0，自动注册 StarRocks 数据库与四个 dataset |
| Schema Registry | Apicurio Registry 3.2.5，已注册 Kafka `ods_log-value` JSON schema |
| 运维观测 | Prometheus 已验证；`ops-dashboard/index.html` 本地看板已生成；Grafana/Loki profile 与 dashboard 配置已预留 |
| 元数据服务 | Hive Metastore 4.0.1，Derby demo 存储，9083 已验证 |
| 数据治理 | `scripts/windows/export-governance-metadata.ps1` 导出 DataHub 风格 URN、血缘、术语 JSON；`export-datahub-mcp.ps1` 导出 MCP-style JSONL |
| 调度编排 | `scripts/windows/run-demo-workflow.ps1` 本地 runner；`scripts/linux/*.sh` 容器/Linux 任务入口；`dolphinscheduler/workflows/ad-lakehouse-demo.yaml` 保存 DAG 模板 |

说明：论文写的是 Flink 2.0 + Paimon 1.0。Maven 当前可用的 Flink 2.0 专用 Paimon bridge 从 1.1.x 起提供，因此默认使用 `paimon-flink-2.0:1.1.1`。如果严格使用 Paimon 1.0，可把 Flink 降到 1.20 并改用 `paimon-flink-1.20`。

## 脚本分层

```text
scripts/
├─ windows/  # Windows 宿主机的 PowerShell 启动、运维和数据同步脚本
└─ linux/    # Linux 宿主机或调度 worker 使用的 Bash 任务入口
```

Windows 用户从 `scripts/windows/start-demo.ps1` 启动。Linux 任务使用 `init-flink-ddl.sh`、`submit-streaming-jobs.sh`、`run-ads-batches.sh`、`sync-starrocks.sh` 和 `verify-stack.sh`。其中 StarRocks 同步暂时通过跨平台 PowerShell Core 复用已验证的解析实现。

## 启动步骤

先启动 Docker Desktop，然后在本目录运行：

```powershell
./scripts/windows/start-demo.ps1
```

如果当前已经有 Flink 流式作业在运行，避免重复提交可使用：

```powershell
./scripts/windows/start-demo.ps1 -SkipStreamingSubmit
```

也可以分步骤运行：

```powershell
./scripts/windows/start-core.ps1
./scripts/windows/init-flink-ddl.ps1
./scripts/windows/submit-streaming-jobs.ps1
```

`start-core.ps1` 会先把 Paimon、Kafka connector、MySQL CDC connector、MySQL JDBC 与 Hadoop shaded jar 下载到 `flink/lib/`，再构建 Flink 镜像。

等 Kafka 事件流和 Flink 作业运行一会儿后，执行：

```powershell
./scripts/windows/run-ads-batches.ps1
./scripts/windows/query-paimon-counts.ps1
./scripts/windows/verify-stack.ps1
```

OLAP / BI 层：

```powershell
docker compose --profile olap up -d starrocks starrocks-be
./scripts/windows/init-starrocks.ps1
./scripts/windows/sync-starrocks-olap.ps1
./scripts/windows/start-bi.ps1
```

访问入口：

- Flink UI: http://127.0.0.1:8082
- StarRocks FE: http://127.0.0.1:8030
- Superset: http://127.0.0.1:8088
- Apicurio Registry: http://127.0.0.1:8081/apis/registry/v3/system/info
- Prometheus: http://127.0.0.1:19090
- Hive Metastore Thrift: 127.0.0.1:9083
- Local Ops Dashboard: `ops-dashboard/index.html`
- Local Scheduler Dashboard: `dolphinscheduler/dashboard/index.html`
- DolphinScheduler: http://127.0.0.1:12345/dolphinscheduler/ui/ (`admin` / `dolphinscheduler123`)
- Grafana: http://127.0.0.1:13000

Superset 默认账号：

```text
admin / admin
```

## 数据链路

1. MySQL 初始化广告主、计划、创意、订单表。
2. 事件生成器持续写 Kafka `ods_log`，订单事件同时写 MySQL `ad_order`，保留 CDC 输入条件。
3. 当前可运行 Flink SQL 读取 Kafka 和 MySQL JDBC 维表，长期写入 Paimon ODS/DIM/DWD；严格 CDC 版本见 `flink-cdc/mysql-to-paimon.yaml`。
4. DWS 同时提供 `dws_ad_metric_stream_10s` 实时窗口指标和 `dws_ad_metric_10s` 批量刷新指标；单机演示环境在运行批任务前会暂停流任务以释放 slot。
5. Flink batch SQL 计算 ADS：
   - `ads_advertiser_retention_di`：广告主留存。
   - `ads_attribution_summary_di`：7 日窗口最后点击归因。
   - `ads_fraud_signal_di`：demo 流量规模下的高点击、异常 CTR、集中用户点击规则信号。
   - `ads_data_quality_result_di`：8 条 ODS/DWD/DWS 质量规则明细。
   - `ads_data_quality_summary_di`：通过数、失败数、质量分和整体状态。
6. StarRocks 创建 Paimon external catalog。当前本地可用 StarRocks 3.1 可以识别 catalog，但直读 Paimon 1.1.1 snapshot 会遇到 reader 兼容问题，因此 `sync-starrocks-olap.ps1` 会把 DWS/ADS 同步到 StarRocks 内部快照表。
7. Superset 连接 StarRocks，自动注册业务指标、留存、归因、反作弊、质量明细和质量总分 6 个 dataset。
8. `export-governance-metadata.ps1` 导出 DataHub 风格离线元数据，覆盖 Kafka、Paimon、StarRocks 资产和核心血缘；`export-datahub-mcp.ps1` 额外导出 `datahub/mcp/metadata_change_proposals.jsonl`。
9. `register-schemas.ps1` 向 Apicurio 注册 `ad-demo/ods_log-value` JSON schema。
10. `generate-ops-dashboard.ps1` 汇总 Flink、Prometheus、StarRocks、治理元数据、调度状态和运行时 fallback，生成本地 HTML 运维看板。
11. `run-demo-workflow.ps1` 会先停止当前 Flink 作业，刷新 DWS/ADS 及数据质量结果，同步 StarRocks，再恢复 ODS、DWD 和实时 DWS 三条长期流任务。
12. `bootstrap-dolphinscheduler.ps1` 通过 DolphinScheduler OpenAPI 自动创建项目和 `lakehouse_component_smoke_test` DAG，依次在 Linux worker 中验证 Flink、Prometheus 和 StarRocks，并将真实执行回执写入 `dolphinscheduler/runs/dolphinscheduler-execution.txt`。完整 DWS/ADS 刷新仍由本机 PowerShell 工作流执行，后续可继续迁移为容器原生任务。

## Paimon 湖仓实验与数据质量

`run-paimon-experiments.ps1` 在独立实验表上验证 Schema Evolution、Snapshot、Time Travel 和 Compaction，不修改业务表结构。报告输出到 `paimon/experiments/latest-report.json`。

数据质量包含 8 条规则：ODS/DWD 非空、层间数量偏差、必填维度完整性、金额非负、CTR/CVR 范围和 ROI 非负。质量分按通过规则比例计算。

## 常用命令

```powershell
docker compose --profile core ps
docker compose --profile core logs -f flink-jobmanager
docker compose --profile core logs -f event-generator
docker compose --profile olap exec -T starrocks bash -lc "mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW CATALOGS;'"
./scripts/windows/run-demo-workflow.ps1
./scripts/windows/run-ads-batches.ps1
./scripts/windows/run-paimon-experiments.ps1
./scripts/windows/sync-starrocks-olap.ps1
./scripts/windows/register-schemas.ps1
./scripts/windows/export-governance-metadata.ps1
./scripts/windows/export-datahub-mcp.ps1
./scripts/windows/generate-ops-dashboard.ps1
./scripts/windows/generate-scheduler-dashboard.ps1
./scripts/windows/stop-stack.ps1
```
