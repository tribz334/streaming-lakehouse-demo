# Real Streaming Lakehouse Stack

这个目录是论文系统的主要实现，使用 Docker Compose 组织真实技术栈组件。

> 当前实现包含论文一致的数据模型与单机三逻辑节点拓扑。三逻辑节点用于复现组件部署、分区副本、并行计算和故障演练，不等价于三台物理主机的性能数据。逐项边界见 `IMPLEMENTATION_AUDIT.md`。

## 技术栈映射

| 论文模块 | 本目录实现 |
| --- | --- |
| 业务库 | MySQL 8.4，开启 binlog/ROW/GTID |
| 维度主数据 | Flink CDC 3.6 将广告主、计划、单元和创意持续同步到 Paimon DIM 表；实时事实保留维度外键，展示层按需关联 |
| 埋点日志 | `event-generator-node-1` 写入 Kafka `ods_log` |
| Kafka 消息总线 | Apache Kafka 3.9.1 KRaft 单节点，6 分区、单副本 |
| Flink CDC | Flink CDC 3.6 YAML Pipeline；MySQL 全量快照后持续消费 binlog，实时维护 Paimon DIM/ODS |
| Paimon 湖仓 | Paimon Flink 2.2 connector 1.4.2，warehouse: `/warehouse/paimon`，Catalog 元数据持久化到 Hive Metastore |
| Flink 流批一体 | Flink 2.2.0 JobManager/TaskManager |
| 流批一体计算 | 实时与离线共用 Flink、业务主键和指标口径；实时热链路直写 StarRocks，离线明细与历史主题数据保存在 Paimon |
| 论文数据字典 | `01_model_tables.sql` 定义当前保留的 DWS/DM 核心表 |
| 订单生命周期 | Paimon `partial-update` 主键表 |
| OLAP 服务 | StarRocks FE/BE；单个 Java Flink 作业将 10 秒窗口结果直接写入 Primary Key 表，离线 ADS 使用内部快照 |
| BI 应用 | Superset 3.0.0，自动注册 StarRocks 数据库与四个 dataset |
| Schema Registry | Apicurio Registry 3.2.5，已注册 Kafka `ods_log-value` JSON schema |
| 运维观测 | Prometheus、Grafana、Loki、Alloy 已验证；容器日志支持按服务、节点和角色集中检索；`ops-dashboard/index.html` 本地看板已生成 |
| 元数据服务 | Hive Metastore 4.0.1，Derby demo 存储；作为 Paimon Catalog 元数据后端，宿主机映射端口 `19083` |
| 数据治理 | `scripts/windows/export-governance-metadata.ps1` 导出 DataHub 风格 URN、血缘、术语 JSON；`export-datahub-mcp.ps1` 导出 MCP-style JSONL |
| 调度编排 | `scripts/linux/*.sh` 容器/Linux 任务入口；`dolphinscheduler/workflows/ad-lakehouse-demo.yaml` 保存 DAG 模板 |

版本组合采用 Flink 2.2.0、Flink CDC 3.6.0-2.2、Paimon Flink 2.2 Bridge 1.4.2 和 Kafka Connector 5.0.0-2.2。Flink 通过 Paimon 官方推荐的 Hive 兼容 bundle 访问 HMS，CDC Pipeline 和 Flink SQL 使用同一个 `thrift://hive-metastore:9083` Catalog。

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

默认的 `docker-compose.yml` 在同一台宿主机上启动一套精简节点：

```text
node-1: MySQL + Kafka-node-1 + Flink JM/TM + event-generator-node-1
        + Hive Metastore
```

`ods_log` 固定为 6 分区、单副本。Kafka 不提供节点容灾，但仍支持 Flink
按 Partition 消费。需要同时启动单 FE/单 BE StarRocks 和完整监控栈时使用：

```powershell
./scripts/windows/start-multi-node.ps1 -WithOlap -WithOps
```

验证精简拓扑状态：

```powershell
./scripts/windows/verify-multi-node.ps1
```

该实验环境用于验证多节点拓扑、任务注册、并行计算 slot、数据采集节点协同和流式链路可运行性。由于所有容器仍部署在同一台物理主机上，它属于单机多 Docker 的逻辑多节点环境，不用于证明跨物理机网络开销或真实多机容灾能力。

也可以分步骤运行：

```powershell
./scripts/windows/download-flink-jars.ps1
docker compose up -d --build
./scripts/windows/init-flink-ddl.ps1
./scripts/windows/submit-cdc-pipeline.ps1
./scripts/windows/submit-streaming-jobs.ps1
```

> 从旧版 filesystem catalog 升级时，已有 Paimon 数据文件不会自动注册到 HMS。
> 本 demo 建议先停止旧作业并使用全新 Docker volume 重新初始化；生产环境应使用
> Paimon 的 catalog 迁移工具或逐表注册流程，不能直接删除现有 warehouse。

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

1. MySQL 初始化广告主、计划、创意、订单表和权威计费明细表 `ad_bill_detail`。
2. 事件生成器把广告行为和页面埋点写入 Kafka 总埋点流 `ods_log`；Flink 按 `log_type` 分流到 `dwd_ad_action_log`、`dwd_page_log`，解析或校验失败的数据进入 `dwd_dirty_log`。订单写入 MySQL `ad_order`，可计费点击写入 MySQL `ad_bill_detail`，不把订单或金额混入埋点日志。
3. Flink CDC 3.6 先对 MySQL 业务表执行一致性快照，再持续读取 binlog，将广告主、计划、单元和创意 Upsert 到 Paimon DIM，并将订单生命周期写入 `ods_ad_order`。Paimon 中已有的事件 ODS/DWD 表保留给离线快照与兼容性验证，不再承担实时热路径。
4. 单个 Java Flink 作业同时读取 Kafka `ods_log` 广告流，以及 MySQL `ad_order`、`ad_bill_detail` 两条 CDC 流。行为事实落入 Kafka `dwd_ad_action_log`；点击流与支付订单流分别映射为 `AdClickEvent` 和 `OrderDetail`，按论文命名的 `product_id + user_id` 分组后，由 `DwdOrderDetail` 通过 `connect + AttributionProcessFunction` 执行 30 分钟 LastClick：点击保存在 `ValueState adClickEvent`，订单保存在 `ListState orderDetailList` 并按事件时间等待 10 秒，由 `onTimer` 输出 Kafka `dwd_order_detail`。计费 CDC 先映射为 `AdBillDetail`，再由 `DwdAdBillDetail` 转换为公共事实；归因订单、行为和计费事实只在 DWS 输入处 `union`，完成 10 秒窗口聚合，并将带有 `advertiser_id`、`campaign_id`、`unit_id`、`creative_id` 的结果直接写入 StarRocks `realtime_ad_attribution_metrics_10s`。广告主、计划、单元和创意名称等展示属性由查询侧关联 DIM 获取。直播、短视频和电商属于内循环；站外投放属于外循环，只进入广告行为与计费，不进入内循环 GMV。
5. Flink batch SQL 计算 ADS：
   - `ads_advertiser_retention_di`：广告主留存。
   - `ads_order_attribution_detail_di`：订单级 30 天 LastClick 明细，互斥区分 30 分钟直接归因、1/3/7/30 日间接归因和自然订单。
   - `ads_attribution_summary_di`：按日期、广告主、活动和归因窗口汇总订单量、GMV 与点击消耗。
   - `ads_creative_offline_di`：创意粒度离线 BI 数据集，聚合 DWS 事实并补齐广告主、计划、单元和创意维度。
   - `ads_fraud_signal_di`：demo 流量规模下的高点击、异常 CTR、集中用户点击规则信号。
6. Superset 连接 StarRocks，自动注册业务 dataset，并分别生成实时核心指标、离线核心指标、留存、广告归因和广告反作弊看板。广告归因看板独立展示 30 分钟、1/3/7/30 日与自然订单的占比、趋势和订单级下钻；广告反作弊看板独立展示可疑用户、点击、消耗、风险评分与广告主级下钻。离线核心指标看板读取每日封存结果，命名和公式与实时大盘一致，默认查看近 14 天并支持任意统计时间范围；所有卡片与日趋势都随所选窗口重新聚合。创意粒度 ADS 仍作为独立明细数据集保留。
8. `export-governance-metadata.ps1` 导出 DataHub 风格离线元数据，覆盖 Kafka、Paimon、StarRocks 资产和核心血缘；`export-datahub-mcp.ps1` 额外导出 `datahub/mcp/metadata_change_proposals.jsonl`。
9. `register-schemas.ps1` 向 Apicurio 注册 `ad-demo/ods_log-value` JSON schema。
10. `generate-ops-dashboard.ps1` 汇总 Flink、Prometheus、StarRocks、治理元数据、调度状态和运行时 fallback，生成本地 HTML 运维看板。
11. `bootstrap-dolphinscheduler.ps1` 通过 DolphinScheduler OpenAPI 注册三条业务 DAG：每日 02:00 离线湖仓刷新、手动实时作业启停，以及每日 00:05 实时累计切日。切日 DAG 先将所有已结束日期汇总封存到 `ad_ads.realtime_ad_metrics_daily`，成功后删除实时主表中今天以前的 10 秒窗口，再校验新业务日视图并写执行回执；实时大盘白天仍由 Flink 10 秒窗口持续更新。

## 实时指标服务

实时查询底表是 StarRocks Primary Key 表。广告曝光/点击来自 Kafka，支付订单与权威消耗分别来自 MySQL `ad_order`、`ad_bill_detail` CDC；Java Flink 作业以窗口时间、广告主、计划、单元和创意构成联合主键，聚合完成后通过 JDBC 批量 Sink 直接写表。核心大盘卡片查询 `v_realtime_ad_metrics_today`，展示当天零点至当前时刻的累计值；广告消耗为内循环与外循环总消耗，内循环 GMV 只汇总内循环场景，`ROAS = 内循环GMV / 内循环消耗`。DolphinScheduler 每日 00:05 先封存已结束日期，再删除今天以前的实时 10 秒窗口并验证切日，但不负责白天的实时累加。当前时效主要由 10 秒事件时间窗口、5 秒 Watermark、MySQL binlog 传播和 1 秒写出批次决定。

## 常用命令

```powershell
docker compose ps
docker compose logs -f flink-jobmanager
docker compose logs -f event-generator-node-1
docker compose --profile olap exec -T starrocks bash -lc "mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW CATALOGS;'"
./scripts/windows/run-ads-batches.ps1
./scripts/windows/sync-starrocks-olap.ps1
./scripts/windows/register-schemas.ps1
./scripts/windows/export-governance-metadata.ps1
./scripts/windows/export-datahub-mcp.ps1
./scripts/windows/generate-ops-dashboard.ps1
./scripts/windows/generate-scheduler-dashboard.ps1
docker compose down --remove-orphans
```
