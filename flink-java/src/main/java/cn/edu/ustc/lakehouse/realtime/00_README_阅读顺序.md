# Flink 实时归因代码阅读顺序

> Java 的 `public class` 文件名必须和类名一致，类名也不能以数字开头。
> 因此保留论文和业务类名，通过本索引提供 `01、02、03...` 阅读顺序。

## 目录分层

```text
realtime/
├─ job/       Flink 作业入口与完整 DAG 编排
├─ config/    运行参数和外部系统配置
├─ model/     各层共享的数据模型
├─ source/    MySQL CDC 数据源
├─ dwd/       数据清洗、分流和明细归因
├─ dws/       窗口聚合与指标计算
└─ sink/      Kafka 与 StarRocks 输出
```

## 第一阶段：从作业入口看完整数据流

### 01 `job/DwsAdMetric.java`

主作业入口。先阅读这个文件，掌握 Kafka、MySQL CDC、DWD 分流、订单归因、DWS 聚合和 StarRocks Sink 的完整 DAG。

### 02 `config/RealtimeJobConfig.java`

作业配置。集中管理 Kafka Topic、MySQL CDC、StarRocks、并行度和启动模式。

## 第二阶段：ODS 总埋点流进入 DWD

### 03 `dwd/DwdLogProcessFunction.java`

读取 Kafka `ods_log` 字符串，解析 JSON，并将数据分成广告、页面和脏数据三路。

### 04 `model/AdEvent.java`

广告行为及后续 DWS 统一事实模型。承载曝光、点击、计费和归因订单的公共指标字段。

### 05 `model/PageLogEvent.java`

页面行为模型，承载页面进入、离开、停留时间、设备和来源信息。

### 06 `sink/KafkaDwdUtil.java`

将四类 DWD 结果写入 Kafka：`dwd_ad_action_log`、`dwd_page_log`、`dwd_dirty_log`、`dwd_order_detail`。

## 第三阶段：点击和订单双流归因

### 07 `model/AdClickEvent.java`

从广告行为流中提取的点击模型，只保留 LastClick 归因需要的字段。

### 08 `source/OrderCdcSource.java`

读取 MySQL `order` CDC，筛选支付成功的商品订单并执行订单生命周期去重。订单只携带用户、商品和交易金额，广告归属由点击流确定。

### 09 `model/OrderDetail.java`

订单明细和归因结果模型，记录 GMV、最佳点击、广告维度和归因状态。

### 10 `dwd/AttributionKey.java`

论文对应的联合归因键：`product_id + user_id`。

### 11 `dwd/DwdOrderDetail.java`

将点击流和订单流分别 `keyBy` 后执行 `connect`，是订单归因流程的组装入口。

### 12 `dwd/AttributionProcessFunction.java`

核心双输入归因函数：`processElement1` 处理点击，`processElement2` 处理订单，`onTimer` 完成10秒等待和30分钟状态清理。

## 第四阶段：权威计费和 DWS 10 秒聚合

### 13 `source/AdBillCdcSource.java`

读取 MySQL `ad_bill` CDC，产出 `AdBill` 计费事实；随后由 `DwdAdBill` 转成 DWS 公共事实格式，为实时指标提供权威广告消耗。

### 14 `dws/MetricKey.java`

DWS 聚合键，包含广告主、计划、单元、创意、媒体和商业场景。

### 15 `dws/DwsMetricAggregation.java`

完整封装10秒窗口、增量累加器和窗口输出，累计曝光、点击、订单、消耗、归因 GMV 和自然 GMV。

### 16 `model/RealtimeMetric.java`

DWS 10秒窗口最终指标模型，计算 CTR、CVR 和 ROI。

## 第五阶段：结果输出

### 17 `sink/StarRocksUtil.java`

将带有广告主、计划、单元和创意外键的10秒实时指标批量写入 StarRocks。维度主数据由独立的 Flink CDC Pipeline 同步到 Paimon，展示层按需关联 DIM，不阻塞实时聚合链路。

## 最短核心阅读路径

如果只想理解论文 4.3.4 的双流归因，按以下顺序阅读：

```text
01 DwsAdMetric
07 AdClickEvent
08 OrderCdcSource
09 OrderDetail
10 AttributionKey
11 DwdOrderDetail
12 AttributionProcessFunction
```

如果想理解完整实时汇总链路，按照 `01 -> 17` 顺序阅读。
