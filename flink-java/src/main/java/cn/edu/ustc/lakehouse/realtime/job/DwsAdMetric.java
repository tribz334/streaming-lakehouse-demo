package cn.edu.ustc.lakehouse.realtime.job;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.dwd.AttributionProcessFunction;
import cn.edu.ustc.lakehouse.realtime.dwd.DwdAdBill;
import cn.edu.ustc.lakehouse.realtime.dwd.DimBroadcastEnrichment;
import cn.edu.ustc.lakehouse.realtime.dwd.DwdLogProcessFunction;
import cn.edu.ustc.lakehouse.realtime.dwd.DwdOrderDetail;
import cn.edu.ustc.lakehouse.realtime.dws.DwsMetricAggregation;
import cn.edu.ustc.lakehouse.realtime.model.AdClickEvent;
import cn.edu.ustc.lakehouse.realtime.model.AdEvent;
import cn.edu.ustc.lakehouse.realtime.model.CreativeDimChange;
import cn.edu.ustc.lakehouse.realtime.model.OrderDetail;
import cn.edu.ustc.lakehouse.realtime.model.RealtimeMetric;
import cn.edu.ustc.lakehouse.realtime.sink.KafkaDwdUtil;
import cn.edu.ustc.lakehouse.realtime.sink.StarRocksUtil;
import cn.edu.ustc.lakehouse.realtime.source.AdBillCdcSource;
import cn.edu.ustc.lakehouse.realtime.source.CreativeDimCdcSource;
import cn.edu.ustc.lakehouse.realtime.source.OrderCdcSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/** Paper-style DWS main program for realtime advertising attribution metrics. */
public final class DwsAdMetric {
    private static final OutputTag<String> DIRTY_EVENTS =
            new OutputTag<>("dirty-ad-events", Types.STRING);
    private static final OutputTag<String> DIM_DIRTY_EVENTS =
            new OutputTag<>("dirty-dim-enrichment", Types.STRING);

    private DwsAdMetric() {}

    public static void main(String[] args) throws Exception {
        RealtimeJobConfig config = RealtimeJobConfig.fromArgs(args);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(config.getParallelism());
        environment.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(
                environment,
                EnvironmentSettings.newInstance().inStreamingMode().build());
        tableEnvironment.getConfig().set("table.exec.sink.upsert-materialize", "NONE");
        
        // =======================
        // Source 层：从 Kafka 读取原始广告事件数据
        // =======================
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getSourceTopic())
                .setGroupId(config.getConsumerGroup())
                .setStartingOffsets(startingOffsets(config.getStartupMode()))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawEvents = environment.fromSource(
                (Source<String, ?, ?>) source,
                WatermarkStrategy.noWatermarks(),
                "Kafka ods_log source");
        
        // =======================
        // DWD层：标准化后的广告事件对象 AdEvents，同时通过侧输出流分离 DirtyLogs
        // =======================
        SingleOutputStreamOperator<AdEvent> parsedEvents = rawEvents
                .process(new DwdLogProcessFunction(DIRTY_EVENTS))
                .name("DWD log parse, validate and dirty-data split");
        KafkaDwdUtil.sinkDwdDirtyLog(parsedEvents.getSideOutput(DIRTY_EVENTS), config);

        BroadcastStream<CreativeDimChange> creativeDimensions = CreativeDimCdcSource
                .create(tableEnvironment, config)
                .broadcast(DimBroadcastEnrichment.DIM_STATE);

        SingleOutputStreamOperator<AdEvent> enrichedAdEvents = parsedEvents
                .connect(creativeDimensions)
                .process(new DimBroadcastEnrichment(DIM_DIRTY_EVENTS))
                .name("DWD enrich creative hierarchy from broadcast DIM CDC");
        KafkaDwdUtil.sinkDwdDirtyLog(
                enrichedAdEvents.getSideOutput(DIM_DIRTY_EVENTS), config);

        DataStream<AdEvent> dwdAdActionEvents = enrichedAdEvents
                .filter(event -> !event.isPaidOrder())
                .name("DWD dwd_ad_action_log advertising action facts");
        KafkaDwdUtil.sinkDwdAdActionLog(dwdAdActionEvents, config);

        DataStream<AdClickEvent> adClickEvents = dwdAdActionEvents
                .filter(AdEvent::isClick)
                .map(AdClickEvent::from)
                .returns(AdClickEvent.class)
                .name("map DWD click facts to AdClickEvent")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(5))
                                .withIdleness(Duration.ofSeconds(10))
                                .withTimestampAssigner(
                                        (click, previousTimestamp) -> click.getClickTimeMillis()));

        DataStream<OrderDetail> orderEvents = OrderCdcSource
                .createOrderDetails(tableEnvironment, config)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(5))
                                .withIdleness(Duration.ofSeconds(10))
                                .withTimestampAssigner(
                                        (order, previousTimestamp) -> order.getCreateTimeMillis()));

        SingleOutputStreamOperator<OrderDetail> dwdOrderDetail =
                DwdOrderDetail.build(adClickEvents, orderEvents);
        KafkaDwdUtil.sinkDwdOrderDetail(dwdOrderDetail, config);

        dwdOrderDetail.getSideOutput(AttributionProcessFunction.LATE_CLICKS)
                .print("dirty-late-ad-click")
                .name("late click dirty-data log");

        DataStream<AdEvent> attributedOrderEvents = dwdOrderDetail
                .map(OrderDetail::toAdEvent)
                .returns(AdEvent.class)
                .name("map attributed OrderDetail to DWD fact event");

        DataStream<AdEvent> billEvents = DwdAdBill.build(
                AdBillCdcSource.create(tableEnvironment, config));

        DataStream<AdEvent> dwsInputEvents = dwdAdActionEvents
                .union(attributedOrderEvents, billEvents)
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<AdEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                            .withIdleness(Duration.ofSeconds(30))
                            .withTimestampAssigner((event, previousTimestamp) -> event.getEventTimeMillis()));

        DataStream<RealtimeMetric> metrics = DwsMetricAggregation.build(dwsInputEvents);

        StarRocksUtil.sink(metrics, config);
        environment.execute("DwsAdMetric");
    }

    private static OffsetsInitializer startingOffsets(String startupMode) {
        return "latest".equalsIgnoreCase(startupMode)
                ? OffsetsInitializer.latest()
                : OffsetsInitializer.earliest();
    }
}
