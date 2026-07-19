package cn.edu.ustc.lakehouse.realtime;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public final class RealtimeAdMetricJob {
    private static final OutputTag<String> DIRTY_EVENTS =
            new OutputTag<>("dirty-ad-events", Types.STRING);

    private RealtimeAdMetricJob() {}

    public static void main(String[] args) {
        RealtimeJobConfig config = RealtimeJobConfig.fromArgs(args);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(config.getParallelism());
        environment.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(
                environment,
                EnvironmentSettings.newInstance().inStreamingMode().build());
        tableEnvironment.getConfig().set("table.exec.sink.upsert-materialize", "NONE");

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

        SingleOutputStreamOperator<AdEvent> parsedEvents = rawEvents
                .process(new AdEventParseProcessFunction(DIRTY_EVENTS))
                .name("parse and validate ad events");
        parsedEvents.getSideOutput(DIRTY_EVENTS)
                .print("dirty-ad-event")
                .name("dirty event log");

        DataStream<RealtimeMetric> metrics = parsedEvents
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AdEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, previousTimestamp) -> event.getEventTimeMillis()))
                .keyBy(AdEvent::toMetricKey)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new MetricAggregateFunction(), new MetricWindowFunction())
                .name("10-second metric aggregation");

        DataStream<RealtimeMetric> enrichedMetrics = AsyncDataStream.unorderedWait(
                        metrics,
                        new AdvertiserDimAsyncFunction(config),
                        3,
                        TimeUnit.SECONDS,
                        100)
                .name("async advertiser dimension lookup");

        StarRocksUtil.sink(tableEnvironment, enrichedMetrics, config);
    }

    private static OffsetsInitializer startingOffsets(String startupMode) {
        return "latest".equalsIgnoreCase(startupMode)
                ? OffsetsInitializer.latest()
                : OffsetsInitializer.earliest();
    }
}
