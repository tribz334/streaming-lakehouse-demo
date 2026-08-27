package cn.edu.ustc.lakehouse.realtime.job;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.dws.RealtimeMetricAggregateFunction;
import cn.edu.ustc.lakehouse.realtime.model.MetricDelta;
import cn.edu.ustc.lakehouse.realtime.model.RealtimeMetric;
import cn.edu.ustc.lakehouse.realtime.util.FlussTableUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/** DWD Fluss streams -> signed deltas -> configurable event-time ADS snapshot. */
public final class RealtimeAdMetricJob {
    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");

    private RealtimeAdMetricJob() {}

    public static void main(String[] args) {
        RealtimeJobConfig config = RealtimeJobConfig.fromArgs(args);
        FlussTableUtil.Context context = FlussTableUtil.createContext(config);
        context.tableEnv().getConfig().set("pipeline.name", "fluss-realtime-metric-datastream-10s");
        String database = config.flussDatabase();

        Table eventTable = context.tableEnv().sqlQuery(
                "SELECT event_type,placement_type,ad_type,ts,dt FROM fluss." + database
                        + ".dwd_ad_event_di" + FlussTableUtil.scanHint(config));
        DataStream<MetricDelta> eventMetrics = withWatermarks(
                context.tableEnv().toChangelogStream(eventTable)
                        .map(RealtimeAdMetricJob::fromAdEvent).returns(MetricDelta.class), config);

        Table billTable = context.tableEnv().sqlQuery(
                "SELECT cost,is_closed,placement_type,ad_type,ts,dt FROM fluss." + database
                        + ".dwd_ad_bill_di" + FlussTableUtil.scanHint(config));
        DataStream<MetricDelta> billMetrics = withWatermarks(
                context.tableEnv().toChangelogStream(billTable)
                        .map(RealtimeAdMetricJob::fromBill).returns(MetricDelta.class), config);

        Table orderTable = context.tableEnv().sqlQuery(
                "SELECT order_type,order_gmv,placement_type,ad_type,order_time,dt FROM fluss."
                        + database + ".dwd_ad_order_event_di" + FlussTableUtil.scanHint(config));
        DataStream<MetricDelta> orderMetrics = withWatermarks(
                context.tableEnv().toChangelogStream(orderTable)
                        .map(RealtimeAdMetricJob::fromOrder).returns(MetricDelta.class), config);

        DataStream<RealtimeMetric> result = eventMetrics.union(billMetrics, orderMetrics)
                .keyBy(ignored -> "platform")
                .window(TumblingEventTimeWindows.of(
                        Time.seconds(config.realtimeMetricWindowSeconds())))
                .aggregate(new RealtimeMetricAggregateFunction.Incremental(),
                        new RealtimeMetricAggregateFunction.WindowResult())
                .name("platform-realtime-metric-window");

        context.tableEnv().createTemporaryView("realtime_metric_window", result);
        context.tableEnv().executeSql("INSERT INTO fluss." + database + ".ads_realtime_metric_10s "
                + "SELECT windowStart,windowEnd,deliveryCount,impressionCount,clickCount,conversionCount,"
                + "cost,closedCost,payOrderCount,refundOrderCount,payOrderGmv,refundOrderGmv,"
                + "shortVideoPayOrderGmv,livePayOrderGmv,imageTextPayOrderGmv,otherAdTypePayOrderGmv,"
                + "searchPayOrderGmv,splashPayOrderGmv,feedPayOrderGmv,rewardedPayOrderGmv,"
                + "bannerPayOrderGmv,otherPlacementPayOrderGmv,dt FROM realtime_metric_window");
    }

    private static DataStream<MetricDelta> withWatermarks(
            DataStream<MetricDelta> stream, RealtimeJobConfig config) {
        return stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<MetricDelta>forBoundedOutOfOrderness(config.outOfOrderness())
                        .withIdleness(config.sourceIdleness())
                        .withTimestampAssigner((metric, previous) -> metric.eventTimeMillis));
    }

    private static MetricDelta fromAdEvent(Row row) {
        int sign = sign(row.getKind());
        MetricDelta metric = base(row, 3, 4, sign);
        metric.placementType = nullableInt(row, 1);
        metric.adType = nullableInt(row, 2);
        String type = string(row, 0);
        if ("delivery".equals(type)) metric.deliveryCount = sign;
        else if ("impression".equals(type)) metric.impressionCount = sign;
        else if ("click".equals(type)) metric.clickCount = sign;
        else if ("conversion".equals(type)) metric.conversionCount = sign;
        return metric;
    }

    private static MetricDelta fromBill(Row row) {
        int sign = sign(row.getKind());
        MetricDelta metric = base(row, 4, 5, sign);
        metric.placementType = nullableInt(row, 2);
        metric.adType = nullableInt(row, 3);
        metric.cost = number(row, 0).longValue() * sign;
        if (number(row, 1).intValue() == 1) metric.closedCost = metric.cost;
        return metric;
    }

    private static MetricDelta fromOrder(Row row) {
        int sign = sign(row.getKind());
        MetricDelta metric = new MetricDelta();
        metric.placementType = nullableInt(row, 2);
        metric.adType = nullableInt(row, 3);
        metric.eventTimeMillis = timestampMillis(row.getField(4));
        metric.dt = string(row, 5);
        long amount = number(row, 1).longValue() * sign;
        if ("PAY".equals(string(row, 0))) {
            metric.payOrderCount = sign;
            metric.payOrderGmv = amount;
        } else if ("REFUND".equals(string(row, 0))) {
            metric.refundOrderCount = sign;
            metric.refundOrderGmv = amount;
        }
        return metric;
    }

    private static MetricDelta base(Row row, int timestampPosition, int dtPosition, int sign) {
        MetricDelta metric = new MetricDelta();
        metric.eventTimeMillis = number(row, timestampPosition).longValue();
        metric.dt = string(row, dtPosition);
        return metric;
    }

    private static int sign(RowKind kind) {
        return kind == RowKind.UPDATE_BEFORE || kind == RowKind.DELETE ? -1 : 1;
    }

    private static int nullableInt(Row row, int position) {
        Object value = row.getField(position);
        return value instanceof Number number ? number.intValue() : 0;
    }

    private static Number number(Row row, int position) {
        Object value = row.getField(position);
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("Expected numeric field at position " + position + ": " + value);
        }
        return number;
    }

    private static String string(Row row, int position) {
        Object value = row.getField(position);
        return value == null ? null : value.toString();
    }

    private static long timestampMillis(Object value) {
        if (value instanceof Instant instant) return instant.toEpochMilli();
        if (value instanceof LocalDateTime dateTime) return dateTime.atZone(ZONE).toInstant().toEpochMilli();
        if (value instanceof java.sql.Timestamp timestamp) return timestamp.toInstant().toEpochMilli();
        throw new IllegalArgumentException("Unsupported timestamp value: " + value);
    }
}
