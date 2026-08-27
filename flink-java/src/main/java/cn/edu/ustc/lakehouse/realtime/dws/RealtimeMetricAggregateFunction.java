package cn.edu.ustc.lakehouse.realtime.dws;

import cn.edu.ustc.lakehouse.realtime.model.MetricAccumulator;
import cn.edu.ustc.lakehouse.realtime.model.MetricDelta;
import cn.edu.ustc.lakehouse.realtime.model.RealtimeMetric;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

/** Incremental platform aggregation for the configured event-time window. */
public final class RealtimeMetricAggregateFunction {
    private RealtimeMetricAggregateFunction() {}

    public static final class Incremental implements
            AggregateFunction<MetricDelta, MetricAccumulator, MetricAccumulator> {
        @Override public MetricAccumulator createAccumulator() { return new MetricAccumulator(); }

        @Override
        public MetricAccumulator add(MetricDelta value, MetricAccumulator acc) {
            acc.dt = value.dt;
            acc.deliveryCount += value.deliveryCount;
            acc.impressionCount += value.impressionCount;
            acc.clickCount += value.clickCount;
            acc.conversionCount += value.conversionCount;
            acc.cost += value.cost;
            acc.closedCost += value.closedCost;
            acc.payOrderCount += value.payOrderCount;
            acc.refundOrderCount += value.refundOrderCount;
            acc.payOrderGmv += value.payOrderGmv;
            acc.refundOrderGmv += value.refundOrderGmv;
            if (value.adType == 1) acc.shortVideoPayOrderGmv += value.payOrderGmv;
            else if (value.adType == 2) acc.livePayOrderGmv += value.payOrderGmv;
            else if (value.adType == 3) acc.imageTextPayOrderGmv += value.payOrderGmv;
            else if (value.adType == 4) acc.otherAdTypePayOrderGmv += value.payOrderGmv;
            if (value.placementType == 1) acc.searchPayOrderGmv += value.payOrderGmv;
            else if (value.placementType == 2) acc.splashPayOrderGmv += value.payOrderGmv;
            else if (value.placementType == 3) acc.feedPayOrderGmv += value.payOrderGmv;
            else if (value.placementType == 4) acc.rewardedPayOrderGmv += value.payOrderGmv;
            else if (value.placementType == 5) acc.bannerPayOrderGmv += value.payOrderGmv;
            else if (value.placementType == 6) acc.otherPlacementPayOrderGmv += value.payOrderGmv;
            return acc;
        }

        @Override public MetricAccumulator getResult(MetricAccumulator acc) { return acc; }

        @Override
        public MetricAccumulator merge(MetricAccumulator left, MetricAccumulator right) {
            MetricDelta value = new MetricDelta();
            value.dt = right.dt;
            value.deliveryCount = right.deliveryCount;
            value.impressionCount = right.impressionCount;
            value.clickCount = right.clickCount;
            value.conversionCount = right.conversionCount;
            value.cost = right.cost;
            value.closedCost = right.closedCost;
            value.payOrderCount = right.payOrderCount;
            value.refundOrderCount = right.refundOrderCount;
            value.payOrderGmv = right.payOrderGmv;
            value.refundOrderGmv = right.refundOrderGmv;
            add(value, left);
            left.shortVideoPayOrderGmv += right.shortVideoPayOrderGmv;
            left.livePayOrderGmv += right.livePayOrderGmv;
            left.imageTextPayOrderGmv += right.imageTextPayOrderGmv;
            left.otherAdTypePayOrderGmv += right.otherAdTypePayOrderGmv;
            left.searchPayOrderGmv += right.searchPayOrderGmv;
            left.splashPayOrderGmv += right.splashPayOrderGmv;
            left.feedPayOrderGmv += right.feedPayOrderGmv;
            left.rewardedPayOrderGmv += right.rewardedPayOrderGmv;
            left.bannerPayOrderGmv += right.bannerPayOrderGmv;
            left.otherPlacementPayOrderGmv += right.otherPlacementPayOrderGmv;
            return left;
        }
    }

    public static final class WindowResult extends
            ProcessWindowFunction<MetricAccumulator, RealtimeMetric, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<MetricAccumulator> values,
                            Collector<RealtimeMetric> out) {
            MetricAccumulator acc = values.iterator().next();
            RealtimeMetric result = new RealtimeMetric();
            result.windowStart = Instant.ofEpochMilli(context.window().getStart());
            result.windowEnd = Instant.ofEpochMilli(context.window().getEnd());
            result.deliveryCount = acc.deliveryCount;
            result.impressionCount = acc.impressionCount;
            result.clickCount = acc.clickCount;
            result.conversionCount = acc.conversionCount;
            result.cost = acc.cost;
            result.closedCost = acc.closedCost;
            result.payOrderCount = acc.payOrderCount;
            result.refundOrderCount = acc.refundOrderCount;
            result.payOrderGmv = acc.payOrderGmv;
            result.refundOrderGmv = acc.refundOrderGmv;
            result.shortVideoPayOrderGmv = acc.shortVideoPayOrderGmv;
            result.livePayOrderGmv = acc.livePayOrderGmv;
            result.imageTextPayOrderGmv = acc.imageTextPayOrderGmv;
            result.otherAdTypePayOrderGmv = acc.otherAdTypePayOrderGmv;
            result.searchPayOrderGmv = acc.searchPayOrderGmv;
            result.splashPayOrderGmv = acc.splashPayOrderGmv;
            result.feedPayOrderGmv = acc.feedPayOrderGmv;
            result.rewardedPayOrderGmv = acc.rewardedPayOrderGmv;
            result.bannerPayOrderGmv = acc.bannerPayOrderGmv;
            result.otherPlacementPayOrderGmv = acc.otherPlacementPayOrderGmv;
            result.dt = acc.dt;
            out.collect(result);
        }
    }
}
