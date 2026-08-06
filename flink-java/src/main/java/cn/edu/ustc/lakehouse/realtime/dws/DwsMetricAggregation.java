package cn.edu.ustc.lakehouse.realtime.dws;

import cn.edu.ustc.lakehouse.realtime.model.AdEvent;
import cn.edu.ustc.lakehouse.realtime.model.RealtimeMetric;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;

/** Complete DWS 10-second metric aggregation, including accumulator and window output. */
public final class DwsMetricAggregation {
    private DwsMetricAggregation() {}

    public static DataStream<RealtimeMetric> build(DataStream<AdEvent> events) {
        return events
                .keyBy(AdEvent::toMetricKey)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new IncrementalAggregate(), new WindowResult())
                .name("DWS 10-second realtime advertising metric aggregation");
    }

    /** Mutable, per-key window state used only inside this aggregation. */
    public static final class Accumulator implements Serializable {
        private BigDecimal spend = BigDecimal.ZERO;
        private BigDecimal orderGmv = BigDecimal.ZERO;
        private BigDecimal attributedGmv = BigDecimal.ZERO;
        private BigDecimal organicGmv = BigDecimal.ZERO;
        private long impressions;
        private long clicks;
        private long paidOrders;
        private long attributedOrders;
        private long organicOrders;

        public Accumulator add(AdEvent event) {
            spend = spend.add(orZero(event.getSpend()));
            if (event.isPaidOrder()) {
                orderGmv = orderGmv.add(orZero(event.getOrderGmv()));
                attributedGmv = attributedGmv.add(orZero(event.getAttributedGmv()));
                organicGmv = organicGmv.add(orZero(event.getOrganicGmv()));
                paidOrders++;
                if ("attributed".equals(event.getAttributionStatus())) attributedOrders++;
                if ("organic".equals(event.getAttributionStatus())) organicOrders++;
            } else if ("impression".equals(event.getEventType())) {
                impressions++;
            } else if ("click".equals(event.getEventType())) {
                clicks++;
            }
            return this;
        }

        public Accumulator merge(Accumulator other) {
            spend = spend.add(other.spend);
            orderGmv = orderGmv.add(other.orderGmv);
            attributedGmv = attributedGmv.add(other.attributedGmv);
            organicGmv = organicGmv.add(other.organicGmv);
            impressions += other.impressions;
            clicks += other.clicks;
            paidOrders += other.paidOrders;
            attributedOrders += other.attributedOrders;
            organicOrders += other.organicOrders;
            return this;
        }

        private static BigDecimal orZero(BigDecimal value) {
            return value == null ? BigDecimal.ZERO : value;
        }

        public BigDecimal getSpend() { return spend; }
        public BigDecimal getOrderGmv() { return orderGmv; }
        public BigDecimal getAttributedGmv() { return attributedGmv; }
        public BigDecimal getOrganicGmv() { return organicGmv; }
        public long getImpressions() { return impressions; }
        public long getClicks() { return clicks; }
        public long getPaidOrders() { return paidOrders; }
        public long getAttributedOrders() { return attributedOrders; }
        public long getOrganicOrders() { return organicOrders; }
    }

    private static final class IncrementalAggregate
            implements AggregateFunction<AdEvent, Accumulator, Accumulator> {
        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        @Override
        public Accumulator add(AdEvent event, Accumulator accumulator) {
            return accumulator.add(event);
        }

        @Override
        public Accumulator getResult(Accumulator accumulator) {
            return accumulator;
        }

        @Override
        public Accumulator merge(Accumulator left, Accumulator right) {
            return left.merge(right);
        }
    }

    private static final class WindowResult
            extends ProcessWindowFunction<Accumulator, RealtimeMetric, MetricKey, TimeWindow> {
        @Override
        public void process(
                MetricKey key,
                Context context,
                Iterable<Accumulator> accumulators,
                Collector<RealtimeMetric> output) {
            output.collect(RealtimeMetric.from(
                    context.window().getStart(),
                    context.window().getEnd(),
                    key,
                    accumulators.iterator().next()));
        }
    }
}
