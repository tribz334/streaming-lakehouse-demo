package cn.edu.ustc.lakehouse.realtime;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricWindowFunction
        extends ProcessWindowFunction<MetricAccumulator, RealtimeMetric, MetricKey, TimeWindow> {
    @Override
    public void process(
            MetricKey key,
            Context context,
            Iterable<MetricAccumulator> accumulators,
            Collector<RealtimeMetric> output) {
        MetricAccumulator accumulator = accumulators.iterator().next();
        output.collect(RealtimeMetric.from(
                context.window().getStart(),
                context.window().getEnd(),
                key,
                accumulator));
    }
}
