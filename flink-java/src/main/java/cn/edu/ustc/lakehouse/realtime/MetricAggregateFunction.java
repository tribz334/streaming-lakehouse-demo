package cn.edu.ustc.lakehouse.realtime;

import org.apache.flink.api.common.functions.AggregateFunction;

public class MetricAggregateFunction
        implements AggregateFunction<AdEvent, MetricAccumulator, MetricAccumulator> {
    @Override
    public MetricAccumulator createAccumulator() {
        return new MetricAccumulator();
    }

    @Override
    public MetricAccumulator add(AdEvent event, MetricAccumulator accumulator) {
        return accumulator.add(event);
    }

    @Override
    public MetricAccumulator getResult(MetricAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public MetricAccumulator merge(MetricAccumulator left, MetricAccumulator right) {
        return left.merge(right);
    }
}
