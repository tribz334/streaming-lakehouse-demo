package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdClickEvent;
import cn.edu.ustc.lakehouse.realtime.model.OrderDetail;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Paper-aligned LastClick process function.
 *
 * <p>Clicks and paid orders remain separate keyed streams. The latest click is
 * retained for thirty minutes, while orders wait ten seconds in event time so
 * a click delayed by transport or CDC scheduling can still claim the order.</p>
 */
public class AttributionProcessFunction extends
        KeyedCoProcessFunction<AttributionKey, AdClickEvent, OrderDetail, OrderDetail> {

    public static final long ATTRIBUTION_WINDOW_MILLIS = Duration.ofMinutes(30).toMillis();
    public static final long ORDER_WAIT_MILLIS = Duration.ofSeconds(10).toMillis();
    public static final OutputTag<AdClickEvent> LATE_CLICKS =
            new OutputTag<>("dirty-late-ad-clicks") {};

    private transient ValueState<AdClickEvent> adClickEvent;
    private transient ListState<OrderDetail> orderDetailList;

    @Override
    public void open(OpenContext openContext) {
        adClickEvent = getRuntimeContext().getState(
                new ValueStateDescriptor<>("adClickEvent", AdClickEvent.class));
        orderDetailList = getRuntimeContext().getListState(
                new ListStateDescriptor<>("orderDetailList", OrderDetail.class));
    }

    /** Click-side input. */
    @Override
    public void processElement1(
            AdClickEvent click, Context context, Collector<OrderDetail> output) throws Exception {
        long watermark = context.timerService().currentWatermark();
        if (watermark != Long.MIN_VALUE
                && click.getClickTimeMillis() + ORDER_WAIT_MILLIS <= watermark) {
            context.output(LATE_CLICKS, click);
            return;
        }

        AdClickEvent current = adClickEvent.value();
        if (current == null || click.getClickTimeMillis() >= current.getClickTimeMillis()) {
            adClickEvent.update(click);
            updateWaitingOrders(click);
        }
        context.timerService().registerEventTimeTimer(
                click.getClickTimeMillis() + ATTRIBUTION_WINDOW_MILLIS);
    }

    /** Order-side input. */
    @Override
    public void processElement2(
            OrderDetail order, Context context, Collector<OrderDetail> output) throws Exception {
        order.attributeTo(adClickEvent.value());
        orderDetailList.add(order);
        context.timerService().registerEventTimeTimer(
                order.getCreateTimeMillis() + ORDER_WAIT_MILLIS);
    }

    @Override
    public void onTimer(
            long timestamp, OnTimerContext context, Collector<OrderDetail> output) throws Exception {
        AdClickEvent click = adClickEvent.value();

        List<OrderDetail> remaining = new ArrayList<>();
        for (OrderDetail order : orderDetailList.get()) {
            if (timestamp >= order.getCreateTimeMillis() + ORDER_WAIT_MILLIS) {
                order.attributeTo(click);
                order.finalizeAttribution();
                output.collect(order);
            } else {
                remaining.add(order);
            }
        }
        orderDetailList.update(remaining);

        if (click != null
                && timestamp >= click.getClickTimeMillis() + ATTRIBUTION_WINDOW_MILLIS) {
            adClickEvent.clear();
        }
    }

    private void updateWaitingOrders(AdClickEvent click) throws Exception {
        List<OrderDetail> updated = new ArrayList<>();
        for (OrderDetail order : orderDetailList.get()) {
            order.attributeTo(click);
            updated.add(order);
        }
        orderDetailList.update(updated);
    }
}
