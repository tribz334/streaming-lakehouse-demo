package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdClickEvent;
import cn.edu.ustc.lakehouse.realtime.model.AttributedOrder;
import cn.edu.ustc.lakehouse.realtime.model.AttributionKey;
import cn.edu.ustc.lakehouse.realtime.model.OrderDetail;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/** Six-hour event-time LastClick attribution keyed by uid + product_id. */
public final class LastClickAttributionFunction extends KeyedCoProcessFunction<
        AttributionKey, AdClickEvent, OrderDetail, AttributedOrder> {
    public static final long ATTRIBUTION_WINDOW_MILLIS = Duration.ofHours(6).toMillis();
    public static final long DEFAULT_ALLOWED_LATENESS_MILLIS = Duration.ofSeconds(10).toMillis();
    private static final Duration STATE_TTL = Duration.ofHours(7);
    public static final OutputTag<AdClickEvent> LATE_CLICKS = new OutputTag<>("late-clicks") {};

    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter HOUR = DateTimeFormatter.ofPattern("HH");

    private final long allowedLatenessMillis;
    private transient ValueState<AdClickEvent> latestClickState;
    private transient ListState<OrderDetail> pendingOrderState;
    private transient ValueState<Long> clickCleanupTimerState;
    private transient MapState<Long, Boolean> completedOrderState;

    public LastClickAttributionFunction() {
        this(DEFAULT_ALLOWED_LATENESS_MILLIS);
    }

    public LastClickAttributionFunction(long allowedLatenessMillis) {
        if (allowedLatenessMillis < 0) {
            throw new IllegalArgumentException("allowedLatenessMillis cannot be negative");
        }
        this.allowedLatenessMillis = allowedLatenessMillis;
    }

    @Override
    public void open(OpenContext openContext) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(STATE_TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<AdClickEvent> clickDescriptor =
                new ValueStateDescriptor<>("latest-click", AdClickEvent.class);
        clickDescriptor.enableTimeToLive(ttl);
        latestClickState = getRuntimeContext().getState(clickDescriptor);

        ListStateDescriptor<OrderDetail> pendingDescriptor =
                new ListStateDescriptor<>("pending-orders", OrderDetail.class);
        pendingDescriptor.enableTimeToLive(ttl);
        pendingOrderState = getRuntimeContext().getListState(pendingDescriptor);

        clickCleanupTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("click-cleanup-timer", Long.class));

        MapStateDescriptor<Long, Boolean> completedDescriptor =
                new MapStateDescriptor<>("completed-orders", Long.class, Boolean.class);
        completedDescriptor.enableTimeToLive(ttl);
        completedOrderState = getRuntimeContext().getMapState(completedDescriptor);
    }

    @Override
    public void processElement1(AdClickEvent click, Context context, Collector<AttributedOrder> out)
            throws Exception {
        long watermark = context.timerService().currentWatermark();
        if (watermark != Long.MIN_VALUE && click.clickTimeMillis + allowedLatenessMillis < watermark) {
            context.output(LATE_CLICKS, click);
            return;
        }

        AdClickEvent current = latestClickState.value();
        if (current == null || click.clickTimeMillis > current.clickTimeMillis) {
            latestClickState.update(click);
            replaceClickCleanupTimer(
                    click.clickTimeMillis + ATTRIBUTION_WINDOW_MILLIS + allowedLatenessMillis,
                    context);
        }
    }

    @Override
    public void processElement2(OrderDetail order, Context context, Collector<AttributedOrder> out)
            throws Exception {
        if (completedOrderState.contains(order.orderId)) {
            return;
        }

        List<OrderDetail> pending = new ArrayList<>();
        boolean alreadyPending = false;
        for (OrderDetail existing : pendingOrderState.get()) {
            if (existing.orderId == order.orderId) {
                order.attributionTimerMillis = existing.attributionTimerMillis;
                pending.add(order);
                alreadyPending = true;
            } else {
                pending.add(existing);
            }
        }

        if (!alreadyPending) {
            order.attributionTimerMillis = order.payTimeMillis + allowedLatenessMillis;
            pending.add(order);
            context.timerService().registerEventTimeTimer(order.attributionTimerMillis);
        }
        pendingOrderState.update(pending);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<AttributedOrder> out)
            throws Exception {
        List<OrderDetail> retained = new ArrayList<>();
        for (OrderDetail order : pendingOrderState.get()) {
            if (order.attributionTimerMillis <= timestamp) {
                if (!completedOrderState.contains(order.orderId)) {
                    out.collect(attribute(order, latestClickState.value()));
                    completedOrderState.put(order.orderId, true);
                }
            } else {
                retained.add(order);
            }
        }
        pendingOrderState.update(retained);

        Long cleanupTimer = clickCleanupTimerState.value();
        if (cleanupTimer != null && cleanupTimer == timestamp) {
            AdClickEvent click = latestClickState.value();
            if (click == null
                    || click.clickTimeMillis + ATTRIBUTION_WINDOW_MILLIS + allowedLatenessMillis
                    <= timestamp) {
                latestClickState.clear();
                clickCleanupTimerState.clear();
            }
        }
    }

    private void replaceClickCleanupTimer(long timestamp, Context context) throws Exception {
        Long previous = clickCleanupTimerState.value();
        if (previous != null && previous != timestamp) {
            context.timerService().deleteEventTimeTimer(previous);
        }
        if (previous == null || previous != timestamp) {
            context.timerService().registerEventTimeTimer(timestamp);
            clickCleanupTimerState.update(timestamp);
        }
    }

    static AttributedOrder attribute(OrderDetail order, AdClickEvent latestClick) {
        long lowerBound = order.payTimeMillis - ATTRIBUTION_WINDOW_MILLIS;
        boolean direct = latestClick != null
                && latestClick.clickTimeMillis >= lowerBound
                && latestClick.clickTimeMillis <= order.payTimeMillis;

        AttributedOrder result = new AttributedOrder();
        result.orderId = order.orderId;
        result.uid = order.uid;
        result.productId = order.productId;
        result.shopId = order.shopId;
        result.productPrice = order.productPrice;
        result.productNum = order.productNum;
        result.totalAmount = order.totalAmount;
        result.paymentMethod = order.paymentMethod;
        result.receiverName = order.receiverName;
        result.receiverPhone = order.receiverPhone;
        result.shippingAddress = order.shippingAddress;
        result.trackingNumber = order.trackingNumber;
        result.orderStatus = order.orderStatus;
        result.createTime = order.createTime;
        result.cancelTime = order.cancelTime;
        result.payTime = order.payTime;
        result.confirmTime = order.confirmTime;
        result.refundTime = order.refundTime;
        result.updatedAt = order.updatedAt;
        result.dt = order.dt;
        result.eventTime = Instant.ofEpochMilli(order.eventTimeMillis);
        result.hour = HOUR.format(result.eventTime.atZone(ZONE));
        result.directAttribution = direct;
        if (direct) {
            result.creativeId = latestClick.creativeId;
            result.slotId = latestClick.slotId;
            result.unitId = latestClick.unitId;
            result.campaignId = latestClick.campaignId;
            result.advertiserId = latestClick.advertiserId;
            result.placementType = latestClick.placementType;
            result.adType = latestClick.adType;
            result.clickTime = Instant.ofEpochMilli(latestClick.clickTimeMillis);
        }
        return result;
    }
}
