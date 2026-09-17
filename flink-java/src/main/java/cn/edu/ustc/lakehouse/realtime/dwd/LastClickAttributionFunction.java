package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdClickEvent;
import cn.edu.ustc.lakehouse.realtime.model.AttributedOrder;
import cn.edu.ustc.lakehouse.realtime.model.AttributionKey;
import cn.edu.ustc.lakehouse.realtime.model.OrderInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/** Six-hour event-time Last Click attribution; DIM lookup is intentionally handled downstream. */
public final class LastClickAttributionFunction extends KeyedCoProcessFunction<
        AttributionKey, AdClickEvent, OrderInfo, AttributedOrder> {
    public static final long ATTRIBUTION_WINDOW_MILLIS = Duration.ofHours(6).toMillis();
    public static final long DEFAULT_ALLOWED_LATENESS_MILLIS = Duration.ofSeconds(10).toMillis();
    private static final Duration ATTRIBUTION_STATE_TTL = Duration.ofHours(7);
    private static final Duration COMPLETED_ORDER_STATE_TTL = Duration.ofDays(32);
    public static final OutputTag<AdClickEvent> LATE_CLICKS = new OutputTag<>("late-clicks") {};

    private static final DateTimeFormatter HOUR = DateTimeFormatter.ofPattern("HH");

    private final long allowedLatenessMillis;
    private transient ListState<AdClickEvent> clickHistoryState;
    private transient ListState<OrderInfo> pendingOrderState;
    private transient MapState<Long, AttributedOrder> completedOrderState;

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
        StateTtlConfig attributionTtl = StateTtlConfig.newBuilder(ATTRIBUTION_STATE_TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ListStateDescriptor<AdClickEvent> clickDescriptor =
                new ListStateDescriptor<>("click-history", AdClickEvent.class);
        clickDescriptor.enableTimeToLive(attributionTtl);
        clickHistoryState = getRuntimeContext().getListState(clickDescriptor);

        ListStateDescriptor<OrderInfo> pendingDescriptor =
                new ListStateDescriptor<>("pending-orders", OrderInfo.class);
        pendingDescriptor.enableTimeToLive(attributionTtl);
        pendingOrderState = getRuntimeContext().getListState(pendingDescriptor);

        MapStateDescriptor<Long, AttributedOrder> completedDescriptor =
                new MapStateDescriptor<>("completed-orders", Long.class, AttributedOrder.class);
        StateTtlConfig completedOrderTtl = StateTtlConfig.newBuilder(COMPLETED_ORDER_STATE_TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        completedDescriptor.enableTimeToLive(completedOrderTtl);
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

        List<AdClickEvent> clicks = new ArrayList<>();
        for (AdClickEvent existing : clickHistoryState.get()) {
            if (existing.eventId != click.eventId) {
                clicks.add(existing);
            }
        }
        clicks.add(click);
        clickHistoryState.update(clicks);
        context.timerService().registerEventTimeTimer(
                click.clickTimeMillis + ATTRIBUTION_WINDOW_MILLIS + allowedLatenessMillis);
    }

    @Override
    public void processElement2(OrderInfo order, Context context, Collector<AttributedOrder> out)
            throws Exception {
        AttributedOrder completed = completedOrderState.get(order.orderId);
        if (completed != null) {
            AttributedOrder updated = updateOrderFields(completed, order);
            completedOrderState.put(order.orderId, updated);
            out.collect(updated);
            return;
        }

        List<OrderInfo> pending = new ArrayList<>();
        boolean alreadyPending = false;
        for (OrderInfo existing : pendingOrderState.get()) {
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
        List<OrderInfo> retained = new ArrayList<>();
        for (OrderInfo order : pendingOrderState.get()) {
            if (order.attributionTimerMillis <= timestamp) {
                if (!completedOrderState.contains(order.orderId)) {
                    AttributedOrder attributed = attribute(
                            order, latestEligibleClick(clickHistoryState.get(), order.payTimeMillis));
                    completedOrderState.put(order.orderId, attributed);
                    out.collect(attributed);
                }
            } else {
                retained.add(order);
            }
        }
        pendingOrderState.update(retained);
        pruneExpiredClicks(timestamp);
    }

    private void pruneExpiredClicks(long timestamp) throws Exception {
        List<AdClickEvent> retained = new ArrayList<>();
        for (AdClickEvent click : clickHistoryState.get()) {
            if (click.clickTimeMillis + ATTRIBUTION_WINDOW_MILLIS + allowedLatenessMillis
                    > timestamp) {
                retained.add(click);
            }
        }
        if (retained.isEmpty()) {
            clickHistoryState.clear();
        } else {
            clickHistoryState.update(retained);
        }
    }

    static AdClickEvent latestEligibleClick(
            Iterable<AdClickEvent> clicks, long orderTimeMillis) {
        if (clicks == null) return null;
        long lowerBound = orderTimeMillis - ATTRIBUTION_WINDOW_MILLIS;
        AdClickEvent latest = null;
        for (AdClickEvent click : clicks) {
            boolean eligible = click.clickTimeMillis >= lowerBound
                    && click.clickTimeMillis <= orderTimeMillis;
            if (eligible && (latest == null
                    || click.clickTimeMillis > latest.clickTimeMillis
                    || (click.clickTimeMillis == latest.clickTimeMillis
                    && click.eventId > latest.eventId))) {
                latest = click;
            }
        }
        return latest;
    }

    static AttributedOrder attribute(OrderInfo order, AdClickEvent latestClick) {
        long lowerBound = order.payTimeMillis - ATTRIBUTION_WINDOW_MILLIS;
        boolean direct = latestClick != null
                && latestClick.clickTimeMillis >= lowerBound
                && latestClick.clickTimeMillis <= order.payTimeMillis;

        AttributedOrder result = updateOrderFields(new AttributedOrder(), order);
        if (direct) {
            result.creativeId = latestClick.creativeId;
        }
        return result;
    }

    static AttributedOrder updateOrderFields(AttributedOrder attributed, OrderInfo order) {
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
        LocalDateTime createdAt = LocalDateTime.parse(
                order.createTime.replace(' ', 'T'), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        result.dt = DateTimeFormatter.BASIC_ISO_DATE.format(createdAt.toLocalDate());
        result.hour = HOUR.format(createdAt);
        result.creativeId = attributed.creativeId;
        return result;
    }
}
