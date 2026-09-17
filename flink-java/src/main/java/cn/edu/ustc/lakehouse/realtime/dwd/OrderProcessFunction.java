package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.OrderInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Stateful paid-order recognizer used before LastClick attribution.
 *
 * <p>The paid-order stage preserves the ODS changelog and emits paid
 * {@link OrderInfo} values through {@link #PAID_ORDER_OUTPUT}. PAY/REFUND
 * metric events are derived later from the Fluss primary-key
 * changelog of dwd_order_acc and are not persisted as another DWD table.
 */
public abstract class OrderProcessFunction<IN, STATE, OUT>
        extends KeyedProcessFunction<Long, IN, OUT> {
    public static final OutputTag<OrderInfo> PAID_ORDER_OUTPUT =
            new OutputTag<>("paid-order-details") {};

    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final Duration ORDER_STATE_TTL = Duration.ofDays(32);

    private final String stateName;
    private final Class<STATE> stateType;
    private transient ValueState<STATE> previousOrderState;

    private OrderProcessFunction(String stateName, Class<STATE> stateType) {
        this.stateName = stateName;
        this.stateType = stateType;
    }

    public static OrderProcessFunction<Row, OrderInfo, Row> paidOrders() {
        return new PaidOrderStage();
    }

    @Override
    public final void open(OpenContext openContext) {
        ValueStateDescriptor<STATE> descriptor =
                new ValueStateDescriptor<>(stateName, stateType);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(ORDER_STATE_TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
        previousOrderState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public final void processElement(IN value, Context context, Collector<OUT> out) throws Exception {
        STATE current = toState(value);
        RowKind kind = changeKind(value);

        if (kind == RowKind.UPDATE_BEFORE) {
            previousOrderState.update(current);
            return;
        }

        STATE previous = previousOrderState.value();
        emitChanges(previous, current, kind, context, out);
        if (kind == RowKind.DELETE) {
            previousOrderState.clear();
        } else {
            previousOrderState.update(current);
        }
    }

    protected abstract STATE toState(IN value);

    protected abstract RowKind changeKind(IN value);

    protected abstract void emitChanges(
            STATE previous,
            STATE current,
            RowKind kind,
            Context context,
            Collector<OUT> out) throws Exception;

    static boolean isFirstPaid(OrderInfo previous, OrderInfo current) {
        return !isPaid(previous) && isPaid(current);
    }

    static List<Row> projectPaidChanges(OrderInfo previous, OrderInfo current) {
        List<Row> changes = new ArrayList<>(2);
        boolean previousPaid = isPaid(previous);
        boolean currentPaid = isPaid(current);

        if (isFirstPaid(previous, current)) {
            changes.add(toSinkRow(RowKind.INSERT, current));
        } else if (previousPaid && currentPaid) {
            changes.add(toSinkRow(RowKind.UPDATE_BEFORE, previous));
            changes.add(toSinkRow(RowKind.UPDATE_AFTER, current));
        } else if (previousPaid) {
            changes.add(toSinkRow(RowKind.DELETE, previous));
        }
        return changes;
    }

    private static boolean isPaid(OrderInfo order) {
        return order != null && order.payTime != null;
    }

    private static Instant parseTimestamp(String value) {
        if (value == null) throw new IllegalArgumentException("Order event timestamp cannot be null");
        try {
            return Instant.parse(value);
        } catch (RuntimeException ignored) {
            String normalized = value.replace(' ', 'T');
            return LocalDateTime.parse(normalized, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .atZone(ZONE).toInstant();
        }
    }

    private static OrderInfo toOrderInfo(Row row) {
        OrderInfo order = new OrderInfo();
        order.orderId = requiredLong(row, 0);
        order.uid = requiredLong(row, 1);
        order.productId = requiredLong(row, 2);
        order.shopId = nullableLong(row, 3);
        order.productPrice = requiredLong(row, 4);
        order.productNum = requiredInt(row, 5);
        order.totalAmount = requiredLong(row, 6);
        order.paymentMethod = nullableInt(row, 7);
        order.receiverName = string(row, 8);
        order.receiverPhone = string(row, 9);
        order.shippingAddress = string(row, 10);
        order.trackingNumber = string(row, 11);
        order.orderStatus = requiredInt(row, 12);
        order.createTime = string(row, 13);
        order.cancelTime = string(row, 14);
        order.payTime = string(row, 15);
        order.confirmTime = string(row, 16);
        order.refundTime = string(row, 17);
        order.updatedAt = string(row, 18);
        order.dt = string(row, 19);
        if (order.payTime != null) {
            order.payTimeMillis = parseTimestamp(order.payTime).toEpochMilli();
            order.eventTimeMillis = order.payTimeMillis;
        }
        return order;
    }

    private static Row toSinkRow(RowKind kind, OrderInfo order) {
        Row row = Row.withPositions(kind, 23);
        row.setField(0, order.orderId + "-pay");
        row.setField(1, order.orderId);
        row.setField(2, order.uid);
        row.setField(3, order.productId);
        row.setField(4, "PAY");
        row.setField(5, order.payTime);
        row.setField(6, order.totalAmount);
        row.setField(7, order.shopId);
        row.setField(8, order.productPrice);
        row.setField(9, order.productNum);
        row.setField(10, order.totalAmount);
        row.setField(11, order.paymentMethod);
        row.setField(12, order.receiverName);
        row.setField(13, order.receiverPhone);
        row.setField(14, order.shippingAddress);
        row.setField(15, order.trackingNumber);
        row.setField(16, order.orderStatus);
        row.setField(17, order.createTime);
        row.setField(18, order.cancelTime);
        row.setField(19, order.confirmTime);
        row.setField(20, order.refundTime);
        row.setField(21, order.updatedAt);
        row.setField(22, order.payTime.substring(0, 10));
        return row;
    }

    private static long requiredLong(Row row, int position) {
        Object value = row.getField(position);
        if (value instanceof Number number) return number.longValue();
        throw new IllegalArgumentException(
                "Expected numeric field at position " + position + ": " + value);
    }

    private static int requiredInt(Row row, int position) {
        return Math.toIntExact(requiredLong(row, position));
    }

    private static Long nullableLong(Row row, int position) {
        Object value = row.getField(position);
        if (value == null) return null;
        if (value instanceof Number number) return number.longValue();
        throw new IllegalArgumentException(
                "Expected nullable numeric field at position " + position + ": " + value);
    }

    private static Integer nullableInt(Row row, int position) {
        Long value = nullableLong(row, position);
        return value == null ? null : Math.toIntExact(value);
    }

    private static String string(Row row, int position) {
        Object value = row.getField(position);
        return value == null ? null : value.toString();
    }

    private static final class PaidOrderStage
            extends OrderProcessFunction<Row, OrderInfo, Row> {
        private PaidOrderStage() {
            super("previous-ods-order", OrderInfo.class);
        }

        @Override
        protected OrderInfo toState(Row value) {
            return toOrderInfo(value);
        }

        @Override
        protected RowKind changeKind(Row value) {
            return value.getKind();
        }

        @Override
        protected void emitChanges(
                OrderInfo previous,
                OrderInfo current,
                RowKind kind,
                Context context,
                Collector<Row> out) {
            OrderInfo effectiveCurrent = kind == RowKind.DELETE ? null : current;
            OrderInfo effectivePrevious =
                    kind == RowKind.DELETE && previous == null ? current : previous;
            for (Row change : projectPaidChanges(effectivePrevious, effectiveCurrent)) {
                out.collect(change);
            }
            if (kind != RowKind.DELETE && isPaid(current)) {
                context.output(PAID_ORDER_OUTPUT, current);
            }
        }
    }

}
