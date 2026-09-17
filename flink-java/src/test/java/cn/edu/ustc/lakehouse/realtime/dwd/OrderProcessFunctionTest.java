package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.OrderInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderProcessFunctionTest {
    @Test
    void recognizesOnlyNullToPresentPaymentTransition() {
        assertTrue(OrderProcessFunction.isFirstPaid(
                order(null, null), order("2026-08-29 10:11:12", null)));
        assertFalse(OrderProcessFunction.isFirstPaid(
                order("2026-08-29 10:11:12", null),
                order("2026-08-29 10:11:12", null)));
    }

    @Test
    void firstPaymentProducesSqlCompatibleInsert() {
        List<Row> changes = OrderProcessFunction.projectPaidChanges(
                order(null, null), order("2026-08-29 10:11:12", null));

        assertEquals(1, changes.size());
        Row row = changes.get(0);
        assertEquals(RowKind.INSERT, row.getKind());
        assertEquals("42-pay", row.getField(0));
        assertEquals("PAY", row.getField(4));
        assertEquals(9900L, row.getField(6));
        assertEquals("2026-08-29", row.getField(22));
    }

    @Test
    void paidUpdatePreservesBeforeAndAfterImages() {
        List<Row> changes = OrderProcessFunction.projectPaidChanges(
                order("2026-08-29 10:11:12", null),
                order("2026-08-29 10:11:12", "2026-08-29 11:00:00"));

        assertEquals(2, changes.size());
        assertEquals(RowKind.UPDATE_BEFORE, changes.get(0).getKind());
        assertEquals(RowKind.UPDATE_AFTER, changes.get(1).getKind());
        assertEquals("2026-08-29 11:00:00", changes.get(1).getField(20));
    }

    private static OrderInfo order(String payTime, String refundTime) {
        OrderInfo order = new OrderInfo();
        order.orderId = 42L;
        order.uid = 7L;
        order.productId = 8L;
        order.shopId = 9L;
        order.productPrice = 3300L;
        order.productNum = 3;
        order.totalAmount = 9900L;
        order.paymentMethod = 1;
        order.orderStatus = 2;
        order.createTime = "2026-08-29 09:00:00";
        order.payTime = payTime;
        order.refundTime = refundTime;
        order.updatedAt = "2026-08-29 11:00:00";
        order.dt = "2026-08-29";
        return order;
    }
}
