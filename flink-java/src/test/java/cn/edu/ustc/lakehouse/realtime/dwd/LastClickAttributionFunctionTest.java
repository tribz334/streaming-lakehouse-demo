package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdClickEvent;
import cn.edu.ustc.lakehouse.realtime.model.AttributedOrder;
import cn.edu.ustc.lakehouse.realtime.model.OrderDetail;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LastClickAttributionFunctionTest {
    private static final long ORDER_TIME = Duration.ofHours(12).toMillis();

    @Test
    void separatesSixHourBusinessWindowFromTenSecondLateness() {
        assertEquals(Duration.ofHours(6).toMillis(),
                LastClickAttributionFunction.ATTRIBUTION_WINDOW_MILLIS);
        assertEquals(Duration.ofSeconds(10).toMillis(),
                LastClickAttributionFunction.DEFAULT_ALLOWED_LATENESS_MILLIS);
    }

    @Test
    void clickBeforeOrderWithinSixHoursIsDirect() {
        AttributedOrder result = LastClickAttributionFunction.attribute(
                order(1), click(101, ORDER_TIME - Duration.ofMinutes(5).toMillis()));
        assertTrue(result.directAttribution);
        assertEquals(101L, result.creativeId);
        assertEquals(ORDER_TIME - Duration.ofMinutes(5).toMillis(), result.clickTime.toEpochMilli());
    }

    @Test
    void newestClickIsUsedWhenStateHasBeenOverwritten() {
        AttributedOrder result = LastClickAttributionFunction.attribute(
                order(2), click(202, ORDER_TIME - Duration.ofSeconds(1).toMillis()));
        assertEquals(202L, result.creativeId);
    }

    @Test
    void clickAfterOrderCannotBeAttributed() {
        AttributedOrder result = LastClickAttributionFunction.attribute(
                order(3), click(303, ORDER_TIME + 1));
        assertFalse(result.directAttribution);
        assertNull(result.creativeId);
        assertNull(result.clickTime);
    }

    @Test
    void clickOlderThanSixHoursCannotBeAttributed() {
        AttributedOrder result = LastClickAttributionFunction.attribute(
                order(4), click(404, ORDER_TIME - Duration.ofHours(6).toMillis() - 1));
        assertFalse(result.directAttribution);
    }

    @Test
    void orderWithoutClickIsRetainedAsOrganic() {
        AttributedOrder result = LastClickAttributionFunction.attribute(order(5), null);
        assertEquals(5L, result.orderId);
        assertFalse(result.directAttribution);
        assertNull(result.advertiserId);
    }

    private static OrderDetail order(long orderId) {
        OrderDetail order = new OrderDetail();
        order.orderId = orderId;
        order.uid = 10;
        order.productId = 20;
        order.payTimeMillis = ORDER_TIME;
        order.eventTimeMillis = ORDER_TIME;
        order.dt = "1970-01-01";
        return order;
    }

    private static AdClickEvent click(long creativeId, long clickTime) {
        AdClickEvent click = new AdClickEvent();
        click.uid = 10;
        click.productId = 20;
        click.creativeId = creativeId;
        click.clickTimeMillis = clickTime;
        return click;
    }
}
