package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdClickEvent;
import cn.edu.ustc.lakehouse.realtime.model.AttributedOrder;
import cn.edu.ustc.lakehouse.realtime.model.OrderInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        assertEquals(101L, result.creativeId);
    }

    @Test
    void newestClickIsUsedWhenStateHasBeenOverwritten() {
        AttributedOrder result = LastClickAttributionFunction.attribute(
                order(2), click(202, ORDER_TIME - Duration.ofSeconds(1).toMillis()));
        assertEquals(202L, result.creativeId);
    }

    @Test
    void laterClickAfterOrderDoesNotHideEligibleEarlierClick() {
        AdClickEvent eligible = click(202, ORDER_TIME - Duration.ofMinutes(2).toMillis());
        AdClickEvent afterOrder = click(303, ORDER_TIME + Duration.ofSeconds(1).toMillis());

        AdClickEvent selected = LastClickAttributionFunction.latestEligibleClick(
                List.of(eligible, afterOrder), ORDER_TIME);

        assertEquals(202L, selected.creativeId);
    }

    @Test
    void eachOrderSelectsItsOwnLatestEligibleClickFromHistory() {
        AdClickEvent first = click(101, ORDER_TIME - Duration.ofHours(1).toMillis());
        AdClickEvent second = click(202, ORDER_TIME + Duration.ofMinutes(10).toMillis());

        assertEquals(101L, LastClickAttributionFunction.latestEligibleClick(
                List.of(first, second), ORDER_TIME).creativeId);
        assertEquals(202L, LastClickAttributionFunction.latestEligibleClick(
                List.of(first, second), ORDER_TIME + Duration.ofMinutes(20).toMillis()).creativeId);
    }

    @Test
    void clickAfterOrderCannotBeAttributed() {
        AttributedOrder result = LastClickAttributionFunction.attribute(
                order(3), click(303, ORDER_TIME + 1));
        assertNull(result.creativeId);
    }

    @Test
    void clickOlderThanSixHoursCannotBeAttributed() {
        AttributedOrder result = LastClickAttributionFunction.attribute(
                order(4), click(404, ORDER_TIME - Duration.ofHours(6).toMillis() - 1));
        assertNull(result.creativeId);
    }

    @Test
    void orderWithoutClickIsRetainedAsOrganic() {
        AttributedOrder result = LastClickAttributionFunction.attribute(order(5), null);
        assertEquals(5L, result.orderId);
        assertNull(result.creativeId);
        assertEquals("19700101", result.dt);
    }

    @Test
    void laterRefundKeepsOriginalAttribution() {
        AttributedOrder attributed = LastClickAttributionFunction.attribute(
                order(6), click(606, ORDER_TIME - Duration.ofMinutes(1).toMillis()));
        OrderInfo refunded = order(6);
        refunded.refundTime = "2026-08-29 12:00:00";

        AttributedOrder updated =
                LastClickAttributionFunction.updateOrderFields(attributed, refunded);

        assertEquals(606L, updated.creativeId);
        assertEquals("2026-08-29 12:00:00", updated.refundTime);
    }

    private static OrderInfo order(long orderId) {
        OrderInfo order = new OrderInfo();
        order.orderId = orderId;
        order.uid = 10;
        order.productId = 20;
        order.createTime = "1970-01-01 11:00:00";
        order.payTime = "1970-01-01 12:00:00";
        order.payTimeMillis = ORDER_TIME;
        order.eventTimeMillis = ORDER_TIME;
        order.dt = "1969-12-31";
        return order;
    }

    private static AdClickEvent click(long creativeId, long clickTime) {
        AdClickEvent click = new AdClickEvent();
        click.eventId = creativeId;
        click.uid = 10;
        click.productId = 20;
        click.creativeId = creativeId;
        click.clickTimeMillis = clickTime;
        return click;
    }
}
