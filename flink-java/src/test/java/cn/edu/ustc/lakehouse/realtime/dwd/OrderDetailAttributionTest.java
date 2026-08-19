package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdClickEvent;
import cn.edu.ustc.lakehouse.realtime.model.AdEvent;
import cn.edu.ustc.lakehouse.realtime.model.OrderDetail;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderDetailAttributionTest {

    @Test
    void keepsTheLatestEligibleClick() {
        long orderTime = 1_800_000L;
        OrderDetail order = order(orderTime);

        assertTrue(order.attributeTo(click("older", orderTime - 20_000L, "creative-old")));
        assertTrue(order.attributeTo(click("latest", orderTime - 2_000L, "creative-new")));
        assertFalse(order.attributeTo(click("out-of-order-old", orderTime - 10_000L, "creative-stale")));

        order.finalizeAttribution();
        AdEvent result = order.toAdEvent();
        assertEquals("attributed", result.getAttributionStatus());
        assertEquals("creative-new", result.getCreativeId());
        assertEquals(new BigDecimal("128.50"), result.getAttributedGmv());
        assertEquals(BigDecimal.ZERO, result.getOrganicGmv());
    }

    @Test
    void marksOrderOrganicWhenClickIsOutsideThirtyMinutes() {
        long orderTime = AttributionProcessFunction.ATTRIBUTION_WINDOW_MILLIS + 20_000L;
        OrderDetail order = order(orderTime);

        assertFalse(order.attributeTo(click("expired", 1L, "creative-old")));
        order.finalizeAttribution();

        AdEvent result = order.toAdEvent();
        assertEquals("organic", result.getAttributionStatus());
        assertEquals("organic", result.getAdvertiserId());
        assertEquals(new BigDecimal("128.50"), result.getOrganicGmv());
        assertEquals(BigDecimal.ZERO, result.getAttributedGmv());
    }

    @Test
    void attributionKeySeparatesProductsForTheSameUser() {
        AdClickEvent productAClick = click("click-a", 10L, "creative-a");
        productAClick.setUserId("user-1");
        productAClick.setProductId("product-a");

        OrderDetail productBOrder = order(20L);
        productBOrder.setUserId("user-1");
        productBOrder.setProductId("product-b");

        assertFalse(AttributionKey.from(productAClick)
                .equals(AttributionKey.from(productBOrder)));
    }

    private static AdClickEvent click(String eventId, long clickTime, String creativeId) {
        AdClickEvent click = new AdClickEvent();
        click.setEventId(eventId);
        click.setClickTimeMillis(clickTime);
        click.setUserId("user-1");
        click.setAdvertiserId("advertiser-1");
        click.setCampaignId("campaign-1");
        click.setUnitId("unit-1");
        click.setSlotId("slot-1");
        click.setProductId("product-1");
        click.setCreativeId(creativeId);
        click.setMedia("app");
        click.setCommerceScene("shop");
        return click;
    }

    private static OrderDetail order(long createTime) {
        OrderDetail order = new OrderDetail();
        order.setEventId("order-event-1");
        order.setOrderId("order-1");
        order.setUserId("user-1");
        order.setProductId("product-1");
        order.setOrderGmv(new BigDecimal("128.50"));
        order.setCreateTimeMillis(createTime);
        order.setPaymentTimeMillis(createTime + 1_000L);
        return order;
    }
}
