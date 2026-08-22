package cn.edu.ustc.lakehouse.realtime.model;

import cn.edu.ustc.lakehouse.realtime.dwd.AttributionProcessFunction;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Paid order detail carried by the order side of the connected attribution
 * stream. It retains the order-side identity and the best click candidate seen
 * during the ten-second event-time wait.
 */
public class OrderDetail implements Serializable {
    private String eventId;
    private String orderId;
    private String userId;
    private String advertiserId;
    private String productId;
    private BigDecimal orderGmv;
    private long createTimeMillis;
    private long paymentTimeMillis;

    private String attributedClickEventId;
    private long attributedClickTimeMillis = Long.MIN_VALUE;
    private String campaignId;
    private String unitId;
    private String slotId;
    private String creativeId;
    private String media;
    private String commerceScene;
    private String attributionStatus = "pending";

    public OrderDetail() {}

    public boolean attributeTo(AdClickEvent click) {
        if (click == null || "external".equals(click.getCommerceScene())) return false;
        long lag = createTimeMillis - click.getClickTimeMillis();
        if (lag < 0 || lag > AttributionProcessFunction.ATTRIBUTION_WINDOW_MILLIS) return false;
        if (attributedClickEventId != null
                && click.getClickTimeMillis() <= attributedClickTimeMillis) return false;

        attributedClickEventId = click.getEventId();
        attributedClickTimeMillis = click.getClickTimeMillis();
        advertiserId = click.getAdvertiserId();
        campaignId = click.getCampaignId();
        unitId = click.getUnitId();
        slotId = click.getSlotId();
        creativeId = click.getCreativeId();
        media = click.getMedia();
        commerceScene = click.getCommerceScene();
        attributionStatus = "attributed";
        return true;
    }

    public void finalizeAttribution() {
        if (attributedClickEventId == null) {
            advertiserId = null;
            campaignId = null;
            unitId = null;
            slotId = null;
            creativeId = null;
            media = "organic";
            commerceScene = "shop";
            attributionStatus = "organic";
        }
    }

    public AdEvent toAdEvent() {
        AdEvent event = new AdEvent();
        event.setEventId(eventId);
        event.setEventTimeMillis(paymentTimeMillis);
        event.setUserId(userId);
        event.setAdvertiserId(advertiserId);
        event.setCampaignId(campaignId);
        event.setUnitId(unitId);
        event.setSlotId(slotId);
        event.setCreativeId(creativeId);
        event.setProductId(productId);
        event.setMedia(media);
        event.setCommerceScene(commerceScene);
        event.setEventType("order_paid");
        event.setSpend(BigDecimal.ZERO);
        event.setOrderGmv(orZero(orderGmv));
        event.setAttributionStatus(attributionStatus);
        if ("attributed".equals(attributionStatus)) {
            event.setAttributedGmv(orZero(orderGmv));
            event.setOrganicGmv(BigDecimal.ZERO);
        } else {
            event.setAttributedGmv(BigDecimal.ZERO);
            event.setOrganicGmv(orZero(orderGmv));
        }
        return event;
    }

    private static BigDecimal orZero(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getAdvertiserId() { return advertiserId; }
    public void setAdvertiserId(String advertiserId) { this.advertiserId = advertiserId; }
    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }
    public BigDecimal getOrderGmv() { return orderGmv; }
    public void setOrderGmv(BigDecimal orderGmv) { this.orderGmv = orderGmv; }
    public long getCreateTimeMillis() { return createTimeMillis; }
    public void setCreateTimeMillis(long createTimeMillis) { this.createTimeMillis = createTimeMillis; }
    public long getPaymentTimeMillis() { return paymentTimeMillis; }
    public void setPaymentTimeMillis(long paymentTimeMillis) { this.paymentTimeMillis = paymentTimeMillis; }
    public String getAttributedClickEventId() { return attributedClickEventId; }
    public void setAttributedClickEventId(String attributedClickEventId) { this.attributedClickEventId = attributedClickEventId; }
    public long getAttributedClickTimeMillis() { return attributedClickTimeMillis; }
    public void setAttributedClickTimeMillis(long attributedClickTimeMillis) { this.attributedClickTimeMillis = attributedClickTimeMillis; }
    public String getCampaignId() { return campaignId; }
    public void setCampaignId(String campaignId) { this.campaignId = campaignId; }
    public String getUnitId() { return unitId; }
    public void setUnitId(String unitId) { this.unitId = unitId; }
    public String getSlotId() { return slotId; }
    public void setSlotId(String slotId) { this.slotId = slotId; }
    public String getCreativeId() { return creativeId; }
    public void setCreativeId(String creativeId) { this.creativeId = creativeId; }
    public String getMedia() { return media; }
    public void setMedia(String media) { this.media = media; }
    public String getCommerceScene() { return commerceScene; }
    public void setCommerceScene(String commerceScene) { this.commerceScene = commerceScene; }
    public String getAttributionStatus() { return attributionStatus; }
    public void setAttributionStatus(String attributionStatus) { this.attributionStatus = attributionStatus; }
}
