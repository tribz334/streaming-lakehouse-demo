package cn.edu.ustc.lakehouse.realtime.model;

import cn.edu.ustc.lakehouse.realtime.dws.MetricKey;

import java.io.Serializable;
import java.math.BigDecimal;

public class AdEvent implements Serializable {
    private String eventId;
    private long eventTimeMillis;
    private String userId;
    private String advertiserId;
    private String campaignId;
    private String unitId;
    private String creativeId;
    private String productId;
    private String pid;
    private String media;
    private String commerceScene;
    private String eventType;
    private String attributionStatus;
    private BigDecimal spend;
    private BigDecimal orderGmv;
    private BigDecimal attributedGmv;
    private BigDecimal organicGmv;

    public AdEvent() {}

    public MetricKey toMetricKey() {
        return new MetricKey(
                advertiserId, campaignId, unitId, creativeId,
                media, commerceScene);
    }

    public boolean isClick() {
        return "click".equals(eventType);
    }

    public boolean isPaidOrder() {
        return "order".equals(eventType) || "order_paid".equals(eventType);
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public long getEventTimeMillis() { return eventTimeMillis; }
    public void setEventTimeMillis(long eventTimeMillis) { this.eventTimeMillis = eventTimeMillis; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getAdvertiserId() { return advertiserId; }
    public void setAdvertiserId(String advertiserId) { this.advertiserId = advertiserId; }
    public String getCampaignId() { return campaignId; }
    public void setCampaignId(String campaignId) { this.campaignId = campaignId; }
    public String getUnitId() { return unitId; }
    public void setUnitId(String unitId) { this.unitId = unitId; }
    public String getCreativeId() { return creativeId; }
    public void setCreativeId(String creativeId) { this.creativeId = creativeId; }
    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }
    public String getPid() { return pid; }
    public void setPid(String pid) { this.pid = pid; }
    public String getMedia() { return media; }
    public void setMedia(String media) { this.media = media; }
    public String getCommerceScene() { return commerceScene; }
    public void setCommerceScene(String commerceScene) { this.commerceScene = commerceScene; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getAttributionStatus() { return attributionStatus; }
    public void setAttributionStatus(String attributionStatus) { this.attributionStatus = attributionStatus; }
    public BigDecimal getSpend() { return spend; }
    public void setSpend(BigDecimal spend) { this.spend = spend; }
    public BigDecimal getOrderGmv() { return orderGmv; }
    public void setOrderGmv(BigDecimal orderGmv) { this.orderGmv = orderGmv; }
    public BigDecimal getAttributedGmv() { return attributedGmv; }
    public void setAttributedGmv(BigDecimal attributedGmv) { this.attributedGmv = attributedGmv; }
    public BigDecimal getOrganicGmv() { return organicGmv; }
    public void setOrganicGmv(BigDecimal organicGmv) { this.organicGmv = organicGmv; }
}
