package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** Strongly typed DWD advertising click used by the LastClick connect flow. */
public class AdClickEvent implements Serializable {
    private String eventId;
    private long clickTimeMillis;
    private String userId;
    private String advertiserId;
    private String campaignId;
    private String unitId;
    private String creativeId;
    private String productId;
    private String pid;
    private String media;
    private String commerceScene;

    public AdClickEvent() {}

    public static AdClickEvent from(AdEvent event) {
        AdClickEvent click = new AdClickEvent();
        click.eventId = event.getEventId();
        click.clickTimeMillis = event.getEventTimeMillis();
        click.userId = event.getUserId();
        click.advertiserId = event.getAdvertiserId();
        click.campaignId = event.getCampaignId();
        click.unitId = event.getUnitId();
        click.creativeId = event.getCreativeId();
        click.productId = event.getProductId();
        click.pid = event.getPid();
        click.media = event.getMedia();
        click.commerceScene = event.getCommerceScene();
        return click;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public long getClickTimeMillis() { return clickTimeMillis; }
    public void setClickTimeMillis(long clickTimeMillis) { this.clickTimeMillis = clickTimeMillis; }
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
}
