package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;
import java.math.BigDecimal;

/** One immutable advertising charge produced by the ad billing system. */
public class AdBill implements Serializable {
    private String billId;
    private long billTimeMillis;
    private String advertiserId;
    private String campaignId;
    private String unitId;
    private String creativeId;
    private String userId;
    private String media;
    private String commerceScene;
    private BigDecimal cost;

    public AdBill() {}

    public String getBillId() { return billId; }
    public void setBillId(String billId) { this.billId = billId; }
    public long getBillTimeMillis() { return billTimeMillis; }
    public void setBillTimeMillis(long billTimeMillis) { this.billTimeMillis = billTimeMillis; }
    public String getAdvertiserId() { return advertiserId; }
    public void setAdvertiserId(String advertiserId) { this.advertiserId = advertiserId; }
    public String getCampaignId() { return campaignId; }
    public void setCampaignId(String campaignId) { this.campaignId = campaignId; }
    public String getUnitId() { return unitId; }
    public void setUnitId(String unitId) { this.unitId = unitId; }
    public String getCreativeId() { return creativeId; }
    public void setCreativeId(String creativeId) { this.creativeId = creativeId; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getMedia() { return media; }
    public void setMedia(String media) { this.media = media; }
    public String getCommerceScene() { return commerceScene; }
    public void setCommerceScene(String commerceScene) { this.commerceScene = commerceScene; }
    public BigDecimal getCost() { return cost; }
    public void setCost(BigDecimal cost) { this.cost = cost; }
}
