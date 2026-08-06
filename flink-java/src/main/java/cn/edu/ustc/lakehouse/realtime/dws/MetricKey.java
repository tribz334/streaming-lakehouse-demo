package cn.edu.ustc.lakehouse.realtime.dws;

import java.io.Serializable;
import java.util.Objects;

public class MetricKey implements Serializable {
    private String advertiserId;
    private String campaignId;
    private String unitId;
    private String creativeId;
    private String media;
    private String commerceScene;

    public MetricKey() {}

    public MetricKey(
            String advertiserId, String campaignId, String unitId, String creativeId,
            String media, String commerceScene) {
        this.advertiserId = advertiserId;
        this.campaignId = campaignId;
        this.unitId = unitId;
        this.creativeId = creativeId;
        this.media = media;
        this.commerceScene = commerceScene;
    }

    public String getAdvertiserId() { return advertiserId; }
    public void setAdvertiserId(String advertiserId) { this.advertiserId = advertiserId; }
    public String getCampaignId() { return campaignId; }
    public void setCampaignId(String campaignId) { this.campaignId = campaignId; }
    public String getUnitId() { return unitId; }
    public void setUnitId(String unitId) { this.unitId = unitId; }
    public String getCreativeId() { return creativeId; }
    public void setCreativeId(String creativeId) { this.creativeId = creativeId; }
    public String getMedia() { return media; }
    public void setMedia(String media) { this.media = media; }
    public String getCommerceScene() { return commerceScene; }
    public void setCommerceScene(String commerceScene) { this.commerceScene = commerceScene; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof MetricKey key)) return false;
        return Objects.equals(advertiserId, key.advertiserId)
                && Objects.equals(campaignId, key.campaignId)
                && Objects.equals(unitId, key.unitId)
                && Objects.equals(creativeId, key.creativeId)
                && Objects.equals(media, key.media)
                && Objects.equals(commerceScene, key.commerceScene);
    }

    @Override
    public int hashCode() {
        return Objects.hash(advertiserId, campaignId, unitId, creativeId, media, commerceScene);
    }
}
