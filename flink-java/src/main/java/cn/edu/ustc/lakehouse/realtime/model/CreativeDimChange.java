package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** Changelog row for the creative -> unit -> campaign -> advertiser hierarchy. */
public class CreativeDimChange implements Serializable {
    private String creativeId;
    private String campaignId;
    private String unitId;
    private String advertiserId;
    private boolean delete;

    public CreativeDimChange() {}

    public String getCreativeId() { return creativeId; }
    public void setCreativeId(String creativeId) { this.creativeId = creativeId; }
    public String getCampaignId() { return campaignId; }
    public void setCampaignId(String campaignId) { this.campaignId = campaignId; }
    public String getUnitId() { return unitId; }
    public void setUnitId(String unitId) { this.unitId = unitId; }
    public String getAdvertiserId() { return advertiserId; }
    public void setAdvertiserId(String advertiserId) { this.advertiserId = advertiserId; }
    public boolean isDelete() { return delete; }
    public void setDelete(boolean delete) { this.delete = delete; }
}
