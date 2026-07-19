package cn.edu.ustc.lakehouse.realtime;

import java.io.Serializable;
import java.math.BigDecimal;

public class AdEvent implements Serializable {
    private String eventId;
    private long eventTimeMillis;
    private String advertiserId;
    private String campaignId;
    private String unitId;
    private String creativeId;
    private String eventType;
    private BigDecimal spend;
    private BigDecimal gmv;

    public AdEvent() {}

    public AdEvent(String eventId, long eventTimeMillis, String advertiserId,
                   String campaignId, String unitId, String creativeId,
                   String eventType, BigDecimal spend, BigDecimal gmv) {
        this.eventId = eventId;
        this.eventTimeMillis = eventTimeMillis;
        this.advertiserId = advertiserId;
        this.campaignId = campaignId;
        this.unitId = unitId;
        this.creativeId = creativeId;
        this.eventType = eventType;
        this.spend = spend;
        this.gmv = gmv;
    }

    public MetricKey toMetricKey() {
        return new MetricKey(advertiserId, campaignId, unitId, creativeId);
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public long getEventTimeMillis() { return eventTimeMillis; }
    public void setEventTimeMillis(long eventTimeMillis) { this.eventTimeMillis = eventTimeMillis; }
    public String getAdvertiserId() { return advertiserId; }
    public void setAdvertiserId(String advertiserId) { this.advertiserId = advertiserId; }
    public String getCampaignId() { return campaignId; }
    public void setCampaignId(String campaignId) { this.campaignId = campaignId; }
    public String getUnitId() { return unitId; }
    public void setUnitId(String unitId) { this.unitId = unitId; }
    public String getCreativeId() { return creativeId; }
    public void setCreativeId(String creativeId) { this.creativeId = creativeId; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public BigDecimal getSpend() { return spend; }
    public void setSpend(BigDecimal spend) { this.spend = spend; }
    public BigDecimal getGmv() { return gmv; }
    public void setGmv(BigDecimal gmv) { this.gmv = gmv; }
}
