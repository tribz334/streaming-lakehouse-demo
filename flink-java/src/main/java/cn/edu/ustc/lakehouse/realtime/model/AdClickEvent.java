package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

public class AdClickEvent implements Serializable {
    public long eventId;
    public long uid;
    public long productId;
    public long creativeId;
    public long slotId;
    public long unitId;
    public long campaignId;
    public long advertiserId;
    public int placementType;
    public int adType;
    public long clickTimeMillis;

    public AdClickEvent() {}
    public AttributionKey key() { return new AttributionKey(uid, productId); }
}
