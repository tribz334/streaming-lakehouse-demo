package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** Signed, source-neutral metric fields produced before window aggregation. */
public class CreativeField implements Serializable {
    public long creativeId;
    public long eventTimeMillis;
    public String dt;
    public int adType;
    public int placementType;
    public long deliveryCount;
    public long impressionCount;
    public long clickCount;
    public long conversionCount;
    public long cost;
    public long closedCost;
    public long payOrderCount;
    public long refundOrderCount;
    public long payOrderGmv;
    public long refundOrderGmv;

    public CreativeField() {}
}
