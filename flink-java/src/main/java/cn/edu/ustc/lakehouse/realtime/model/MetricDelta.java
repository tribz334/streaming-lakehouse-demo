package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** Signed metric change; UPDATE_BEFORE/DELETE records carry negative values. */
public class MetricDelta implements Serializable {
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

    public MetricDelta() {}
}
