package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;
import java.time.Instant;

/** Final window summary published to the StarRocks-facing DWS table. */
public class CreativeSummary implements Serializable {
    public long creativeId;
    public Instant windowStart;
    public Instant windowEnd;
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
    public long shortVideoPayOrderGmv;
    public long livePayOrderGmv;
    public long imageTextPayOrderGmv;
    public long otherAdTypePayOrderGmv;
    public long searchPayOrderGmv;
    public long splashPayOrderGmv;
    public long feedPayOrderGmv;
    public long rewardedPayOrderGmv;
    public long bannerPayOrderGmv;
    public long otherPlacementPayOrderGmv;
    public String dt;

    public CreativeSummary() {}
}
