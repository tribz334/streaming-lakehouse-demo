package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

public class MetricAccumulator implements Serializable {
    public String dt;
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

    public MetricAccumulator() {}
}
