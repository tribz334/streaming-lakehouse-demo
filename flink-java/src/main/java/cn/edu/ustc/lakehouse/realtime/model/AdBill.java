package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** Atomic advertising bill fact enriched only with the closed-loop flag. */
public class AdBill implements Serializable {
    public long billId;
    public long creativeId;
    public Long unitId;
    public Long campaignId;
    public Long advertiserId;
    public Long slotId;
    public Long uid;
    public int billingType;
    public String commerceChannel;
    public long cost;
    public byte isClosed;
    public long ts;
    public String updatedAt;
    public String dt;
    public String hour;

    public AdBill() {}
}
